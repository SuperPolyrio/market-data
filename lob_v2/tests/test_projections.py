import asyncio
import json
from pathlib import Path
import pytest

from lobv2.archive import DirectoryStore,build_archive,commit_archive,read_archive
from lobv2.journal import Journal
from lobv2.projections import observation_projection,tape_projection,checkpoint,commit_projection


def captured(tmp_path):
    async def run():
        inputs=[];manifests=[]
        store=DirectoryStore(tmp_path/'remote')
        for producer in ['P','Q']:
            j=Journal(tmp_path/producer,producer,'capture')
            for i in range(3):j.append('RAW',{'connection':producer,'recv_seq':i+1},b'{  "exact": "original bytes" }\n')
            await j.close()
            path=tmp_path/(producer+'.lobseg');build_archive(next(j.root.glob('*.journal')),path)
            inputs.append(path);manifests.append(commit_archive(store,path,'capture')[1])
        return inputs,manifests,store
    return asyncio.run(run())


def test_payload_reuse_preserves_all_independent_occurrences_and_original_frames(tmp_path):
    inputs,manifests,store=captured(tmp_path)
    out=tmp_path/'view';receipt=observation_projection(inputs,out)
    assert receipt['raw_occurrences']==6 and receipt['occurrences']==6
    assert receipt['event_occurrences_removed']==0
    assert receipt['unique_payload_bytes']*6==receipt['raw_payload_bytes']
    result=commit_projection(store,out,'observations',manifests)
    assert not result['manifest']['authorizes_capture_deletion']
    assert result==commit_projection(store,out,'observations',manifests)
    assert all(path.exists() for path in inputs)


def test_projection_object_failure_does_not_commit_or_delete_capture(tmp_path,monkeypatch):
    inputs,manifests,store=captured(tmp_path)
    out=tmp_path/'view';observation_projection(inputs,out)
    original=store.put
    def put(key,data):
        if key.startswith('query/commits/'):raise OSError('injected manifest failure')
        return original(key,data)
    monkeypatch.setattr(store,'put',put)
    with pytest.raises(OSError):commit_projection(store,out,'observations',manifests)
    assert not list((store.root/'query/commits').rglob('*.json'))
    assert all(path.exists() for path in inputs)


def test_quiet_checkpoint_keeps_original_cut_and_is_not_native_event(tmp_path):
    from lobv2.core import Owner
    from test_semantics import SCOPE
    from test_remediation import reset
    async def run():
        j=Journal(tmp_path/'tape','G','tape');owner=Owner(j,SCOPE,'TAPE')
        r=owner.emit('m',reset(),{'plant':'P','lane':0},reset=True)
        await j.close()
        path=tmp_path/'tape.lobseg';build_archive(next(j.root.glob('*.journal')),path)
        view=tape_projection([path],tmp_path/'query')
        assert view['records']==1 and view['tape_frame_readback']=='IDENTICAL'
        future=r.created_ns+3600*10**9
        receipt=checkpoint([path],tmp_path/'checkpoint',generated_ns=future)
        row=json.loads((tmp_path/'checkpoint/checkpoint.jsonl').read_text())
        assert row['generated_utc_ns']==future and not row['native_event']
        assert row['last_applied_cut']['record_sequence']==r.offset
        assert row['assets']['1']['last_source_time']=='1'
        assert receipt['native_events_created']==0
    asyncio.run(run())


def test_query_cold_mirror_commits_last_and_preserves_hot_on_failure(tmp_path,monkeypatch):
    import hashlib
    from test_semantics import tier_modules
    receiver,mirror,db,_,_,_=tier_modules(tmp_path,monkeypatch)
    data=b'physical parquet data';key='query/objects/test/file.parquet';commit='query/commits/test/id.json'
    m={'view':'observations','input_ranges':[], 'files':[{'key':key,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}]}
    receiver.put(key,data);receiver.put(commit,json.dumps(m).encode())
    original=mirror.put_immutable
    def fail(root,key,data):
        if key==commit:raise OSError('cold query commit failure')
        return original(root,key,data)
    monkeypatch.setattr(mirror,'put_immutable',fail)
    with pytest.raises(OSError):mirror.mirror(receiver.HOT/commit,db)
    assert (receiver.HOT/key).exists() and (receiver.HOT/commit).exists()
    monkeypatch.setattr(mirror,'put_immutable',original)
    mirror.mirror(receiver.HOT/commit,db)
    assert receiver.read(key)==data and not (receiver.HOT/key).exists()
    assert (receiver.COLD/commit).exists() and db.execute('select count(*) from query_commits').fetchone()[0]==1
    assert db.execute('select count(*) from commits').fetchone()[0]==0
    db.close()


def test_live_quiet_checkpoint_waits_for_exact_archive_prefix(tmp_path,monkeypatch):
    import importlib.util
    from lobv2.core import Owner
    from lobv2.book import state_view
    from test_semantics import SCOPE
    from test_remediation import reset
    path=Path(__file__).parents[1]/'tools/checkpoint_live.py'
    spec=importlib.util.spec_from_file_location('checkpoint_live_test',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    async def run():
        data=tmp_path/'data/G/journal';j=Journal(data,'G','tape');owner=Owner(j,SCOPE,'TAPE')
        owner.emit('m',reset(),{'plant':'P','lane':0},reset=True);await j.sync()
        snapshot,sub=owner.bootstrap(deferred=True);owner.subscribers.discard(sub)
        snapshot['items']=[{**item,'assets':state_view(item['assets'])} for item in snapshot['items']]
        snapshot['generated_utc_ns']+=3600*10**9
        output=tmp_path/'checkpoints';output.mkdir();saved=module.save(snapshot,'G',output)
        store=DirectoryStore(tmp_path/'remote');monkeypatch.setattr(module,'SSHStore',lambda config:store)
        settings={'data_root':str(tmp_path/'data'),'archive':{'ssh_store':{}}}
        module.publish(settings,output)
        assert list((output/'pending').glob('*/checkpoint-receipt.json')) and not list((output/'receipts').glob('*.json'))
        await j.close();source=next(data.glob('*.journal'));packed=tmp_path/'tape.lobseg';build_archive(source,packed)
        key,manifest=commit_archive(store,packed,'tape')
        (data/'proof.archive.json').write_text(json.dumps({'manifest_key':key,'manifest':manifest}))
        module.publish(settings,output)
        receipt=json.loads(next((output/'receipts').glob('*.json')).read_bytes())
        assert receipt['status']=='REMOTE_ARCHIVED' and receipt['tape_cut']==1
        assert receipt['generated_utc_ns']==snapshot['generated_utc_ns'] and receipt['native_events_created']==0
        assert not list((output/'pending').glob('*')) and source.exists()
    asyncio.run(run())
