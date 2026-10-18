import asyncio
from copy import deepcopy
from decimal import Decimal
import os
import threading
import time

import pytest

from lobv2.book import apply, apply_state, prepare_record, state_view, freeze_state
from lobv2.core import Owner
from lobv2.journal import Journal, read_journal


def reset(epoch="A"):
    asset = {"bids":[["0.4","1234567890.1234567890123456789"]],"asks":[["0.6","3"]],"ready":True,
             "snapshot_epoch":1,"last_source_time":"1","baseline_ref":{},"last_ref":{}}
    return {"kind":"SOURCE_RESET","scope":"MARKET","assets":{"1":deepcopy(asset),"2":deepcopy(asset)},
            "source_epoch":epoch}


def group(i):
    return {"kind":"CHANGE_GROUP","raw_ref":{"i":i},"source_time_raw":str(i),"changes":[
        {"asset":"1","side":"BUY","price":"0.4000","size":str(i)},
        {"asset":"1","side":"BUY","price":"0.4","size":"0"},
        {"asset":"1","side":"BUY","price":"0.400","size":str(i+1)},
        {"asset":"2","side":"SELL","price":"0.6","size":"0" if i%2 else "3"}]}


def test_every_complete_cut_matches_independent_array_reference():
    reference, actual = {}, {}
    for event in [reset(),*[group(i) for i in range(200)],reset("B"),*[group(i) for i in range(200,400)],{"kind":"GAP"}]:
        prepared = prepare_record(event)
        before = actual.get("1",{}).get("bids")
        reference = apply(reference,event)
        actual = apply_state(actual,event,prepared)
        assert state_view(actual)==reference
        if event["kind"]=="CHANGE_GROUP":
            assert actual["1"]["bids"] is before
            assert all(isinstance(p,Decimal) for p in actual["1"]["bids"])


def test_invalid_middle_group_never_exposes_partial_state():
    state = apply_state({},reset())
    before = state_view(state)
    event = group(3)
    event["changes"][2]["size"] = "bad"
    with pytest.raises(ValueError):
        apply_state(state,event)
    assert state_view(state)==before


def test_sync_and_rotation_are_off_owner_and_cuts_stay_ordered(tmp_path,monkeypatch):
    async def run():
        j = Journal(tmp_path,"P","capture")
        owner_thread = threading.get_ident()
        original = os.fdatasync
        threads = []
        def slow_sync(fd):
            threads.append(threading.get_ident())
            time.sleep(.02)
            return original(fd)
        monkeypatch.setattr(os,"fdatasync",slow_sync)
        for i in range(50):j.append("RAW",{"i":i},b"raw")
        task = asyncio.create_task(j.sync())
        await asyncio.sleep(.005)
        assert not task.done() and j.durable_offset==0
        j.append("RAW",{"i":50},b"after fixed batch cut")
        await task
        assert j.published_offset==j.applied_offset==j.durable_offset==50 <= j.written_offset <= j.offset
        assert 51==j.pending[0].offset
        assert all(t!=owner_thread for t in threads)
        receipt=j.cut_receipts["durable"]
        assert not j.complete_durable("old-generation",999,receipt)
        assert not j.complete_durable(j.stream,50,receipt)
        assert j.durable_offset==50
        await j.close()
        assert j.published_offset==j.applied_offset==j.durable_offset==j.written_offset==51
    asyncio.run(run())


def test_cancel_inflight_sync_does_not_duplicate_writes(tmp_path,monkeypatch):
    async def run():
        j=Journal(tmp_path,"P","capture")
        entered,release=threading.Event(),threading.Event()
        original=os.fdatasync
        def wait(fd):
            entered.set();release.wait(2);return original(fd)
        monkeypatch.setattr(os,"fdatasync",wait)
        for i in range(25):j.append("RAW",{"i":i},b"same payload, distinct occurrence")
        task=asyncio.create_task(j.sync())
        while not entered.is_set():await asyncio.sleep(.001)
        task.cancel();release.set()
        with pytest.raises(asyncio.CancelledError):await task
        await j.close()
        records=[r for path in tmp_path.glob("*.journal") for r in read_journal(path)]
        assert [r.offset for r in records]==list(range(1,26))
    asyncio.run(run())


def test_working_epoch_B_cannot_leak_before_committed_epoch_A(tmp_path):
    async def run():
        j=Journal(tmp_path,"P","capture")
        scope={"markets":[{"market":"m","assets":["1","2"],"version":"1","asset_id_kind":"CLOB_TOKEN_ID"}]}
        owner=Owner(j,scope,"PLANT_EMIT")
        owner.emit("m",reset("A"),{"connection":"A"},reset=True)
        await j.sync()
        frozen,_=owner.bootstrap()
        owner.emit("m",group(6),{"connection":"A"})
        owner.emit("m",reset("B"),{"connection":"B"},reset=True)
        assert owner._committed_epoch("m")==1 and owner.epochs["m"]==2
        assert state_view(owner.committed["m"])["1"]["bids"]==frozen["items"][0]["assets"]["1"]["bids"]
        j.batch_records=1
        seen=[]
        j.callbacks.append(lambda rec:seen.append((rec.body["book_epoch"],owner._committed_epoch("m"),state_view(owner.committed["m"]))))
        await j.sync()
        # Owner's base callback applies exact sequence; no newest-working check.
        assert seen[0][0]==1 and seen[0][2]["1"]["bids"]==[["0.4","7"]]
        assert seen[1][0]==2 and seen[1][2]["1"]["bids"]==reset()["assets"]["1"]["bids"]
        await j.close()
    asyncio.run(run())


def test_short_write_and_failure_preserve_prefix(tmp_path,monkeypatch):
    async def run():
        j=Journal(tmp_path,"P","capture")
        original=os.write
        monkeypatch.setattr(os,"write",lambda fd,data:original(fd,data[:max(1,len(data)//3)]))
        for i in range(5):j.append("RAW",{"i":i},os.urandom(1000))
        await j.sync()
        assert j.applied_offset==5
        monkeypatch.setattr(os,"write",lambda fd,data:0)
        j.append("RAW",{"i":5},b"never published")
        with pytest.raises(OSError):await j.sync()
        assert j.durable_offset==j.applied_offset==5 and j.failure
        os.close(j.fd);j._io.shutdown()
    asyncio.run(run())


def test_frozen_cut_survives_in_place_groups_gap_and_resets():
    state=apply_state({},reset())
    frozen=freeze_state(state)
    expected=state_view(frozen)
    assert state['1']['bids'] is frozen['1']['bids']
    state=apply_state(state,group(9))
    assert state['1']['bids'] is not frozen['1']['bids']
    apply_state(state,{'kind':'GAP'})
    apply_state(state,reset('B'))
    assert state_view(frozen)==expected and all(a['ready'] for a in frozen.values())


def test_text_probe_ambiguity_cannot_be_reused_by_late_pong():
    from lobv2.recovery import TextProbe
    probe=TextProbe(2,3)
    assert probe.begin(10_000_000)
    probe.written(10_100_000)
    assert probe.expire(13_000_000)
    assert not probe.begin(14_000_000) and probe.observe(14_100_000) is None
    assert probe.ambiguous and probe.consecutive==0
    fresh=TextProbe(2,3)
    for i in range(3):
        now=20_000_000+i*10_000_000
        assert fresh.begin(now)
        fresh.written(now+100_000)
        result=fresh.observe(now+200_000)
        assert result['healthy']==(i==2)
    assert fresh.observe(60_000_000) is None


def test_recovery_budget_is_shared_and_release_is_immediate(tmp_path):
    from lobv2.recovery import RecoveryBudget
    config={'recovery_lock_root':str(tmp_path),'recovery_sessions_per_host':2,'recovery_sessions_per_plant':1,'recovery_sessions_per_provider':1}
    first=RecoveryBudget(config,'P-0001','LA')
    assert first.try_acquire()
    same_provider=RecoveryBudget(config,'Q-9999','LA')
    assert not same_provider.try_acquire()
    same_plant=RecoveryBudget(config,'P-9999','profile12')
    assert not same_plant.try_acquire()
    first.release()
    assert same_provider.try_acquire()
    same_provider.release()


def test_pong_liveness_cannot_hide_local_queue_and_quiet_token_is_valid(tmp_path):
    from test_semantics import make_plant,initialize
    from lobv2.wire import ByteQueue,MAX_RECORD
    async def run():
        p=make_plant(tmp_path);initialize(p);await p.journal.sync()
        p.require_transport_proof=True;p.require_local_progress=True;p.runtime_config={}
        p.transport_quality(0,0,'c0',True,rtt_ms=1)
        assert p.usable(('m',0,0))
        queue=ByteQueue(MAX_RECORD);queue.put(b'data');queue.arrivals[0]-=2_000_000_000
        p.ingress_queues[(0,0)]=queue
        assert p.healthy_transport(0,0) and not p.usable(('m',0,0))
        assert 'LOCAL_QUEUE_BACKLOG' in p.eligibility_reasons(('m',0,0),'1')
        await queue.get()
        assert p.usable(('m',0,0))
        await p.journal.close()
    asyncio.run(run())


def test_partial_candidate_never_retires_socket_still_serving_other_market(tmp_path,monkeypatch):
    import json
    import lobv2.node as node
    from lobv2.core import Plant
    sockets=[]
    scope={'markets':[{'market':m,'assets':[str(i),str(i+1)],'version':'1','asset_id_kind':'CLOB_TOKEN_ID'} for m,i in [('m1',11),('m2',21)]]}
    class Socket:
        def __init__(self):
            self.index=len(sockets);sockets.append(self);self.closed=False;self.pongs=0;self.tokens=[];self.book_sent=False
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def send(self,data):
            if data=='PING':self.pongs+=1
            else:
                body=json.loads(data)
                if body.get('type')=='market':self.tokens=body['assets_ids']
        async def close(self,**kwargs):self.closed=True
        def __aiter__(self):return self
        async def __anext__(self):
            await asyncio.sleep(.003)
            if self.closed:raise StopAsyncIteration
            if not self.book_sent:
                self.book_sent=True
                return json.dumps([{'event_type':'book','market':m['market'],'asset_id':t,'bids':[],'asks':[]}
                    for m in scope['markets'] for t in m['assets'] if t in self.tokens and not(self.index==0 and t=='22')])
            if self.pongs:self.pongs-=1;return 'PONG'
            return '{}'
    monkeypatch.setattr(node.websockets,'connect',lambda *a,**kw:Socket())
    async def run():
        j=Journal(tmp_path/'j','P','capture');p=Plant(j,scope);stop=asyncio.Event()
        config={'outbound_proxy':'direct','ws_max_bytes':1024,'queue_bytes':16*1024*1024,'heartbeat_seconds':.01,
            'first_snapshot_timeout_seconds':.15,'maximum_healthy_rtt_ms':100,'backlog_reconnect_grace_seconds':.04,
            'recovery_poll_seconds':.005,'recovery_lock_root':str(tmp_path/'budget')}
        p.runtime_config=config;p.require_transport_proof=True;p.require_source_admission=True;p.defer_qualification=True
        tasks=[asyncio.create_task(j.run(stop,.002)),asyncio.create_task(node.qualification_loop(p,stop)),
               asyncio.create_task(node.lane_loop(p,0,0,scope['markets'],config,stop))]
        try:
            for _ in range(100):
                if len(p.selected)==2:break
                await asyncio.sleep(.02)
            assert p.selected['m1'][1]==0 and p.selected['m2'][1]>=1_000_000
            await asyncio.sleep(.05)
            assert not sockets[0].closed
            assert p.usable(p.selected['m1']) and p.usable(p.selected['m2'])
        finally:
            stop.set()
            for task in tasks:task.cancel()
            results=await asyncio.gather(*tasks,return_exceptions=True)
            await j.close()
        assert not [r for r in results if isinstance(r,Exception)]
    asyncio.run(run())


def test_streamed_decoder_keeps_exact_numbers_and_rejects_entire_bad_frame(tmp_path):
    import json
    from lobv2.core import decode_events
    from test_semantics import make_plant,FIXTURE
    async def run():
        raw=b' [ {"price":0.12345678901234567890123456789}, {}, [1,2] ] '
        assert await decode_events(raw)==json.loads(raw,parse_float=str)
        for raw in [b'[] garbage',b'[{},]',b'[{}',b'[,{}]',b'']:
            with pytest.raises(ValueError):await decode_events(raw)
        p=make_plant(tmp_path)
        raw=('['+json.dumps(FIXTURE[0])+','+json.dumps(FIXTURE[1])+',]').encode()
        await p.ingest_async(0,0,'c0',1,raw)
        assert not p.working and p.parse_errors==1
        await p.journal.close()
        assert [r.raw for path in p.journal.root.glob('*.journal') for r in read_journal(path) if r.kind=='RAW']==[raw]
    asyncio.run(run())


def test_durable_raw_before_async_book_parse_still_releases_baselines(tmp_path):
    import json
    from test_semantics import make_plant,FIXTURE
    async def run():
        p=make_plant(tmp_path)
        raw=json.dumps(FIXTURE[:2]*1000).encode()
        stop=asyncio.Event();sync=asyncio.create_task(p.journal.run(stop,.001))
        rec=await p.ingest_async(0,0,'c0',1,raw)
        await p.journal.sync()
        assert all(s['baseline_durable'] for key,s in p.states.items() if key[1]==0)
        assert all(s['baseline_capture_durable_ref']['record_sequence']==rec.offset for key,s in p.states.items() if key[1]==0)
        assert p.working and not p.baseline_pending
        stop.set();await sync;await p.journal.close()
    asyncio.run(run())


def test_lifecycle_outbox_has_original_durable_capture_identity(tmp_path):
    import json
    from test_semantics import make_plant
    from lobv2.wire import FRAME,decode
    async def run():
        p=make_plant(tmp_path)
        event={'event_type':'market_resolved','market':'unknown-but-resolved','id':'999'}
        raw=json.dumps(event).encode()
        rec=await p.ingest_async(0,0,'c0',1,raw)
        await p.journal.sync()
        progress=json.loads(next((p.journal.root.parent/'lifecycle').glob('*.progress.json')).read_bytes())
        data=open(progress['path'],'rb').read()
        assert len(data)==progress['durable_bytes']
        notification=decode(data)
        assert notification.body['event']==event
        assert notification.body['capture_durable_receipt']['record_sequence']==rec.offset
        assert notification.body['capture_durable_receipt']['log_generation']==rec.stream
        await p.journal.close()
        records=[r for f in p.journal.root.glob('*.journal') for r in read_journal(f)]
        assert next(r for r in records if r.kind=='RAW').raw==raw
        assert next(r for r in records if r.kind=='LIFECYCLE_CAPTURED').body['raw_ref']['capture_offset']==rec.offset
    asyncio.run(run())
