import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import struct

import pytest

from lobv2.archive import (DirectoryStore, build_archive, commit_archive, evict, footer_of,
                          progress, read_archive, restore, verify_archive)
from lobv2.book import SourceBook, apply, decimal, digest, state_view
from lobv2.catalog import identity, shards, validate_scope
from lobv2.core import Gateway, Plant
from lobv2.journal import Journal, read_journal, recover_open
from lobv2.node import receive_snapshot, serve, transmit_snapshot
from lobv2.validation import export_parquet, unused_port
from lobv2.wire import ByteQueue, MAX_RECORD, control, decode, encode, read_frame, send

SCOPE={"markets":[{"market":"m","assets":["1","2"],"version":"1","asset_id_kind":"CLOB_TOKEN_ID"}]}
FIXTURE=[json.loads(line) for line in (Path(__file__).parent/"fixtures/native.jsonl").read_text().splitlines()]


def make_plant(tmp_path, name="P", lanes=2):
    journal=Journal(tmp_path/name,name,"capture")
    plant=Plant(journal,SCOPE,lanes)
    for lane in range(lanes):
        plant.connect(0,lane,f"c{lane}",1,SCOPE["markets"])
        plant.written(0,lane,f"c{lane}",["1","2"],f"a{lane}")
    return plant


def feed(plant,event,lane=0,seq=1,connection=None):
    return plant.ingest(0,lane,connection or f"c{lane}",seq,json.dumps(event).encode())


def initialize(plant,lane=0):
    for i,event in enumerate(FIXTURE[:2],1): feed(plant,event,lane,i)


def change(size="15",price="0.5",**kwargs):
    return {"event_type":"price_change","market":"m","timestamp":"123", "price_changes":[
        {"asset_id":"1","side":"BUY","price":price,"size":size,"hash":"same",**kwargs}]}


def test_T01_T02_T06_duplicate_observations_are_preserved(tmp_path):
    async def run():
        p=make_plant(tmp_path)
        initialize(p,0); initialize(p,1)
        for seq in (3,4): feed(p,change(),0,seq)
        feed(p,change(),1,3)
        await p.journal.close()
        records=list(read_journal(next((tmp_path/"P").glob("*.journal"))))
        assert len([r for r in records if r.kind=="RAW"])==7
        groups=[r for r in records if r.kind=="PLANT_EMIT" and r.body["kind"]=="CHANGE_GROUP"]
        assert len(groups)==2
        assert groups[0].body["raw_ref"]!=groups[1].body["raw_ref"]
        assert p.nonselected==2  # lane1's second snapshot and its delta
    asyncio.run(run())


@pytest.mark.parametrize("test_id,size,expected",[("T11","15","15"),("T11","0",None),("T17","12345678901234567890.12345678901234567890123456789","12345678901234567890.12345678901234567890123456789")])
def test_absolute_size_and_precision(test_id,size,expected):
    book=SourceBook("m",["1","2"])
    book.observe(FIXTURE[0],{})
    book.observe(change(size),{})
    bids=book.snapshot()["1"]["bids"]
    assert bids==([["0.5",expected]] if expected else [])


def test_T12_T13_complete_array_atomicity():
    book=SourceBook("m",["1","2"])
    book.observe(FIXTURE[0],{})
    before=book.snapshot()
    event=deepcopy(FIXTURE[2]); event["price_changes"][1]["size"]="invalid"
    with pytest.raises(ValueError): book.observe(event,{})
    assert book.snapshot()==before
    normalized=book.observe(FIXTURE[2],{})
    assert len(normalized["changes"])==2
    assert book.snapshot()["1"]["bids"]==[]


def test_T14_before_snapshot_is_not_replayed_later():
    book=SourceBook("m",["1","2"])
    assert book.observe(change("50"),{})["kind"]=="BEFORE_SNAPSHOT"
    book.observe(FIXTURE[0],{})
    assert book.snapshot()["1"]["bids"]==[["0.5","10"]]
    assert not book.ready


def test_T16_asset_snapshot_scope_and_numeric_keys():
    book=SourceBook("m",["1","2"])
    book.observe(FIXTURE[0],{})
    book.observe(FIXTURE[1],{})
    book.observe(change("20","0.5000"),{})
    assert book.snapshot()["1"]["bids"]==[["0.5","20"]]
    book.observe(FIXTURE[1],{})
    assert book.snapshot()["1"]["bids"]==[["0.5","20"]]
    assert book.ready  # legal empty second asset


def test_T07_T08_T09_T10_source_switch_is_full_reset(tmp_path):
    async def run():
        p=make_plant(tmp_path)
        initialize(p,0)
        feed(p,FIXTURE[0],1)
        p.disconnect(0,0,"c0","FAIL")
        assert "m" not in p.selected  # half target must not qualify
        initialize(p,0)
        initialize(p,1)
        feed(p,change("30"),0,3)
        feed(p,change("20"),1,3)
        assert state_view(p.working["m"])["1"]["bids"]==[["0.5","30"]]
        p.disconnect(0,0,"c0","FAIL")
        assert state_view(p.working["m"])["1"]["bids"]==[["0.5","20"]]
        assert state_view(p.working["m"])["2"]["ready"]
        await p.journal.close()
        emits=[r for r in read_journal(next((tmp_path/"P").glob("*.journal"))) if r.kind=="PLANT_EMIT"]
        reset=emits[-1].body
        assert reset["kind"]=="SOURCE_RESET" and reset["scope"]=="MARKET"
        assert reset["quality"]["continuity_unknown"]
        assert reset["route"]["lane"]==1
    asyncio.run(run())


def test_T15_T18_raw_retained_unknown_and_old_epoch(tmp_path):
    async def run():
        p=make_plant(tmp_path)
        initialize(p)
        before=deepcopy(p.working)
        raw=b'  {"event_type":"future","market":"m","new_field":1}  '
        p.ingest(0,0,"c0",3,raw)
        p.connect(0,0,"new",2,SCOPE["markets"])
        feed(p,change("999"),connection="c0")
        assert not p.books[("m",0,0)].ready
        assert p.working==before
        p.ingest(0,1,"c1",1,b"not json")
        await p.journal.close()
        records=list(read_journal(next((tmp_path/"P").glob("*.journal"))))
        assert raw in [r.raw for r in records]
        assert b"not json" in [r.raw for r in records]
    asyncio.run(run())


def test_T03_T04_T05_T24_T25_transport_identity_and_durability(tmp_path):
    async def run():
        p=make_plant(tmp_path)
        g=Gateway(Journal(tmp_path/"G","G","tape"),SCOPE)
        initialize(p)
        assert not p.committed
        await p.journal.sync()
        snap,sub=p.bootstrap()
        g.start_session("P","s")
        g.snapshot("P","s",snap)
        await g.journal.sync()
        feed(p,change(),seq=3)
        assert sub["queue"].bytes==0  # input + emit not yet durable
        await p.journal.sync()
        rec=decode(await sub["queue"].get())
        g.accept("P","s",rec)
        for _ in range(10): assert not g.accept("P","s",rec)
        assert g.accepted==1 and g.retries==10
        assert state_view(g.committed["m"])["1"]["bids"]==[["0.5","10"]]
        bad=encode(rec.kind,rec.producer,rec.stream,rec.offset,{**rec.body,"x":True})
        with pytest.raises(ValueError,match="CONFLICT"): g.accept("P","s",bad)
        await g.journal.sync()
        assert state_view(g.committed["m"])["1"]["bids"]==[["0.5","15"]]
        await p.journal.close(); await g.journal.close()
    asyncio.run(run())


def test_T19_T20_T22_T41_bootstrap_cut_and_filtered_offsets(tmp_path):
    async def run():
        p=make_plant(tmp_path)
        initialize(p)
        await p.journal.sync()
        cut=p.committed_seq["m"]
        for i in range(10): feed(p,change(str(20+i)),seq=i+3)
        snapshot,sub=p.bootstrap(["m"])
        assert snapshot["items"][0]["cut"]==cut
        assert snapshot["items"][0]["assets"]["1"]["bids"]==[["0.5","10"]]
        assert snapshot["snapshot_id"]!=p.journal.stream
        assert state_view(p.working["m"])["1"]["bids"]==[["0.5","29"]]
        await p.journal.sync()
        records=[decode(await sub["queue"].get()) for _ in range(10)]
        assert [r.body["market_seq"] for r in records]==list(range(cut+1,cut+11))
        assert any(b.offset-a.offset>1 for a,b in zip(records,records[1:]))  # raw/control offsets filtered
        assert snapshot["items"][0]["assets"]["1"]["bids"]==[["0.5","10"]]
        await p.journal.close()
    asyncio.run(run())


def test_T21_T23_T44_bounded_queues_and_invalid_lengths():
    with pytest.raises(ValueError): ByteQueue(MAX_RECORD-1)
    q=ByteQueue(MAX_RECORD,max_items=1)
    q.put(b"first")
    with pytest.raises(BufferError,match="RESYNC"): q.put(b"second")
    assert q.failed
    with pytest.raises(ValueError,match="LENGTH"):
        decode(struct.pack("<II",2**32-1,0))


def test_T26_T28_T39_T45_journal_tail_and_original_encoded_frames(tmp_path):
    async def run():
        j=Journal(tmp_path,"P","capture")
        a=j.append("RAW",{},b' { "number":0.50 } ')
        b=j.append("RAW",{},b' { "number":0.5 } ')
        await j.sync()
        os.write(j.fd,b"\x12\x00")  # controlled partial final record
        os.close(j.fd); j.fd=None
        recovered=recover_open(tmp_path)
        assert recovered[0]["last_recoverable_offset"]==2
        rows=list(read_journal(next(tmp_path.glob("*.journal"))))
        assert [r.frame for r in rows]==[a.frame,b.frame]
        assert rows[0].raw!=rows[1].raw
        assert list(tmp_path.glob("*.recovery.json"))
    asyncio.run(run())


def test_T27_T46_stale_sessions_and_new_bootstrap(tmp_path):
    async def run():
        p=make_plant(tmp_path); initialize(p); await p.journal.sync()
        g=Gateway(Journal(tmp_path/"G","G","tape"),SCOPE)
        snapshot,sub=p.bootstrap()
        g.start_session("P","old"); g.snapshot("P","old",snapshot)
        feed(p,change(),seq=3); await p.journal.sync()
        rec=decode(await sub["queue"].get())
        g.start_session("P","new")
        with pytest.raises(ValueError,match="STALE"): g.accept("P","old",rec)
        snapshot2,_=p.bootstrap()
        g.snapshot("P","new",snapshot2)
        assert state_view(g.working["m"])["1"]["bids"]==[["0.5","15"]]
        with pytest.raises(ValueError,match="RESET_REQUIRED"): g.accept("P","new",rec)
        await p.journal.close(); await g.journal.close()
    asyncio.run(run())


def test_T09_gateway_propagates_selected_lane_reset(tmp_path):
    async def run():
        p=make_plant(tmp_path); initialize(p,0); initialize(p,1); await p.journal.sync()
        g=Gateway(Journal(tmp_path/"G","G","tape"),SCOPE)
        snapshot,sub=p.bootstrap(); g.start_session("P","s"); g.snapshot("P","s",snapshot)
        epoch=g.epochs["m"]
        p.disconnect(0,0,"c0","FAIL"); await p.journal.sync()
        rec=decode(await sub["queue"].get()); assert rec.body["kind"]=="SOURCE_RESET"
        g.accept("P","s",rec)
        assert g.epochs["m"]==epoch+1 and g.routes["m"]["lane"]==1
        await p.journal.close(); await g.journal.close()
    asyncio.run(run())


def test_T42_group_sync_multiple_groups_no_per_group_wait(tmp_path):
    async def run():
        p=make_plant(tmp_path); initialize(p)
        for i in range(250): feed(p,change(str(i+1)),seq=i+3)
        assert p.journal.syncs==0 and not p.committed
        await p.journal.sync()
        assert p.journal.max_sync_batch>=750
        assert p.committed_seq["m"]==251
        await p.journal.close()
    asyncio.run(run())


def test_T29_T30_T31_T32_T35_T40_T43_archive_commits_restore_cleanup(tmp_path):
    async def run():
        j=Journal(tmp_path/"journal","P","capture")
        original=[j.append("RAW",{"recv_seq":i},b"raw repeated"*100) for i in range(30)]
        await j.close()
        journal=next((tmp_path/"journal").glob("*.journal"))
        packed=tmp_path/"segment.lobseg"
        build_archive(journal,packed,block_bytes=1024)
        store=DirectoryStore(tmp_path/"store")
        with pytest.raises(OSError): commit_archive(store,packed,"capture",fail_before_manifest=True)
        assert journal.exists() and not store.keys("commits")
        key,manifest=commit_archive(store,packed,"capture")
        assert commit_archive(store,packed,"capture")[0]==key
        with pytest.raises(ValueError,match="CONFLICT"): store.put(key,b"different")
        assert store.keys("commits")==[key]  # no local catalog needed
        with pytest.raises(ValueError,match="DEPENDENCIES"): evict(journal,packed,store,key,active_dependencies=["reader"])
        receipt=evict(journal,packed,store,key,active_dependencies=[])
        assert receipt["released_bytes"]>0 and not journal.exists()
        restored=tmp_path/"restored.lobseg"
        restore(store,key,restored)
        assert [r.frame for r in read_archive(restored)]==[r.frame for r in original]
        assert len(manifest["blocks"])>1
        state=progress([{"first":1,"last":9},{"first":11,"last":12}],12)
        assert state["remote_contiguous_offset"]==9 and state["holes"]==[[10,10]]
        assert state["uncommitted_records"]==1
    asyncio.run(run())


def test_T28_archive_corruption_rejected(tmp_path):
    async def run():
        j=Journal(tmp_path/"j","P","capture"); j.append("RAW",{},b"evidence"); await j.close()
        path=tmp_path/"s.lobseg"; build_archive(next((tmp_path/"j").glob("*.journal")),path)
        raw=bytearray(path.read_bytes()); raw[45]^=1; path.write_bytes(raw)
        with pytest.raises(ValueError): verify_archive(path)
    asyncio.run(run())


def test_T44_segment_rotation_before_full_record(tmp_path):
    async def run():
        j=Journal(tmp_path,"P","capture",segment_bytes=MAX_RECORD+65536)
        j.append("RAW",{},os.urandom(9*1024*1024))
        j.append("RAW",{},os.urandom(9*1024*1024))
        await j.sync()
        assert list(tmp_path.glob("*.journal"))
        await j.close()
        paths=list(tmp_path.glob("*.journal"))
        assert len(paths)==2 and all(p.stat().st_size<=MAX_RECORD+65536 for p in paths)
    asyncio.run(run())


@pytest.mark.parametrize("error",[OSError(5,"EIO"),OSError(28,"ENOSPC")])
def test_sync_failure_never_publishes(tmp_path,monkeypatch,error):
    async def run():
        p=make_plant(tmp_path); initialize(p)
        saved=os.fdatasync
        def fail(fd): raise error
        monkeypatch.setattr(os,"fdatasync",fail)
        with pytest.raises(OSError): await p.journal.sync()
        assert not p.committed and p.journal.durable_offset==0 and p.journal.failure
        monkeypatch.setattr(os,"fdatasync",saved)
        os.close(p.journal.fd)
    asyncio.run(run())


def test_T21_network_partial_bootstrap_rejected():
    async def run():
        reader=asyncio.StreamReader()
        reader.feed_data(control("BOOTSTRAP_BEGIN",{"bytes":10,"chunks":2,"snapshot_id":"s","session":"x"}))
        reader.feed_eof()
        with pytest.raises(asyncio.IncompleteReadError): await receive_snapshot(reader)
    asyncio.run(run())


def test_T47_cross_generation_returns_reset_required(tmp_path):
    async def run():
        p=make_plant(tmp_path); stop=asyncio.Event(); port=unused_port()
        config={"host":"127.0.0.1","port":port,"queue_bytes":MAX_RECORD}
        task=asyncio.create_task(serve(p,config,stop))
        for _ in range(100):
            try:
                reader,writer=await asyncio.open_connection("127.0.0.1",port); break
            except ConnectionRefusedError: await asyncio.sleep(.01)
        await send(writer,control("HELLO",{"schema":2,"tape_uuid":"old"}))
        rec=decode(await read_frame(reader))
        assert rec.kind=="RESET_REQUIRED" and rec.body["reason"]=="GENERATION_CHANGED"
        writer.close(); await writer.wait_closed(); stop.set(); await task
        await p.journal.close()
    asyncio.run(run())


def test_identity_scope_and_pair_shards():
    with pytest.raises(ValueError): validate_scope({"markets":[*SCOPE["markets"],*SCOPE["markets"]]})
    assert len(shards(SCOPE,2))==1
    row={"id":"x","conditionId":"m","clobTokenIds":"[\"1\",\"2\"]","positionIds":["3","4"],"outcomes":"[\"Yes\",\"No\"]"}
    assert identity(row)["assets"]==["1","2"]
    assert identity({**row,"version":2})["assets"]==["3","4"]
    with pytest.raises(ValueError): identity({**row,"version":3})


def test_stale_write_callback_and_book_before_write_ok(tmp_path):
    async def run():
        p=make_plant(tmp_path); initialize(p); await p.journal.sync()
        assert p.states[(0,0,"1")]["ready"]
        p.written(0,0,"c0",["1","2"],"late")
        assert p.states[(0,0,"1")]["status"]=="READY"
        p.connect(0,0,"new",2,SCOPE["markets"])
        assert not p.written(0,0,"c0",["1","2"],"old")
        assert not p.states[(0,0,"1")]["written"]
        await p.journal.close()
    asyncio.run(run())


def test_T33_parquet_is_separate_and_rebuildable(tmp_path):
    async def run():
        p=make_plant(tmp_path); initialize(p); await p.journal.sync()
        g=Gateway(Journal(tmp_path/"G","G","tape"),SCOPE)
        snapshot,_=p.bootstrap(); g.start_session("P","s"); g.snapshot("P","s",snapshot)
        await g.journal.close(); await p.journal.close()
        assert not (tmp_path/"query").exists()
        receipt=export_parquet(tmp_path/"G",tmp_path/"query")
        assert receipt["records"]==1 and receipt["tape_frame_readback"]=="IDENTICAL"
    asyncio.run(run())


def test_T34_archive_outage_retains_source_and_spool_budget_fails_closed(tmp_path):
    from lobv2.node import metrics_loop
    async def run():
        p=make_plant(tmp_path); initialize(p); await p.journal.close()
        journal=next((tmp_path/"P").glob("*.journal"))
        packed=tmp_path/"outage.lobseg"; build_archive(journal,packed)
        store=DirectoryStore(tmp_path/"store")
        def unavailable(key,data): raise OSError("OFFLINE")
        store.put=unavailable
        with pytest.raises(OSError,match="OFFLINE"): commit_archive(store,packed,"capture")
        assert journal.exists() and packed.exists()
        p.runtime_config={"spool_max_bytes":1,"minimum_free_bytes":0}
        with pytest.raises(OSError,match="SPOOL"):
            # metrics_loop expects node/journal; use a tiny isolated node root.
            root=tmp_path/"node"; (root/"journal").mkdir(parents=True)
            (root/"journal/x").write_bytes(b"too large")
            await metrics_loop(p,root,asyncio.Event())
    asyncio.run(run())


def test_T38_clock_rollback_preserves_source_order_and_monotonic_latency(tmp_path,monkeypatch):
    import time
    async def run():
        p=make_plant(tmp_path); initialize(p); await p.journal.sync()
        wall=time.time_ns()
        monkeypatch.setattr(time,"time_ns",lambda:wall-10_000_000_000)
        feed(p,change("11"),seq=3)
        feed(p,change("12"),seq=4)
        await p.journal.sync()
        assert state_view(p.committed["m"])["1"]["bids"]==[["0.5","12"]]
        assert all(value>=0 for value in p.latencies_ms)
        await p.journal.close()
    asyncio.run(run())


def test_T44_incompressible_container_limit_retains_journal(tmp_path):
    async def run():
        j=Journal(tmp_path/"j","P","capture"); j.append("RAW",{},os.urandom(10000)); await j.close()
        path=next((tmp_path/"j").glob("*.journal"))
        with pytest.raises(ValueError,match="TOO_LARGE"):
            build_archive(path,tmp_path/"small.lobseg",max_bytes=100)
        assert path.exists() and not (tmp_path/"small.lobseg").exists()
    asyncio.run(run())


def test_T45_new_journal_header_and_parent_sync_precede_append(tmp_path,monkeypatch):
    import lobv2.journal as module
    calls=[]
    original_sync_dir=module.sync_dir
    original_sync=os.fdatasync
    def directory(path): calls.append("dir"); return original_sync_dir(path)
    def file(fd): calls.append("file"); return original_sync(fd)
    monkeypatch.setattr(module,"sync_dir",directory)
    monkeypatch.setattr(os,"fdatasync",file)
    async def run():
        j=Journal(tmp_path/"new","P","capture")
        assert "file" in calls and calls[-1]=="dir"
        j.append("RAW",{},b"first")
        assert j.durable_offset==0
        await j.close()
    asyncio.run(run())


def test_T19_actual_tape_market_filter_ignores_other_market_offsets(tmp_path):
    async def run():
        scope={"markets":[*SCOPE["markets"],{**SCOPE["markets"][0],"market":"other","assets":["3","4"]}]}
        g=Gateway(Journal(tmp_path,"G","tape"),scope)
        views={}
        for i,market in enumerate(("m","other")):
            views[market]={str(i*2+j):{"bids":[],"asks":[],"ready":True,"snapshot_epoch":1,
                "last_ref":None,"baseline_ref":None,"last_source_time":None} for j in (1,2)}
            g.emit(market,{"kind":"SOURCE_RESET","scope":"MARKET","assets":views[market]}, {"plant":"P"},reset=True)
        await g.journal.sync()
        snapshot,sub=g.bootstrap(["m"])
        g.emit("other",{"kind":"SNAPSHOT","scope":"ASSET","assets":{"3":views["other"]["3"]}},{"plant":"P"})
        g.emit("m",{"kind":"SNAPSHOT","scope":"ASSET","assets":{"1":views["m"]["1"]}},{"plant":"P"})
        await g.journal.sync()
        rec=decode(await sub["queue"].get())
        assert rec.offset==4 and rec.body["prev_market_seq"]==1 and rec.body["market_seq"]==2
        assert snapshot["items"][0]["cut"]==1
        await g.journal.close()
    asyncio.run(run())


def test_T03_retry_outside_bounded_identity_window_requires_bootstrap(tmp_path):
    async def run():
        p=make_plant(tmp_path); initialize(p); await p.journal.sync()
        g=Gateway(Journal(tmp_path/"G","G","tape"),SCOPE,retry_window=1)
        snapshot,sub=p.bootstrap(); g.start_session("P","s"); g.snapshot("P","s",snapshot)
        sent=[]
        for seq in (3,4):
            feed(p,change(str(seq)),seq=seq); await p.journal.sync()
            rec=decode(await sub["queue"].get()); sent.append(rec); g.accept("P","s",rec)
        with pytest.raises(ValueError,match="RESET_REQUIRED"): g.accept("P","s",sent[0])
        await p.journal.close(); await g.journal.close()
    asyncio.run(run())


def test_T21_overflow_during_snapshot_never_sends_end_or_ready():
    async def run():
        queue=ByteQueue(MAX_RECORD)
        sent=[]
        class Writer:
            def write(self,data): sent.append(decode(data))
            async def drain(self): queue.failed=True
        with pytest.raises(BufferError,match="CANCELLED"):
            await transmit_snapshot(Writer(),asyncio.StreamReader(),{"snapshot_id":"s","items":[]},"session",queue)
        assert [rec.kind for rec in sent]==["BOOTSTRAP_BEGIN"]
    asyncio.run(run())


def test_T23_slow_subscription_resync_does_not_stop_recording(tmp_path):
    async def run():
        p=make_plant(tmp_path); initialize(p); await p.journal.sync()
        _,sub=p.bootstrap()
        sub["queue"].queue=asyncio.Queue(maxsize=1)
        for seq in (3,4,5): feed(p,change(str(seq)),seq=seq)
        await p.journal.sync()
        assert sub["queue"].failed and p.overflows==1
        assert p.committed_seq["m"]==4 and p.journal.pending_size==0
        await p.journal.close()
    asyncio.run(run())


def test_T30_T31_s3_api_ambiguous_success_and_conflict_are_read_back():
    import io
    from lobv2.archive import S3Store
    class Client:
        def __init__(self): self.objects={}
        def put_object(self,**kwargs):
            assert kwargs["IfNoneMatch"]=="*" and kwargs["ChecksumSHA256"]
            if kwargs["Key"] in self.objects: raise OSError("412 Precondition Failed")
            self.objects[kwargs["Key"]]=kwargs["Body"]
            raise TimeoutError("successful PUT response lost")
        def get_object(self,**kwargs):
            data=self.objects[kwargs["Key"]]
            return {"ContentLength":len(data),"Body":io.BytesIO(data)}
    store=S3Store.__new__(S3Store)
    store.client,store.bucket,store.prefix=Client(),"fixture","isolated"
    store.put("commits/a.json",b"expected")
    store.put("commits/a.json",b"expected")
    with pytest.raises(OSError,match="412"): store.put("commits/a.json",b"different")


def test_T39_protobuf_field_order_and_unknown_fields_keep_original_frame():
    from lobv2.wire import frame,unframe
    original=encode("TAPE","G","uuid",5,{"x":1})
    payload=unframe(original.frame)
    assert payload[:2]==b"\x08\x02"
    alternate=frame(payload[2:]+payload[:2]+b"\x98\x06\x01")
    decoded=decode(alternate)
    assert (decoded.stream,decoded.offset)==("uuid",5)
    assert decoded.body==original.body and decoded.frame==alternate and decoded.frame!=original.frame


def test_book_budget_is_atomic():
    book=SourceBook("m",["1","2"],max_levels=2)
    book.observe(FIXTURE[0],{})
    before=book.snapshot()
    with pytest.raises(ValueError,match="MEMORY_BUDGET"): book.observe(change("10","0.4"),{})
    assert book.snapshot()==before


def test_gateway_rejects_wrong_stream_partial_reset_and_unannounced_route(tmp_path):
    async def run():
        p=make_plant(tmp_path); initialize(p); await p.journal.sync()
        g=Gateway(Journal(tmp_path/"G","G","tape"),SCOPE)
        snapshot,sub=p.bootstrap();g.start_session("P","s");g.snapshot("P","s",snapshot)
        feed(p,change(),seq=3); await p.journal.sync();rec=decode(await sub["queue"].get())
        wrong=encode(rec.kind,"P","other-generation",rec.offset,rec.body)
        with pytest.raises(ValueError,match="GENERATION"):g.accept("P","s",wrong)
        badroute=encode(rec.kind,"P",rec.stream,rec.offset,{**rec.body,"route":{**rec.body["route"],"lane":1}})
        with pytest.raises(ValueError,match="ROUTE_CHANGED"):g.accept("P","s",badroute)
        badreset=encode(rec.kind,"P",rec.stream,rec.offset,{**rec.body,"kind":"SOURCE_RESET","scope":"MARKET","assets":{"1":state_view(p.committed["m"])["1"]}})
        with pytest.raises(ValueError,match="INCOMPLETE"):g.accept("P","s",badreset)
        assert g.accepted==0
        assert g.accept("P","s",rec)
        await p.journal.close();await g.journal.close()
    asyncio.run(run())


def test_native_bbo_does_not_prune_unmentioned_levels_or_merge_messages():
    book=SourceBook("m",["1","2"])
    event=deepcopy(FIXTURE[0]);event["bids"]=[{"price":"0.774","size":"211.18"},{"price":"0.77","size":"10"}]
    book.observe(event,{})
    update=change("29509.47","0.77",best_bid="0.77",best_ask="0.8")
    normalized=book.observe(update,{"recv_seq":2})
    assert len(normalized["changes"])==1
    assert book.snapshot()["1"]["bids"][0]==["0.774","211.18"]
    deletion=change("0","0.774",best_bid="0.77",best_ask="0.8")
    book.observe(deletion,{"recv_seq":3})
    assert book.snapshot()["1"]["bids"]==[["0.77","29509.47"]]


def test_batched_transport_delivers_all_groups_with_one_flow_credit_per_batch(tmp_path):
    from lobv2.node import plant_session
    async def run():
        p=make_plant(tmp_path); initialize(p); await p.journal.sync()
        g=Gateway(Journal(tmp_path/"G","G","tape"),SCOPE)
        stop=asyncio.Event();port=unused_port()
        config={"host":"127.0.0.1","port":port,"queue_bytes":MAX_RECORD}
        server=asyncio.create_task(serve(p,config,stop))
        session=asyncio.create_task(plant_session(g,"P",config,stop))
        for _ in range(1000):
            if g.selected.get("m")=="P": break
            await asyncio.sleep(.01)
        assert g.selected.get("m")=="P"
        for seq in range(3,303):feed(p,change(str(seq)),seq=seq)
        await p.journal.sync()
        for _ in range(1000):
            if g.seq.get("m")==301: break
            await asyncio.sleep(.01)
        assert g.seq["m"]==301 and g.accepted==300
        await g.journal.sync()
        assert state_view(g.committed["m"])["1"]["bids"]==[["0.5","302"]]
        stop.set();session.cancel();await asyncio.gather(session,return_exceptions=True);await server
        await p.journal.close();await g.journal.close()
    asyncio.run(run())


def test_readiness_recovers_after_transient_crossed_book(tmp_path):
    from lobv2.core import Plant
    from lobv2.journal import Journal
    import asyncio
    scope={"markets":[{"market":"m","assets":["1","2"],"version":"1","asset_id_kind":"CLOB_TOKEN_ID"}]}
    j=Journal(tmp_path/'j','P','capture')
    p=Plant(j,scope)
    p.connect(0,0,'c',1,scope['markets'])
    p.written(0,0,'c',['1','2'],'a')
    def observe(seq,event):
        p.ingest(0,0,'c',seq,json.dumps(event).encode())
        asyncio.run(j.sync())
    for seq,token in enumerate(['1','2'],1):
        observe(seq,{'event_type':'book','market':'m','asset_id':token,'bids':[{'price':'.4','size':'2'}],'asks':[{'price':'.6','size':'2'}]})
    assert all(s['ready'] for s in p.states.values())
    observe(3,{'event_type':'price_change','market':'m','price_changes':[{'asset_id':'1','side':'BUY','price':'.7','size':'1'}]})
    assert not any(s['ready'] for s in p.states.values())
    observe(4,{'event_type':'price_change','market':'m','price_changes':[{'asset_id':'1','side':'BUY','price':'.7','size':'0'}]})
    assert all(s['ready'] for s in p.states.values())
    asyncio.run(j.close())


def test_quiet_journal_seals_on_time_without_another_message(tmp_path):
    from lobv2.journal import Journal,read_journal
    import asyncio
    async def run():
        j=Journal(tmp_path/'j','P','capture',segment_seconds=.04)
        j.append('TEST',{'n':1})
        stop=asyncio.Event();task=asyncio.create_task(j.run(stop,.005))
        await asyncio.sleep(.08)
        sealed=list((tmp_path/'j').glob('*.journal'))
        assert len(sealed)==1
        assert [r.body for r in read_journal(sealed[0])]==[{'n':1}]
        stop.set();await task;await j.close()
    asyncio.run(run())


def test_unlimited_discovery_exhausts_cursor_and_events_are_supplementary(monkeypatch):
    import asyncio
    import lobv2.catalog as catalog
    class Response:
        def __init__(self,data):self.data=data
        def raise_for_status(self):pass
        def json(self):return self.data
    class Client:
        def __init__(self,*a,**kw):self.calls=[]
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def get(self,url,params):
            self.calls.append(dict(params))
            i=2 if params.get('after_cursor') else 1
            market={'id':str(i),'conditionId':'m'+str(i),'clobTokenIds':[str(i*2),str(i*2+1)],'outcomes':['Yes','No']}
            page={'events':[{'markets':[market]}]} if '/events/' in url else {'markets':[market]}
            if i==1:page['next_cursor']='second'
            return Response(page)
    monkeypatch.setattr(catalog.httpx,'AsyncClient',Client)
    result=asyncio.run(catalog.discover())
    assert len(result['markets'])==2 and result['pages']==2 and result['full_universe_sweep']
    result=asyncio.run(catalog.discover(events=True))
    assert len(result['markets'])==2 and not result['full_universe_sweep'] and result['full_events_sweep']


def test_progressing_initial_dump_is_not_discarded_for_delayed_pong(tmp_path,monkeypatch):
    import asyncio
    from lobv2.core import Plant
    from lobv2.journal import Journal
    import lobv2.node as node
    scope={'markets':[{'market':'m','assets':['1','2'],'version':'1','asset_id_kind':'CLOB_TOKEN_ID'}]}
    class Socket:
        closed=False
        pings=0
        messages=0
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def send(self,data):
            if data=='PING':self.pings+=1
        async def close(self,**kwargs):self.closed=True
        def __aiter__(self):return self
        async def __anext__(self):
            await asyncio.sleep(.01)
            if self.closed:raise StopAsyncIteration
            self.messages+=1
            if self.messages==10:return 'PONG'
            return json.dumps([{'event_type':'book','market':'m','asset_id':t,'bids':[],'asks':[]} for t in ['1','2']])
    socket=Socket()
    monkeypatch.setattr(node.websockets,'connect',lambda *args,**kwargs:socket)
    async def run():
        journal=Journal(tmp_path/'j','P','capture');plant=Plant(journal,scope);stop=asyncio.Event()
        config={'outbound_proxy':'direct','ws_max_bytes':1024,'queue_bytes':16777216,'heartbeat_seconds':.01,
            'pong_timeout_seconds':.04,'first_snapshot_timeout_seconds':100,'maximum_healthy_rtt_ms':20}
        task=asyncio.create_task(node.lane_loop(plant,0,0,scope['markets'],config,stop))
        await asyncio.sleep(.15)
        assert not socket.closed and plant.raw_received>4
        assert socket.pings>=5  # delayed PONG must not stop the heartbeat cadence
        assert plant.transports[(0,0)]['rtt_ms']>50
        assert not plant.transports[(0,0)]['healthy']  # oldest outstanding PING, not latest
        stop.set();task.cancel();await asyncio.gather(task,return_exceptions=True);await journal.close()
    asyncio.run(run())


def test_cancelled_connection_tasks_do_not_reserve_future_startup_minutes(tmp_path):
    import asyncio,time
    from lobv2.node import connection_credit
    async def run():
        config={'connection_rate_file':str(tmp_path/'credit'),'connections_per_second':40}
        await connection_credit(config)
        queued=[asyncio.create_task(connection_credit(config)) for _ in range(20)]
        await asyncio.sleep(.005)
        for task in queued:task.cancel()
        await asyncio.gather(*queued,return_exceptions=True)
        start=time.monotonic();await connection_credit(config)
        assert time.monotonic()-start<.10
    asyncio.run(run())


def test_progressing_stale_lane_requests_new_native_baseline(tmp_path,monkeypatch):
    import asyncio
    from lobv2.core import Plant
    from lobv2.journal import Journal,read_journal
    import lobv2.node as node
    scope={'markets':[{'market':'m','assets':['1','2'],'version':'1','asset_id_kind':'CLOB_TOKEN_ID'}]}
    class Socket:
        closed=False
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def send(self,data):pass
        async def close(self,**kwargs):self.closed=True
        def __aiter__(self):return self
        async def __anext__(self):
            await asyncio.sleep(.005)
            if self.closed:raise StopAsyncIteration
            return json.dumps([{'event_type':'book','market':'m','asset_id':t,'bids':[],'asks':[]} for t in ['1','2']])
    socket=Socket();monkeypatch.setattr(node.websockets,'connect',lambda *args,**kwargs:socket)
    async def run():
        journal=Journal(tmp_path/'j','P','capture');plant=Plant(journal,scope);stop=asyncio.Event()
        config={'outbound_proxy':'direct','ws_max_bytes':1024,'queue_bytes':16777216,'heartbeat_seconds':.01,
            'pong_timeout_seconds':1,'first_snapshot_timeout_seconds':100,
            'backlog_reconnect_grace_seconds':.03,'maximum_receive_backlog_seconds':.02}
        task=asyncio.create_task(node.lane_loop(plant,0,0,scope['markets'],config,stop))
        await asyncio.sleep(.1)
        assert socket.closed and plant.raw_received>2
        assert not plant.books[('m',0,0)].ready
        stop.set();task.cancel();await asyncio.gather(task,return_exceptions=True);await journal.close()
        records=[r for p in (tmp_path/'j').glob('*.journal') for r in read_journal(p)]
        assert sum(r.kind=='RAW' for r in records)==plant.raw_received
        assert any(r.kind=='BACKLOG_RESYNC' for r in records)
    asyncio.run(run())


def test_large_ssh_input_keeps_flowing_across_parent_poll_timeouts(tmp_path,monkeypatch):
    import hashlib,sys,subprocess,threading
    import lobv2.ssh_store as module
    real_popen=subprocess.Popen
    def slow_reader(argv,**kwargs):
        code="import sys,time,hashlib;time.sleep(.4);data=sys.stdin.buffer.read();sys.stdout.write(hashlib.sha256(data).hexdigest())"
        return real_popen([sys.executable,'-c',code],**kwargs)
    monkeypatch.setattr(module.subprocess,'Popen',slow_reader)
    store=module.SSHStore({'host':'local-test','identity_file':'unused','root':'unused','helper':'unused','lock_root':str(tmp_path/'locks')})
    store.cancel_event=threading.Event();timer=threading.Timer(3,store.cancel_event.set);timer.start()
    data=b'full-payload'*200000
    try:
        assert store.request('put','large-object',data).decode()==hashlib.sha256(data).hexdigest()
    finally:timer.cancel()


def test_empty_current_snapshot_after_plant_restart_forgets_old_sequences(tmp_path):
    async def run():
        old=make_plant(tmp_path/'old');initialize(old)
        for seq in range(3,8):feed(old,change(str(seq)),seq=seq)
        await old.journal.sync()
        gateway=Gateway(Journal(tmp_path/'G','G','tape'),SCOPE)
        gateway.start_session('P','old-session');snapshot,subscription=old.bootstrap()
        gateway.snapshot('P','old-session',snapshot);await gateway.journal.sync()
        assert gateway.shadows[('P','m')]['seq']>1
        new=make_plant(tmp_path/'new')
        gateway.start_session('P','new-session');empty,subscription=new.bootstrap()
        assert empty['items']==[]
        gateway.snapshot('P','new-session',empty)
        initialize(new);await new.journal.sync()
        records=[r for path in new.journal.root.glob('*.open') for r in read_journal(path) if r.kind=='PLANT_EMIT']
        assert records[0].body['prev_market_seq']==0
        assert gateway.accept('P','new-session',records[0])
        await gateway.journal.sync()
        assert gateway.selected['m']=='P' and gateway.shadows[('P','m')]['session']=='new-session'
        await old.journal.close();await new.journal.close();await gateway.journal.close()
    asyncio.run(run())


def tier_modules(tmp_path,monkeypatch):
    import importlib.util,sys,sqlite3
    directory=Path(__file__).parents[1]/'tools'
    spec=importlib.util.spec_from_file_location('archive_receiver',directory/'archive_receiver.py');receiver=importlib.util.module_from_spec(spec);spec.loader.exec_module(receiver)
    receiver.HOT=tmp_path/'hot';receiver.COLD=tmp_path/'cold';receiver.MIN_FREE_BYTES=0;receiver.MIN_FREE_INODES=0
    monkeypatch.setitem(sys.modules,'archive_receiver',receiver)
    spec=importlib.util.spec_from_file_location('cold_mirror_test',directory/'archive_cold_mirror.py');mirror=importlib.util.module_from_spec(spec);spec.loader.exec_module(mirror)
    db=sqlite3.connect(tmp_path/'progress.sqlite',check_same_thread=False)
    db.execute('create table commits(key text primary key,producer text,stream text,first_offset integer,last_offset integer,records integer,bytes integer)')
    data=b'original-compressed-record-frames'*10000;key='objects/tape/G/s/1-3.lobseg';manifest_key='commits/tape/G/s/1-3.json'
    manifest={'data_key':key,'producer':'G','stream':'s','first':1,'last':3,'count':3,'object_bytes':len(data),'object_sha256':hashlib.sha256(data).hexdigest()}
    receiver.put(key,data);receiver.put(manifest_key,json.dumps(manifest).encode())
    return receiver,mirror,db,data,key,manifest_key


def test_remote_nvme_commit_cold_mirror_and_logical_readback(tmp_path,monkeypatch):
    receiver,mirror,db,data,key,manifest_key=tier_modules(tmp_path,monkeypatch)
    assert receiver.read(key)==data and not (receiver.COLD/manifest_key).exists()
    mirror.mirror(receiver.HOT/manifest_key,db)
    assert receiver.read(key)==data and (receiver.COLD/manifest_key).exists()
    assert not (receiver.HOT/key).exists() and not (receiver.HOT/manifest_key).exists()
    receiver.put(key,data)
    assert not (receiver.HOT/key).exists()
    with pytest.raises(ValueError,match='IMMUTABLE_OBJECT_CONFLICT'):receiver.put(key,b'changed')
    assert mirror.progress(db)['G/s']['contiguous_offset']==3
    db.close()


def test_failed_cold_manifest_keeps_remote_hot_custody(tmp_path,monkeypatch):
    receiver,mirror,db,data,key,manifest_key=tier_modules(tmp_path,monkeypatch)
    original=mirror.put_immutable
    def fail_manifest(root,key,value):
        if key.startswith('commits/'):raise OSError('INJECTED_COLD_MANIFEST_FAILURE')
        return original(root,key,value)
    monkeypatch.setattr(mirror,'put_immutable',fail_manifest)
    with pytest.raises(OSError):mirror.mirror(receiver.HOT/manifest_key,db)
    assert (receiver.HOT/key).read_bytes()==data and (receiver.HOT/manifest_key).exists()
    assert not (receiver.COLD/manifest_key).exists()
    assert db.execute('select count(*) from commits').fetchone()[0]==0
    db.close()


def test_cold_mirror_waits_for_active_hot_file_reader(tmp_path,monkeypatch):
    import threading,time
    from contextlib import contextmanager
    receiver,mirror,db,data,key,manifest_key=tier_modules(tmp_path,monkeypatch)
    opened=threading.Event();release=threading.Event();attempted=threading.Event();consumed=[];failures=[]
    def reader():
        with receiver.lease(key):
            with (receiver.HOT/key).open('rb') as src:
                opened.set();release.wait(3);consumed.append(src.read())
    original=mirror.lease
    @contextmanager
    def watched(value,exclusive=False):
        if value==key and exclusive:attempted.set()
        with original(value,exclusive) as path:yield path
    monkeypatch.setattr(mirror,'lease',watched)
    def run_mirror():
        try:mirror.mirror(receiver.HOT/manifest_key,db)
        except Exception as exc:failures.append(exc)
    a=threading.Thread(target=reader);a.start();assert opened.wait(2)
    b=threading.Thread(target=run_mirror);b.start();assert attempted.wait(2)
    assert (receiver.HOT/key).exists() and (receiver.COLD/manifest_key).exists()
    release.set();a.join(3);b.join(3)
    assert not a.is_alive() and not b.is_alive() and not failures
    assert consumed==[data] and receiver.read(key)==data and not (receiver.HOT/key).exists()
    db.close()


def test_remote_nvme_reserve_refuses_new_object_without_deleting_commits(tmp_path,monkeypatch):
    receiver,mirror,db,data,key,manifest_key=tier_modules(tmp_path,monkeypatch)
    receiver.MIN_FREE_BYTES=10**30
    with pytest.raises(OSError,match='RESERVE'):receiver.put('objects/new',b'new')
    assert receiver.read(key)==data and (receiver.HOT/manifest_key).exists()
    assert not (receiver.HOT/'objects/new').exists()
    db.close()


def test_transport_proof_gates_publication_and_healthy_primary_resets_whole_market(tmp_path):
    async def run():
        p=make_plant(tmp_path);p.require_transport_proof=True;p.prefer_primary_lane=True
        initialize(p,0);initialize(p,1);await p.journal.sync()
        assert not p.selected and not p.committed and all(s['baseline_durable'] and not s['ready'] for s in p.states.values())
        pong=p.ingest(0,1,'c1',3,b'PONG');p.transport_quality(0,1,'c1',True,rtt_ms=100,raw_ref={'capture_offset':pong.offset});await p.journal.sync()
        assert p.selected['m'][2]==1
        first_epoch=p.epochs['m']
        pong=p.ingest(0,0,'c0',3,b'PONG');p.transport_quality(0,0,'c0',True,rtt_ms=50,raw_ref={'capture_offset':pong.offset});await p.journal.sync()
        assert p.selected['m'][2]==0 and p.epochs['m']==first_epoch+1
        assert set(p.committed['m'])=={'1','2'} and p._committed_meta['m']['kind']=='SOURCE_RESET'
        assert p._committed_meta['m']['quality']['transport_healthy']
        p.transport_quality(0,0,'c0',False,rtt_ms=9000);await p.journal.sync()
        assert p.selected['m'][2]==1
        p.transport_quality(0,1,'c1',False,rtt_ms=9000);await p.journal.sync()
        assert 'm' not in p.selected and not any(a['ready'] for a in p.committed['m'].values())
        before=p.raw_received;feed(p,change('321'),0,4);await p.journal.sync()
        assert p.raw_received==before+1 and 'm' not in p.selected
        await p.journal.close()
    asyncio.run(run())


def test_archive_policy_is_taken_from_original_records_not_current_running_code(tmp_path):
    for index,policy in enumerate(['single_ready_route_v2.0.0','single_healthy_route_v2.0.1']):
        journal=Journal(tmp_path/str(index),'P','capture');journal.policy_version=policy;journal.append('RAW',{'test':True},b'original')
        asyncio.run(journal.close());source=next(journal.root.glob('*.journal'));packed=source.with_suffix('.lobseg');build_archive(source,packed)
        key,manifest=commit_archive(DirectoryStore(tmp_path/'store'),packed,'capture')
        assert manifest['policy_version']==policy
