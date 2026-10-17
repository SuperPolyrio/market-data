import asyncio
from copy import deepcopy
from decimal import Decimal
import os
import threading
import time

import pytest

from lobv2.book import apply, apply_state, prepare_record, state_view
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
