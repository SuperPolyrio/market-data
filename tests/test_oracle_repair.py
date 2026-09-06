from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json

from eth_abi import encode
import pytest

from market_data import oracle, oracle_repair as repair


def _log(block=100, index=1, *, settle=False):
    abi = oracle.CONDITION_RESOLUTION if settle else oracle.CONDITION_PREPARATION
    return {
        "address": oracle.CTF,
        "topics": [oracle._topic(abi), "0x" + "11" * 32, "0x" + "00" * 12 + "22" * 20, "0x" + "33" * 32],
        "data": "0x" + (encode(["uint256", "uint256[]"], [2, [0, 1]]) if settle else encode(["uint256"], [2])).hex(),
        "blockNumber": hex(block), "logIndex": hex(index), "transactionIndex": "0x0",
        "transactionHash": "0x" + f"{block:064x}", "blockHash": "0x" + "44" * 32,
        "removed": False,
    }


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class Connection:
    def __init__(self, rows=(), state=None):
        self.rows = {repair._key(row): deepcopy(row) for row in rows}
        self.state = state
        self.receipts = {}
        self.committed = (deepcopy(self.rows), deepcopy(self.state), deepcopy(self.receipts))
        self.ddl = []
        self.rollback_count = 0
        self.state_keys = []
        self.writes = 0

    def execute(self, sql, params=()):
        if sql.startswith("CREATE "):
            self.ddl.append(sql)
            return Rows([])
        if "INSERT INTO ops.oracle_ctf_repair_windows" in sql:
            cursor_key, start, end, events, stored, digest, receipt_json = params
            receipt = json.loads(receipt_json)
            assert receipt["verified"] is True and events == stored
            assert receipt["events_sha256"] == digest
            self.receipts[cursor_key, start, end] = receipt
            return Rows([])
        if "pg_try_advisory_lock" in sql:
            return Rows([{"locked": True}])
        if "pg_advisory_unlock" in sql:
            return Rows([])
        if "to_regclass" in sql:
            return Rows([{"relation": "ops.sync_state"}])
        if "FROM ops.sync_state" in sql:
            assert params == (repair.CURSOR_KEY,)
            return Rows([self.state] if self.state else [])
        if "FROM oracle.oracle_events" in sql:
            start, end, source = params
            return Rows([deepcopy(row) for row in self.rows.values() if start <= row["block_number"] <= end and row["source_oracle"] == source])
        raise AssertionError(sql)

    def commit(self):
        self.committed = (deepcopy(self.rows), deepcopy(self.state), deepcopy(self.receipts))

    def rollback(self):
        self.rollback_count += 1
        self.rows, self.state, self.receipts = deepcopy(self.committed)


class Rpc:
    def __init__(self, logs=()):
        self.events = list(logs)
        self.ranges = []
        self.timestamp_blocks = []
        self.limit = None
        self.failure_start = None

    def call(self, method):
        assert method == "eth_chainId"
        return hex(137)

    def head(self):
        return 1000

    def logs(self, start, end, *, addresses, topics):
        assert addresses == [oracle.CTF]
        assert topics == [list(oracle.CTF_BY_TOPIC)]
        self.ranges.append((start, end))
        if self.limit and end - start + 1 > self.limit:
            raise ConnectionError("RPC: query returned more than 10000 results")
        if self.failure_start is not None and start >= self.failure_start:
            raise ConnectionError("RPC: timeout")
        return [log for log in self.events if start <= oracle._int(log["blockNumber"]) <= end]

    def blocks(self, numbers):
        self.timestamp_blocks.extend(numbers)
        return [{"timestamp": hex(1700000000 + number)} for number in numbers]


@pytest.fixture(autouse=True)
def storage(monkeypatch):
    monkeypatch.setattr(oracle, "_load_markets", lambda *_args, **_kwargs: oracle._market_index(()))

    def write(conn, rows):
        for row in rows:
            conn.rows[repair._key(row)] = deepcopy(row)
            conn.writes += 1

    def set_state(conn, key, *, value, last_block):
        conn.state_keys.append(key)
        conn.state = {"value": json.dumps(value), "last_block": last_block}

    monkeypatch.setattr(oracle, "_write_oracle_events", write)
    monkeypatch.setattr(repair, "set_state", set_state)


def _record(log):
    return oracle._updown_records(None, [log], {oracle._int(log["blockNumber"]): datetime.now(timezone.utc)})[0]


def test_dry_run_reports_missing_request_without_writes_or_cursor():
    rpc, conn = Rpc([_log()]), Connection()
    stats = repair.run_repair(rpc, conn, from_block=100, to_block=100)
    assert stats["last_window"]["before"] == {"missing": 1, "extra": 0, "mismatched": 0}
    assert stats["unverified_windows"] == 1
    assert not stats["complete"]
    assert conn.writes == 0 and conn.state is None
    assert not conn.ddl and not conn.receipts
    assert not rpc.timestamp_blocks


def test_repair_verifies_missing_request_and_wrong_payout_before_checkpoint():
    logs = [_log(), _log(index=2, settle=True)]
    wrong = _record(logs[1])
    wrong["payout"] = "[1, 0]"
    rpc, conn = Rpc(logs), Connection([wrong])
    stats = repair.run_repair(rpc, conn, from_block=100, to_block=100, apply=True)
    assert stats["last_window"]["before"] == {"missing": 1, "extra": 0, "mismatched": 1}
    assert stats["complete"] and stats["verified_events"] == 2
    assert stats["written_events"] == 2 and len(conn.rows) == 2
    assert conn.state_keys == [repair.CURSOR_KEY]
    assert rpc.timestamp_blocks == [100]
    assert conn.rows[repair._key(wrong)]["payout"] == "[0, 1]"
    assert conn.receipts[repair.CURSOR_KEY, 100, 100] == stats["last_window"]


def test_matching_events_skip_writes_and_timestamp_calls():
    log = _log(settle=True)
    rpc, conn = Rpc([log]), Connection([_record(log)])
    stats = repair.run_repair(rpc, conn, from_block=100, to_block=100, apply=True)
    assert stats["complete"] and stats["verified_events"] == 1
    assert not conn.writes and not rpc.timestamp_blocks


@pytest.mark.parametrize("field,value", [("payout", "broken-json"), ("event_time", None)])
def test_invalid_projection_or_missing_timestamp_is_repaired(field, value):
    log = _log(settle=True)
    row = _record(log)
    row[field] = value
    rpc, conn = Rpc([log]), Connection([row])
    stats = repair.run_repair(rpc, conn, from_block=100, to_block=100, apply=True)
    assert stats["complete"] and stats["written_events"] == 1
    assert conn.rows[repair._key(row)]["event_time"] == datetime.fromtimestamp(1700000100, timezone.utc)
    assert conn.rows[repair._key(row)]["payout"] == "[0, 1]"


def test_extra_database_event_rolls_back_partial_repair_and_cursor():
    conn = Connection([_record(_log(index=2))])
    with pytest.raises(RuntimeError, match="verification failed"):
        repair.run_repair(Rpc([_log()]), conn, from_block=100, to_block=100, apply=True)
    assert len(conn.rows) == 1
    assert conn.state is None and conn.state_keys == []
    assert not conn.receipts
    assert conn.rollback_count > 0


def test_rpc_failure_keeps_only_previously_verified_window_and_resumes():
    rpc, conn = Rpc([_log(100), _log(101)]), Connection()
    rpc.failure_start = 101
    with pytest.raises(ConnectionError, match="timeout"):
        repair.run_repair(rpc, conn, from_block=100, to_block=101, window_blocks=1, apply=True)
    assert conn.state["last_block"] == 100 and len(conn.rows) == 1
    assert list(conn.receipts) == [(repair.CURSOR_KEY, 100, 100)]
    rpc.failure_start = None
    rpc.ranges.clear()
    stats = repair.run_repair(rpc, conn, to_block=101, window_blocks=1, apply=True)
    assert rpc.ranges == [(101, 101)]
    assert stats["verified_windows"] == 2 and stats["verified_events"] == 2
    assert stats["complete"] and conn.state["last_block"] == 101
    assert list(conn.receipts) == [(repair.CURSOR_KEY, 100, 100), (repair.CURSOR_KEY, 101, 101)]


def test_cursor_failure_rolls_back_rows_and_already_staged_receipt(monkeypatch):
    def fail_cursor(*_args, **_kwargs):
        raise RuntimeError("cursor write failed")

    monkeypatch.setattr(repair, "set_state", fail_cursor)
    conn = Connection()
    with pytest.raises(RuntimeError, match="cursor write failed"):
        repair.run_repair(Rpc([_log()]), conn, from_block=100, to_block=100, apply=True)
    assert not conn.rows and not conn.receipts and conn.state is None


def test_audit_fails_on_null_timestamp_without_writes_or_ddl():
    log = _log()
    row = _record(log)
    row["event_time"] = None
    rpc, conn = Rpc([log]), Connection([row])
    stats = repair.run_repair(rpc, conn, from_block=100, to_block=100)
    assert stats["last_window"]["after_missing_timestamps"] == 1
    assert stats["last_window"]["verified"] is False and not stats["complete"]
    assert not conn.writes and not conn.ddl and not conn.receipts
    assert not rpc.timestamp_blocks and conn.state is None


def test_repaired_timestamp_must_match_rpc_after_database_write(monkeypatch):
    def corrupt_time(conn, rows):
        for row in rows:
            conn.rows[repair._key(row)] = dict(row, event_time=datetime(1970, 1, 1, tzinfo=timezone.utc))

    monkeypatch.setattr(oracle, "_write_oracle_events", corrupt_time)
    conn = Connection()
    with pytest.raises(RuntimeError, match='"after_timestamp_mismatches": 1'):
        repair.run_repair(Rpc([_log()]), conn, from_block=100, to_block=100, apply=True)
    assert not conn.rows and not conn.receipts and conn.state is None


def test_provider_result_limit_splits_contiguously_and_max_windows_is_resumable():
    rpc, conn = Rpc([_log(100), _log(103)]), Connection()
    rpc.limit = 2
    first = repair.run_repair(rpc, conn, from_block=100, to_block=103, window_blocks=4, max_windows=1, apply=True)
    assert not first["complete"] and first["last_block"] == 101
    assert rpc.ranges == [(100, 103), (100, 101)]
    rpc.ranges.clear()
    second = repair.run_repair(rpc, conn, to_block=103, window_blocks=4, apply=True)
    assert second["complete"] and rpc.ranges == [(102, 103)]
    assert second["verified_events"] == 2


def test_single_block_result_limit_and_arbitrary_rpc_errors_do_not_get_silenced():
    class FailingRpc(Rpc):
        def logs(self, start, end, **_kwargs):
            self.ranges.append((start, end))
            raise ConnectionError(self.error)

    for error in ("timeout", "rate limit exceeded", "query returned more than 10000 results"):
        rpc = FailingRpc()
        rpc.error = error
        with pytest.raises(ConnectionError, match=error):
            list(repair._fetch_windows(rpc, 100, 100 if "query" in error else 200))
        assert len(rpc.ranges) == 1


def test_decoder_filter_and_timestamp_failure_cannot_advance_cursor(monkeypatch):
    conn = Connection()
    original = oracle._updown_records
    monkeypatch.setattr(oracle, "_updown_records", lambda *_args: [])
    with pytest.raises(RuntimeError, match="omitted chain events"):
        repair.run_repair(Rpc([_log()]), conn, from_block=100, to_block=100, apply=True)
    assert conn.state is None and not conn.rows
    monkeypatch.setattr(oracle, "_updown_records", original)
    rpc = Rpc([_log()])
    rpc.blocks = lambda _numbers: [None]
    with pytest.raises(RuntimeError, match="missing Polygon block timestamp"):
        repair.run_repair(rpc, conn, from_block=100, to_block=100, apply=True)
    assert conn.state is None and not conn.rows


def test_resume_rejects_changed_origin_and_unconfirmed_target():
    conn = Connection()
    repair.run_repair(Rpc(), conn, from_block=100, to_block=100, apply=True)
    with pytest.raises(RuntimeError, match="cursor origin"):
        repair.run_repair(Rpc(), conn, from_block=99, to_block=101, apply=True)
    with pytest.raises(ValueError, match="confirmed head"):
        repair.run_repair(Rpc(), conn, to_block=1000, apply=True)
    assert conn.state["last_block"] == 100


def test_default_start_is_ctf_deployment_and_never_uma_legacy_start(monkeypatch):
    rpc, conn = Rpc(), Connection()
    rpc.head = lambda: repair.CTF_START_BLOCK + oracle.CONFIRMATIONS
    monkeypatch.setattr(oracle, "get_state", lambda *_args: pytest.fail("legacy cursor read"))
    stats = repair.run_repair(rpc, conn, max_windows=1)
    assert stats["from_block"] == 4_023_686
    assert rpc.ranges == [(4_023_686, 4_023_686)]
