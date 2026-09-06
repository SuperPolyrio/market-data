from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json

from eth_abi import encode
import pytest

from market_data import oracle, oracle_modules as modules


BLOCK = 87_480_000
NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)


def _condition(module=1, nonce=1):
    return ((module << 248) | (nonce << 120)).to_bytes(32, "big")[:31]


def _log(abi=modules.CONDITION_RESOLVED, indexed=None, plain=None, *, address=modules.BINARY_MODULE, block=BLOCK, index=1):
    indexed = [_condition()] if indexed is None else indexed
    plain = [[0, 1_000_000]] if plain is None else plain
    indexed_types = [item["type"] for item in abi["inputs"] if item["indexed"]]
    plain_types = [item["type"] for item in abi["inputs"] if not item["indexed"]]
    return {
        "address": address, "blockNumber": hex(block), "logIndex": hex(index),
        "transactionIndex": "0x0", "transactionHash": "0x" + f"{block:064x}",
        "blockHash": "0x" + "99" * 32, "removed": False,
        "topics": [oracle._topic(abi), *["0x" + encode([kind], [value]).hex() for kind, value in zip(indexed_types, indexed)]],
        "data": "0x" + encode(plain_types, plain).hex(),
    }


def test_exact_bytes31_resolution_payout_and_provenance():
    log = _log()
    row = modules._decode(log, NOW)
    assert row["condition_id"] == "0x" + _condition().hex()
    assert len(row["condition_id"]) == 64
    assert row["event_status"] == "settle" and row["event_name"] == "ConditionResolved"
    assert row["payouts_json"] == [0, 1_000_000]
    assert row["payload_json"]["topics"] == log["topics"]
    assert row["payload_json"]["data"] == log["data"]
    assert row["payload_json"]["abi_sha256"] == modules.MODULES[modules.BINARY_MODULE][2]


def test_reports_migration_mapping_and_upgrades_do_not_create_duplicate_settlement():
    report = modules._decode(_log(modules.RESULT_REPORTED, ["0x" + "11" * 20, _condition()], [[500_000, 500_000]]), NOW)
    assert report["event_status"] == "report"
    registration = modules._decode(_log(modules.MIGRATION_REGISTERED, [_condition(), bytes.fromhex("22" * 32)], []), NOW)
    assert registration["event_status"] == "prepare"
    assert registration["payload_json"]["args"]["legacyConditionId"] == "0x" + "22" * 32
    upgraded = modules._decode(_log(modules.UPGRADED, ["0x" + "33" * 20], [], address=modules.COMBINATORIAL_MODULE), NOW)
    assert upgraded["event_status"] == "upgrade" and upgraded["condition_id"] is None
    assert upgraded["payload_json"]["args"]["implementation"].lower() == "0x" + "33" * 20


@pytest.mark.parametrize("payouts", [[], [1_000_000], [0, 1], [1_000_000, 1], [0, 0, 1_000_000]])
def test_invalid_terminal_payout_fails_closed(payouts):
    with pytest.raises(ValueError, match="two entries"):
        modules._decode(_log(plain=[payouts]), NOW)


def test_combo_legs_preserve_order_tokens_and_condition_ids():
    legs = [(1 << 248) | (1 << 120), (2 << 248) | (2 << 120) | 1]
    condition = modules.combo_condition_id(legs)
    # Independent production getConditionId(uint256[]) eth_call, 2026-09-06.
    assert condition == "0x03887b8b3c59cdde1aa4867afb749cdfbf0000000000000000000000000000"
    log = _log(modules.COMBINATORIAL_PREPARED, [bytes.fromhex(condition[2:])], [legs], address=modules.COMBINATORIAL_MODULE)
    row = modules._decode(log, NOW)
    assert row["condition_id"] == condition and row["payouts_json"] == []
    assert row["event_status"] == "prepare"
    assert row["legs_json"] == [
        {"position_id": str(legs[0]), "condition_id": "0x" + _condition(1, 1).hex(), "module_id": 1, "outcome_index": 0},
        {"position_id": str(legs[1]), "condition_id": "0x" + _condition(2, 2).hex(), "module_id": 2, "outcome_index": 1},
    ]


@pytest.mark.parametrize("legs", [[], [1 << 248] * 51, [1 << 248, (1 << 248) | 1], [2 << 248, 1 << 248], [3 << 248], [(1 << 248) | 2]])
def test_combo_invalid_definitions_are_rejected(legs):
    with pytest.raises(ValueError):
        modules._legs(legs, modules.combo_condition_id(legs))


def test_combo_id_and_source_module_must_match():
    with pytest.raises(ValueError, match="does not match"):
        modules._legs([1 << 248], "0x" + _condition(3).hex())
    with pytest.raises(ValueError, match="condition module ID"):
        modules._decode(_log(indexed=[_condition(2)]), NOW)
    with pytest.raises(ValueError, match="module ABI"):
        modules._decode(_log(indexed=[_condition(3)], address=modules.COMBINATORIAL_MODULE), NOW)


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class Connection:
    def __init__(self):
        self.rows = {}
        self.receipts = {}
        self.observations = {}
        self.state = None
        self.ddl = []
        self.committed = deepcopy((self.rows, self.receipts, self.state, self.observations))

    def execute(self, sql, params=()):
        if sql.startswith("CREATE "):
            self.ddl.append(sql)
            return Rows([])
        if "pg_try_advisory_lock" in sql:
            return Rows([{"locked": True}])
        if "pg_advisory_xact_lock" in sql:
            return Rows([])
        if "pg_advisory_unlock" in sql:
            return Rows([])
        if "to_regclass" in sql:
            return Rows([{"relation": "present"}])
        if "FROM ops.sync_state" in sql:
            assert params == (modules.CURSOR_KEY,)
            return Rows([self.state] if self.state else [])
        if "FROM oracle.module_events" in sql:
            return Rows([deepcopy(row) for row in self.rows.values() if params[0] <= row["block_number"] <= params[1]])
        if "INSERT INTO ops.oracle_module_backfill_windows" in sql:
            key, low, high, digest, receipt = params
            receipt = json.loads(receipt)
            assert receipt["verified"] and receipt["events_sha256"] == digest
            self.receipts[key, low, high] = receipt
            return Rows([])
        if "INSERT INTO oracle.module_implementation_observations" in sql:
            assert "block_number <= EXCLUDED.block_number" in sql
            current = self.observations.get(params["module_address"])
            if current is None or current["block_number"] <= params["block_number"]:
                self.observations[params["module_address"]] = deepcopy(params)
            return Rows([])
        raise AssertionError(sql)

    def commit(self):
        self.committed = deepcopy((self.rows, self.receipts, self.state, self.observations))

    def rollback(self):
        self.rows, self.receipts, self.state, self.observations = deepcopy(self.committed)


class Rpc:
    def __init__(self, logs=()):
        self.events = logs
        self.ranges = []
        self.fail = None
        self.limit = None
        self.storage_calls = []
        self.storage_values = None

    def call(self, method):
        assert method == "eth_chainId"
        return hex(137)

    def head(self):
        return BLOCK + 100

    def logs(self, start, end, *, addresses, topics):
        self.ranges.append((start, end))
        assert set(addresses) <= set(modules.MODULES)
        assert topics == [list(modules.EVENTS)]
        if self.fail is not None and start >= self.fail:
            raise ConnectionError("RPC unavailable")
        if self.limit and end - start + 1 > self.limit:
            raise ConnectionError("query returned more than 50000 results")
        return [log for log in self.events if start <= oracle._int(log["blockNumber"]) <= end]

    def blocks(self, numbers):
        return [{"timestamp": hex(int(NOW.timestamp()))} for _ in numbers]


    def block(self, number):
        return {"number": hex(number), "hash": "0x" + "99" * 32}

    def batch_call(self, method, params):
        assert method == "eth_getStorageAt"
        params = list(params)
        self.storage_calls.append(params)
        return self.storage_values if self.storage_values is not None else [
            "0x" + "00" * 12 + modules.MODULES[row[0]][1][2:] for row in params
        ]


@pytest.fixture
def storage(monkeypatch):
    def write(conn, rows):
        for row in rows:
            conn.rows[row["tx_hash"], row["log_index"]] = deepcopy(row)

    def state(conn, key, *, value, last_block):
        assert key == modules.CURSOR_KEY and value["last_block"] == last_block
        conn.state = {"value": json.dumps(value)}

    monkeypatch.setattr(modules, "_write", write)
    monkeypatch.setattr(modules, "set_state", state)


def test_read_only_audit_has_no_ddl_or_cursor(storage):
    conn = Connection()
    stats = modules.run(Rpc([_log()]), conn, from_block=BLOCK, to_block=BLOCK)
    assert stats["unverified_windows"] == 1 and not stats["complete"]
    assert not conn.ddl and not conn.rows and not conn.receipts and conn.state is None
    assert not conn.observations


def test_missing_metadata_does_not_filter_module_events_and_resume_is_atomic(storage):
    conn, rpc = Connection(), Rpc([_log(), _log(block=BLOCK + 1)])
    rpc.fail = BLOCK + 1
    with pytest.raises(ConnectionError, match="unavailable"):
        modules.run(rpc, conn, from_block=BLOCK, to_block=BLOCK + 1, window_blocks=1, apply=True)
    assert len(conn.rows) == len(conn.receipts) == 1
    assert json.loads(conn.state["value"])["last_block"] == BLOCK
    rpc.fail = None
    rpc.ranges.clear()
    stats = modules.run(rpc, conn, to_block=BLOCK + 1, apply=True)
    assert stats["complete"] and stats["module_events"] == 2
    assert rpc.ranges == [(BLOCK + 1, BLOCK + 1)]
    assert len(conn.rows) == len(conn.receipts) == 2


def test_failed_cursor_rolls_back_staged_receipt_and_events(storage, monkeypatch):
    def fail(*_args, **_kwargs):
        raise RuntimeError("cursor failure")

    monkeypatch.setattr(modules, "set_state", fail)
    conn = Connection()
    with pytest.raises(RuntimeError, match="cursor failure"):
        modules.run(Rpc([_log()]), conn, from_block=BLOCK, to_block=BLOCK, apply=True)
    assert not conn.rows and not conn.receipts and conn.state is None
    assert not conn.observations


def test_semantic_verification_includes_timestamp_and_rolls_back(storage, monkeypatch):
    def corrupt(conn, rows):
        for row in rows:
            conn.rows[row["tx_hash"], row["log_index"]] = dict(row, event_time=None)

    monkeypatch.setattr(modules, "_write", corrupt)
    conn = Connection()
    with pytest.raises(RuntimeError, match="verification failed"):
        modules.run(Rpc([_log()]), conn, from_block=BLOCK, to_block=BLOCK, apply=True)
    assert not conn.rows and not conn.receipts and conn.state is None


def test_provider_limit_splits_but_arbitrary_rpc_errors_fail(storage):
    rpc, conn = Rpc(), Connection()
    rpc.limit = 1
    stats = modules.run(rpc, conn, from_block=BLOCK, to_block=BLOCK + 1, window_blocks=2, apply=True)
    assert stats["complete"] and stats["windows"] == 2
    assert rpc.ranges == [(BLOCK, BLOCK + 1), (BLOCK, BLOCK), (BLOCK + 1, BLOCK + 1)]
    rpc.fail = BLOCK
    with pytest.raises(ConnectionError, match="unavailable"):
        list(modules._fetch_windows(rpc, BLOCK, BLOCK))


def test_deployment_starts_match_verified_proxy_origins():
    assert modules.MODULE_STARTS == {
        modules.BINARY_MODULE: 87_479_502,
        modules.NEG_RISK_MODULE: 87_479_533,
        modules.COMBINATORIAL_MODULE: 87_479_439,
    }
    assert modules.DEPLOYMENT_PROOF_SHA256 == "c1fa03b74aeb3bcf1f1c89620a0e475b6fe6b0f40ff5c73af6eb5bb72924d5dc"


def test_implementation_observations_use_explicit_block_and_preserve_newer_state():
    rpc, conn = Rpc(), Connection()
    rows = modules.observe_implementations(rpc, conn, BLOCK + 1)
    assert len(rows) == len(conn.observations) == 3
    assert rpc.storage_calls == [[[address, modules.IMPLEMENTATION_SLOT, hex(BLOCK + 1)] for address in modules.MODULES]]
    assert all(row["block_hash"] == "0x" + "99" * 32 and row["source"] == "rpc_storage_observation" for row in rows)
    assert not conn.rows and not conn.receipts
    modules.observe_implementations(rpc, conn, BLOCK)
    assert all(row["block_number"] == BLOCK + 1 for row in conn.observations.values())


def test_unknown_nonzero_implementation_is_observed_for_evidence_to_reject():
    rpc, conn = Rpc(), Connection()
    rpc.storage_values = ["0x" + "00" * 12 + "11" * 20] * 3
    rows = modules.observe_implementations(rpc, conn, BLOCK)
    assert all(row["implementation"] == "0x" + "11" * 20 for row in rows)
    assert len(conn.observations) == 3 and not conn.rows


@pytest.mark.parametrize("values", [[], ["0x" + "00" * 32] * 3, ["0x1234"] * 3])
def test_missing_zero_or_malformed_storage_cannot_be_persisted(values):
    rpc, conn = Rpc(), Connection()
    rpc.storage_values = values
    with pytest.raises(RuntimeError, match="storage"):
        modules.observe_implementations(rpc, conn, BLOCK)
    assert not conn.observations and not conn.rows


def test_observation_requires_confirmed_stable_block_hash():
    rpc, conn = Rpc(), Connection()
    with pytest.raises(ValueError, match="confirmed block"):
        modules.observe_implementations(rpc, conn, rpc.head())
    rpc.block = lambda number: {"number": hex(number)}
    with pytest.raises(RuntimeError, match="header/hash"):
        modules.observe_implementations(rpc, conn, BLOCK)
    calls = []

    def changed(number):
        calls.append(number)
        return {"number": hex(number), "hash": "0x" + ("99" if len(calls) == 1 else "88") * 32}

    rpc.block = changed
    with pytest.raises(RuntimeError, match="block changed"):
        modules.observe_implementations(rpc, conn, BLOCK)
    assert not conn.observations and not conn.rows
