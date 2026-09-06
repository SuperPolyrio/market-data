from __future__ import annotations

from datetime import datetime, timezone
import inspect

import pytest
from eth_abi import encode

import market_data.oracle as oracle


def _topic_value(kind: str, value):
    if kind == "address":
        return "0x" + "00" * 12 + str(value).lower().removeprefix("0x")
    if kind == "bytes32":
        raw = bytes(value)
        return "0x" + raw.hex()
    if kind.startswith("uint"):
        return "0x" + int(value).to_bytes(32, "big").hex()
    raise AssertionError(kind)


def _log(abi, indexed, unindexed, *, address, block=100, index=2):
    indexed_inputs = [item for item in abi["inputs"] if item["indexed"]]
    plain_inputs = [item for item in abi["inputs"] if not item["indexed"]]
    return {
        "address": address.lower(),
        "topics": [
            oracle._topic(abi),
            *[
                _topic_value(item["type"], value)
                for item, value in zip(indexed_inputs, indexed)
            ],
        ],
        "data": "0x" + encode(
            [item["type"] for item in plain_inputs], unindexed
        ).hex(),
        "blockNumber": hex(block),
        "transactionIndex": "0x0",
        "logIndex": hex(index),
        "transactionHash": "0x" + "ab" * 32,
        "blockHash": "0x" + "cd" * 32,
        "removed": False,
    }


def test_manifest_keeps_all_official_oracle_eras_and_requesters():
    assert oracle.OFFICIAL_MANIFEST_COMMIT == "75d1818547862a5bd3477ed2e6b16f693d42dab6"
    assert set(oracle.ORACLE_STARTS) == {
        oracle.OLD_ORACLE,
        oracle.OO_V2_OLD,
        oracle.OO_V2,
    }
    assert oracle.REQUESTERS_BY_ORACLE[oracle.OLD_ORACLE] == {oracle.OLD_ADAPTER}
    assert oracle.SPORTS_REQUESTER in oracle.REQUESTERS_BY_ORACLE[oracle.OO_V2]


def test_live_and_backfill_use_distinct_durable_cursors():
    assert oracle._state_keys("live") == ("oracle_sync", "oracle_updown_sync_live")
    assert oracle._state_keys("backfill") == (
        "oracle_backfill_full",
        "oracle_backfill_updown",
    )
    assert oracle.UMA_LIVENESS_KEY == "oracle_sync_live"
    assert oracle.UMA_LIVENESS_KEY not in oracle._state_keys("live")


def test_uma_request_is_decoded_and_non_polymarket_requester_is_rejected():
    requester = "0x6a9d222616c90fca5754cd1333cfd9b7fb6a4f74"
    log = _log(
        oracle.REQUEST,
        [requester],
        [
            b"YES_OR_NO_QUERY".ljust(32, b"\0"),
            1_700_000_000,
            b"title: Will it rain?, description: test, market_id: 123, p1: 1, p2: 2",
            "0x" + "22" * 20,
            10,
            20,
        ],
        address=oracle.OO_V2,
    )
    assert oracle._authorized(log)
    row = oracle._decode_uma(log, datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert row["status"] == "request"
    assert row["gamma"] == "123"
    assert row["title"] == "Will it rain?"
    assert row["p1"] == "1"
    assert row["p2"] == "2"

    log["topics"][1] = "0x" + "00" * 12 + "99" * 20
    assert not oracle._authorized(log)


def test_ancillary_preserves_bytes_and_escapes_postgres_nul():
    encoded, readable = oracle._ancillary(b"title: a\x00b")

    assert encoded == "0x7469746c653a20610062"
    assert readable == r"title: a\x00b"
    assert "\x00" not in readable


def test_current_question_initialized_and_neg_risk_bridge_decode():
    qid = bytes.fromhex("11" * 32)
    adapter = "0x6a9d222616c90fca5754cd1333cfd9b7fb6a4f74"
    initialized = _log(
        oracle.QUESTION_INITIALIZED,
        [qid, 1_700_000_000, "0x" + "33" * 20],
        [b"title: test, market_id: 9", "0x" + "44" * 20, 1, 2],
        address=adapter,
    )
    row = oracle._decode_adapter(
        initialized, datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    assert row is not None
    assert row["event_status"] == "adapter_question_initialized"
    assert row["question_id"] == "0x" + "11" * 32
    assert row["ancillary_hex"].startswith("0x7469746c653a")
    assert row["source_adapter"] == adapter

    prepared = _log(
        oracle.QUESTION_PREPARED,
        [bytes.fromhex("55" * 32), bytes.fromhex("66" * 32), qid],
        [0, b"bridge"],
        address=oracle.NEG_RISK_OPERATORS[0],
    )
    mapping = oracle._decode_prepared(prepared)
    assert mapping == {
        "request_id": "0x" + "11" * 32,
        "question_id": "0x" + "66" * 32,
        "market_id": "0x" + "55" * 32,
        "source_operator": oracle.NEG_RISK_OPERATORS[0],
    }


def test_ctf_resolution_decodes_payouts():
    condition = bytes.fromhex("77" * 32)
    question = bytes.fromhex("88" * 32)
    log = _log(
        oracle.CONDITION_RESOLUTION,
        [condition, "0x" + "99" * 20, question],
        [2, [0, 1]],
        address=oracle.CTF,
    )
    args = oracle._decode(oracle.CONDITION_RESOLUTION, log)
    assert oracle._hex(args["conditionId"]) == "0x" + "77" * 32
    assert oracle._address(args["oracle"]) == "0x" + "99" * 20
    assert list(args["payoutNumerators"]) == [0, 1]


def test_market_bridge_never_guesses_an_ambiguous_question():
    rows = [
        {"id": 1, "gamma_market_id": "10", "condition_id": "0xaa", "question_id": "0x01"},
        {"id": 2, "gamma_market_id": "20", "condition_id": "0xbb", "question_id": "0x01"},
    ]
    index = oracle._market_index(rows)
    assert "0x01" not in index["question"]
    assert oracle._bridge(index, "10", "")[0]["id"] == 1
    assert oracle._bridge(index, "", "0xbb")[0]["id"] == 2
    assert oracle._bridge(index, "", "0x01") == (None, "")


def test_live_cursor_uses_slower_domain_and_rewinds(monkeypatch):
    values = {
        oracle.UMA_LIVE_KEY: {"last_block": 90_000_000},
        oracle.UPDOWN_LIVE_KEY: {"last_block": 89_999_900},
    }
    monkeypatch.setattr(oracle, "get_state", lambda _conn, key: values.get(key))
    assert oracle._start_block(
        object(), mode="live", target=91_000_000, explicit=None, rewind=100
    ) == 89_999_801


def test_backfill_without_cursor_starts_at_pinned_history(monkeypatch):
    monkeypatch.setattr(oracle, "get_state", lambda _conn, _key: None)
    assert oracle._start_block(
        object(), mode="backfill", target=90_000_000, explicit=None, rewind=100
    ) == oracle.FULL_HISTORY_START


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _ConflictConnection:
    def execute(self, *_args, **_kwargs):
        return _Rows([{
            "request_id": "0x01",
            "question_id": "0xold",
            "market_id": "",
            "source_operator": "",
        }])


def test_neg_risk_mapping_conflict_fails_closed():
    with pytest.raises(RuntimeError, match="conflicting QuestionPrepared"):
        oracle._write_neg_map(_ConflictConnection(), [{
            "request_id": "0x01",
            "question_id": "0xnew",
            "market_id": "",
            "source_operator": "",
        }])


def test_oracle_writer_is_tx_log_idempotent():
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def executemany(self, sql, rows):
            self.sql = sql
            self.rows = rows

    cursor = Cursor()
    conn = type("Conn", (), {"cursor": lambda self: cursor})()
    oracle._write_oracle_events(conn, [{"tx_hash": "0x1"}])
    assert "ON CONFLICT (tx_hash, log_index) DO UPDATE" in cursor.sql


def test_runtime_ddl_and_projection_sql_have_one_key_clause():
    class Connection:
        def __init__(self):
            self.sql = []

        def execute(self, sql, *_args):
            self.sql.append(sql)
            if "to_regclass" in sql:
                return _Rows([{"ready": False}])
            return _Rows([])

    conn = Connection()
    oracle._ensure_schema(conn)
    ddl = "\n".join(conn.sql)
    assert ddl.count("request_id TEXT PRIMARY KEY") == 1

    projection = Connection()
    oracle._refresh_adapter_projection(projection, ["0x01"])
    statement = projection.sql[0]
    assert statement.count("WHERE event_status") == 1
    assert statement.count("payload_json->>'ancillary_data' = ANY") == 1
    assert inspect.getsource(oracle.collect_window).count(
        '_bridge(lifecycle_markets, "", lookup)'
    ) == 1


@pytest.mark.parametrize('abi,plain,status,payout', [
    (oracle.CONDITION_PREPARATION, [2], 'request', ''),
    (oracle.CONDITION_RESOLUTION, [2, [1, 1]], 'settle', '[1, 1]'),
])
def test_ctf_events_survive_late_or_missing_market(monkeypatch, abi, plain, status, payout):
    question = bytes.fromhex('88' * 32)
    condition = bytes.fromhex('77' * 32)
    log = _log(abi, [condition, '0x' + '99' * 20, question], plain, address=oracle.CTF)
    monkeypatch.setattr(oracle, '_load_markets', lambda *_a, **_k: oracle._market_index(()))
    rows = oracle._updown_records(object(), [log], {100: datetime(2026, 1, 1, tzinfo=timezone.utc)})
    assert len(rows) == 1
    assert rows[0]['market_id'] is None
    assert rows[0]['event_status'] == status
    assert rows[0]['condition_id'] == '0x' + condition.hex()
    assert rows[0]['question_id'] == '0x' + question.hex()
    assert rows[0]['payout'] == payout


def test_ctf_condition_cannot_be_replaced_by_shared_question(monkeypatch):
    question = bytes.fromhex('88' * 32)
    condition = bytes.fromhex('77' * 32)
    log = _log(oracle.CONDITION_PREPARATION,
               [condition, '0x' + '99' * 20, question], [2], address=oracle.CTF)
    other = {'id': 7, 'question_id': '0x' + question.hex(), 'condition_id': '0x' + '66' * 32}
    monkeypatch.setattr(oracle, '_load_markets', lambda *_a, **_k: oracle._market_index([other]))
    row = oracle._updown_records(object(), [log], {100: datetime(2026, 1, 1, tzinfo=timezone.utc)})[0]
    assert row['market_id'] is None
    assert row['condition_id'] == '0x' + condition.hex()


def test_fetch_splits_provider_limits_but_propagates_transport_failure():
    class LimitedRpc:
        def logs(self, start, end, **_kwargs):
            if start != end:
                raise ConnectionError('Query returned more than 50000 results')
            return [{'blockNumber': hex(start), 'removed': False}]
    assert [r['blockNumber'] for r in oracle._fetch(LimitedRpc(), 1, 4, [oracle.CTF], oracle.CTF_BY_TOPIC)] == ['0x1', '0x2', '0x3', '0x4']
    class UnavailableRpc:
        def logs(self, *_args, **_kwargs):
            raise ConnectionError('upstream unavailable')
    with pytest.raises(ConnectionError, match='upstream unavailable'):
        oracle._fetch(UnavailableRpc(), 1, 4, [oracle.CTF], oracle.CTF_BY_TOPIC)


def test_log_timestamps_reuse_metadata_and_fetch_only_missing_headers():
    class Rpc:
        def blocks(self, numbers):
            assert numbers == [2]
            return [{'number': '0x2', 'timestamp': '0x66', 'hash': '0xbb'}]
    logs = [
        {'blockNumber': '0x1', 'blockTimestamp': '0x65', 'blockHash': '0xaa'},
        {'blockNumber': '0x1', 'blockTimestamp': '0x65', 'blockHash': '0xaa'},
        {'blockNumber': '0x2', 'blockHash': '0xbb'},
    ]
    assert {k:int(v.timestamp()) for k,v in oracle._timestamps(Rpc(), logs).items()} == {1:101, 2:102}
    logs[1]['blockTimestamp'] = '0x67'
    with pytest.raises(RuntimeError, match='conflicting Polygon block timestamp'):
        oracle._timestamps(Rpc(), logs)
    logs[1]['blockTimestamp'] = '0x65'
    logs[1]['blockHash'] = '0xcc'
    with pytest.raises(RuntimeError, match='conflicting Polygon block hashes'):
        oracle._timestamps(Rpc(), logs)


def test_uma_backfill_never_advances_independent_ctf_or_module_cursors(monkeypatch):
    assert oracle._state_keys('backfill', uma_only=True) == (oracle.UMA_BACKFILL_KEY,)
    states = {oracle.UMA_BACKFILL_KEY: {'last_block': 80_000_000},
              oracle.UPDOWN_BACKFILL_KEY: {'last_block': 79_000_000}}
    monkeypatch.setattr(oracle, 'get_state', lambda _c, k: states.get(k))
    assert oracle._start_block(object(), mode='backfill', target=90_000_000,
                              explicit=None, rewind=100, uma_only=True) == 80_000_001
    with pytest.raises(ValueError, match='uma-only is restricted'):
        oracle.run(mode='live', uma_only=True)
