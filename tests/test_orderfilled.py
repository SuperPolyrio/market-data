from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from market_data import market, orderfilled


def word(value: int) -> str:
    return value.to_bytes(32, "big").hex()


def topic_address(value: str) -> str:
    return "0x" + value.removeprefix("0x").zfill(64)


def log(*, address: str, topic0: str, words: list[int], block: int = 100) -> dict:
    return {
        "address": address,
        "topics": [
            topic0,
            "0x" + "11" * 32,
            topic_address("aa" * 20),
            topic_address("bb" * 20),
        ],
        "data": "0x" + "".join(word(value) for value in words),
        "transactionHash": "0x" + "22" * 32,
        "logIndex": "0x3",
        "blockNumber": hex(block),
    }


@pytest.mark.parametrize(
    ("words", "side", "token", "price", "size"),
    [
        ([0, 77, 2_000_000, 4_000_000, 1], "BUY", "77", "0.5000000000", "4"),
        ([77, 0, 4_000_000, 2_000_000, 1], "SELL", "77", "0.5000000000", "4"),
    ],
)
def test_fast_decodes_legacy_buy_and_sell(words, side, token, price, size):
    decoded = orderfilled.decode_log(
        log(
            address=orderfilled.LEGACY_EXCHANGES[0],
            topic0=orderfilled.LEGACY_TOPIC,
            words=words,
        )
    )
    assert decoded["event_version"] == "legacy"
    assert decoded["side"] == side
    assert decoded["token_id"] == token
    assert decoded["price"] == price
    assert decoded["size"] == size


@pytest.mark.parametrize(
    ("side_value", "side", "amount0", "amount1"),
    [(0, "BUY", 2_000_000, 4_000_000), (1, "SELL", 4_000_000, 2_000_000)],
)
def test_fast_decodes_all_v2026_exchanges(side_value, side, amount0, amount1):
    for address in orderfilled.V2026_EXCHANGES:
        decoded = orderfilled.decode_log(
            log(
                address=address,
                topic0=orderfilled.V2026_TOPIC,
                words=[side_value, 88, amount0, amount1, 7, 9, 10],
            )
        )
        assert decoded["event_version"] == "v2026"
        assert decoded["side"] == side
        assert decoded["token_id"] == "88"
        assert decoded["maker_order_hash"] == "0x" + word(9)


def test_topic_cannot_be_used_on_wrong_exchange():
    with pytest.raises(orderfilled.DecodeError):
        orderfilled.decode_log(
            log(
                address=orderfilled.V2026_EXCHANGES[0],
                topic0=orderfilled.LEGACY_TOPIC,
                words=[0, 1, 1, 1, 0],
            )
        )


def test_exact_raw_audit_checks_keys_not_only_counts():
    chain = [
        {"contract": orderfilled.LEGACY_EXCHANGES[0], "tx_hash": "0x01", "log_index": 1},
        {"contract": orderfilled.LEGACY_EXCHANGES[0], "tx_hash": "0x02", "log_index": 2},
    ]
    wrong_same_count = [chain[0], {**chain[1], "tx_hash": "0x03"}]
    assert orderfilled.compare_raw_keys(chain, chain)["exact"] is True
    result = orderfilled.compare_raw_keys(chain, wrong_same_count)
    assert result["exact"] is False
    assert result["missing_keys"] == result["extra_keys"] == 1


def test_unknown_token_is_queued_not_projected_or_turned_into_market():
    raw = {
        "tx_hash": "0x01",
        "log_index": 1,
        "token_id": "999",
        "side": "BUY",
        "block_number": 10,
    }
    facts, unresolved = orderfilled.build_projection([raw], {})
    assert facts == []
    assert unresolved == [raw]
    source = inspect.getsource(orderfilled)
    assert "INSERT INTO core.markets" not in source
    assert "UPDATE core.markets" not in source


def test_projection_uses_registry_outcome_index_and_buy_sell_codes():
    base = {
        "tx_hash": "0x01",
        "log_index": 1,
        "token_id": "99",
        "side": "SELL",
        "block_number": 10,
        "condition_id": "unused",
        "maker": "0x" + "aa" * 20,
        "taker": "0x" + "bb" * 20,
        "price": "0.5",
        "size": "2",
        "order_hash": "0x" + "11" * 32,
        "contract": orderfilled.LEGACY_EXCHANGES[0],
        "maker_amount": "2000000",
        "taker_amount": "1000000",
        "fee": "0",
    }
    owners = {
        "99": {
            "market_id": 7,
            "condition_id": "0xcondition",
            "outcome": "NO",
            "outcome_index": 1,
            "identity_kind": "canonical_gamma",
        }
    }
    facts, unresolved = orderfilled.build_projection([base], owners)
    assert unresolved == []
    assert facts[0]["market_id"] == 7
    assert facts[0]["outcome_code"] == 2
    assert facts[0]["side_code"] == 2


def test_official_combo_projection_does_not_claim_yes_or_no():
    raw = {
        "tx_hash": "0x" + "01" * 32,
        "log_index": 1,
        "token_id": "99",
        "side": "BUY",
        "block_number": 10,
        "maker": "0x" + "aa" * 20,
        "taker": "0x" + "bb" * 20,
        "price": "0.5",
        "size": "2",
        "order_hash": "0x" + "11" * 32,
        "contract": orderfilled.V2026_EXCHANGES[-1],
        "maker_amount": "1000000",
        "taker_amount": "2000000",
        "fee": "0",
    }
    owners = {
        "99": {
            "market_id": 7,
            "condition_id": "0xcombo",
            "outcome": "position_0",
            "outcome_index": 0,
            "identity_kind": "official_combo",
        }
    }
    facts, unresolved = orderfilled.build_projection([raw], owners)
    assert unresolved == []
    assert facts[0]["outcome_code"] == 0


def test_e333_projection_never_inherits_legacy_yes_no_shell_labels():
    raw = {
        "tx_hash": "0x" + "02" * 32,
        "log_index": 2,
        "token_id": "99",
        "side": "SELL",
        "block_number": 11,
        "maker": "0x" + "aa" * 20,
        "taker": "0x" + "bb" * 20,
        "price": "0.5",
        "size": "2",
        "order_hash": "0x" + "11" * 32,
        "contract": orderfilled.V2026_EXCHANGES[-1],
        "maker_amount": "2000000",
        "taker_amount": "1000000",
        "fee": "0",
    }
    legacy_owner = {
        "99": {
            "market_id": 7,
            "condition_id": "0xcombo",
            "outcome": "YES",
            "outcome_index": 0,
            "identity_kind": "canonical_gamma",
        }
    }

    facts, unresolved = orderfilled.build_projection([raw], legacy_owner)

    assert unresolved == []
    assert facts[0]["side_code"] == 2
    assert facts[0]["outcome_code"] == 0


def test_registry_queries_exclude_retired_legacy_identities():
    source = inspect.getsource(orderfilled.load_token_owners) + inspect.getsource(
        orderfilled.retry_resolved_gaps
    )
    for identity_kind in ("legacy_placeholder", "legacy_ghost", "invalid_shell"):
        assert identity_kind in source


def test_clickhouse_schema_and_engine_have_no_derived_financial_tables():
    sql = (Path(__file__).parents[1] / "sql" / "clickhouse_orderfilled.sql").read_text().lower()
    source = inspect.getsource(orderfilled).lower()
    forbidden = ("cashflow", "pnl", "materialized view")
    for term in forbidden:
        assert term not in sql
        assert term not in source
    assert "orderfilled_fact" in sql
    assert "side_code uint8" in sql
    assert "buffer" not in sql


def test_decimal_token_id_is_written_as_uint256_hex(monkeypatch):
    writer = orderfilled.ClickHouseWriter(orderfilled.ClickHouseConfig())
    queries = []
    inserts = []
    monkeypatch.setattr(writer, "query", lambda query: queries.append(query) or "")
    monkeypatch.setattr(writer, "insert", lambda query, payload: inserts.append((query, payload)))
    row = {
        "tx_hash": "0x" + "11" * 32,
        "log_index": 1,
        "market_id": 2,
        "condition_id": "0xcondition",
        "token_id": "123456789",
        "outcome_code": 1,
        "maker": "0x" + "aa" * 20,
        "taker": "0x" + "bb" * 20,
        "side_code": 1,
        "price": "0.5",
        "size": "2",
        "block_number": 100,
        "order_hash": "0x" + "22" * 32,
        "contract": orderfilled.LEGACY_EXCHANGES[0],
        "maker_amount": "1000000",
        "taker_amount": "2000000",
        "fee": "0",
    }
    assert writer.write_facts([row]) == 1
    expected = f"{123456789:064x}"
    assert expected in inserts[0][1]
    assert all("orderfilled_fact_buffer" not in query for query in queries)
    assert "INSERT INTO orderfilled_fact" in inserts[0][0]


def test_fetch_window_gets_each_unique_block_timestamp_once():
    class FakeRpc:
        def __init__(self):
            self.block_calls = []

        def logs(self, *_args, **_kwargs):
            first = log(
                address=orderfilled.LEGACY_EXCHANGES[0],
                topic0=orderfilled.LEGACY_TOPIC,
                words=[0, 77, 2_000_000, 4_000_000, 1],
                block=100,
            )
            second = {**first, "transactionHash": "0x" + "33" * 32, "logIndex": "0x4"}
            return [first, second]

        def block(self, number):
            self.block_calls.append(number)
            return {"timestamp": hex(1_700_000_000)}

    rpc = FakeRpc()
    rows = orderfilled.fetch_decode_window(rpc, 100, 100)
    assert rpc.block_calls == [100]
    assert rows[0]["block_time"].isoformat() == "2023-11-14T22:13:20+00:00"
    assert rows[1]["block_time"] == rows[0]["block_time"]


def test_legacy_cursor_adoption_is_explicit_atomic_and_one_time(monkeypatch):
    states = {"trade_sync": {"last_block": 123}}

    class Conn:
        commits = 0
        rollbacks = 0

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    conn = Conn()
    monkeypatch.setattr(orderfilled, "get_state", lambda _conn, key: states.get(key))

    def set_state(_conn, key, *, last_block, value, **_kwargs):
        states[key] = {"last_block": last_block, "value": value}

    monkeypatch.setattr(orderfilled, "set_state", set_state)
    assert orderfilled.adopt_legacy_cursor(conn) == 123
    assert states[orderfilled.RAW_CURSOR]["last_block"] == 123
    assert states[orderfilled.FACT_CURSOR]["last_block"] == 123
    assert conn.commits == 1
    with pytest.raises(RuntimeError, match="after either new cursor exists"):
        orderfilled.adopt_legacy_cursor(conn)


def test_adopt_run_is_database_only_and_returns_before_rpc(monkeypatch, capsys):
    class Conn:
        def close(self):
            pass

    args = type("Args", (), {
        "confirmations": orderfilled.CONFIRMATIONS,
        "adopt_legacy_cursor": True,
        "from_block": None,
        "to_block": None,
    })()
    monkeypatch.setattr(orderfilled, "connect_postgres", lambda: Conn())
    monkeypatch.setattr(orderfilled, "ensure_schema", lambda _conn: None)
    monkeypatch.setattr(orderfilled, "adopt_legacy_cursor", lambda _conn: 456)
    monkeypatch.setattr(
        orderfilled,
        "RpcClient",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("RPC must not run")),
    )
    assert orderfilled.run(args) == 0
    assert '"last_block": 456' in capsys.readouterr().out


def test_legacy_compat_cursor_advances_only_without_open_gaps(monkeypatch):
    states = {orderfilled.FACT_CURSOR: {"last_block": 789}}
    writes = []

    class Result:
        def __init__(self, has_open):
            self.has_open = has_open

        def fetchone(self):
            return {"has_open": self.has_open}

    class Conn:
        has_open = False
        commits = 0

        def execute(self, _query):
            return Result(self.has_open)

        def commit(self):
            self.commits += 1

    conn = Conn()
    monkeypatch.setattr(orderfilled, "get_state", lambda _conn, key: states.get(key))
    monkeypatch.setattr(
        orderfilled,
        "set_state",
        lambda _conn, key, **values: writes.append((key, values)),
    )
    assert orderfilled.advance_legacy_trade_cursor_if_complete(conn) == 789
    assert writes[0][0] == "trade_sync"
    assert writes[0][1]["last_block"] == 789
    assert conn.commits == 1

    conn.has_open = True
    assert orderfilled.advance_legacy_trade_cursor_if_complete(conn) is None
    assert len(writes) == 1


@pytest.mark.parametrize("replay_outcome", ["success", "write_failure", "owner_missing"])
def test_gap_closes_only_after_full_history_replay(monkeypatch, replay_outcome):
    historical = orderfilled.decode_log(
        log(
            address=orderfilled.LEGACY_EXCHANGES[0],
            topic0=orderfilled.LEGACY_TOPIC,
            words=[0, 77, 2_000_000, 4_000_000, 1],
            block=100,
        )
    )
    fresh = {**historical, "block_number": 200, "tx_hash": "0x" + "33" * 32}
    owner = {
        "market_id": 7,
        "condition_id": "0xcondition",
        "outcome_index": 0,
        "identity_kind": "canonical_gamma",
    }
    owners = {}
    monkeypatch.setattr(orderfilled, "load_token_owners", lambda *_args: owners)

    class Result:
        def __init__(self, rows):
            self.rows = rows

        def fetchall(self):
            return self.rows

    class Conn:
        status = None
        commits = 0

        def execute(self, query, params):
            if "INSERT INTO ops.orderfilled_registry_gaps" in query:
                self.status = "open"
            elif "UPDATE ops.orderfilled_registry_gaps" in query:
                self.status = "resolved"
            elif "SELECT gap.token_id" in query:
                return Result([{"token_id": "77"}] if self.status == "open" else [])
            elif "FROM core.orderfilled_raw" in query:
                assert params == (["77"],)
                return Result([historical, fresh])
            else:
                raise AssertionError(query)

        def commit(self):
            self.commits += 1

    class Writer:
        fail = False

        def __init__(self):
            self.keys = set()

        def write_facts(self, rows):
            if self.fail:
                raise RuntimeError("ClickHouse write failed")
            keys = {orderfilled.event_key(row) for row in rows}
            inserted = len(keys - self.keys)
            self.keys.update(keys)
            return inserted

    conn, writer = Conn(), Writer()
    orderfilled.project_rows(conn, writer, [historical])
    assert conn.status == "open"
    assert writer.keys == set()

    owners["77"] = owner
    orderfilled.project_rows(conn, writer, [fresh])
    assert conn.status == "open"
    assert writer.keys == {orderfilled.event_key(fresh)}
    assert conn.commits == 2

    writer.fail = replay_outcome == "write_failure"
    if replay_outcome == "write_failure":
        with pytest.raises(RuntimeError, match="ClickHouse write failed"):
            orderfilled.retry_resolved_gaps(conn, writer, limit_tokens=1)
        assert conn.status == "open"
        assert conn.commits == 2
    elif replay_outcome == "owner_missing":
        owners.clear()
        result = orderfilled.retry_resolved_gaps(conn, writer, limit_tokens=1)
        assert result["unresolved"] == 2
        assert conn.status == "open"
        assert writer.keys == {orderfilled.event_key(fresh)}
    else:
        result = orderfilled.retry_resolved_gaps(conn, writer, limit_tokens=1)
        assert result == {"tokens": 1, "raw": 2, "facts": 2, "inserted": 1, "unresolved": 0}
        assert conn.status == "resolved"
        assert conn.commits == 3
        assert writer.keys == {orderfilled.event_key(historical), orderfilled.event_key(fresh)}


def test_non_combo_gap_uses_bounded_official_gamma_lookup(monkeypatch):
    class Rows:
        def fetchall(self):
            return [{"token_id": "77"}, {"token_id": "88"}]

    class Conn:
        commits = 0
        rollbacks = 0

        def execute(self, query, params):
            if query.lstrip().startswith("SELECT"):
                assert params == (orderfilled.V2026_EXCHANGES[-1], 2)
                return Rows()
            assert "gamma_lookup_pending" in query
            assert params == (["77", "88"],)

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    calls = []
    monkeypatch.setattr(
        market,
        "sync_gamma_markets_for_token_ids",
        lambda tokens, *, conn: calls.append((tokens, conn)) or 1,
    )
    conn = Conn()

    result = orderfilled.resolve_open_gamma_gaps(conn, limit_tokens=2)

    assert result == {"attempted_tokens": 2, "persisted_markets": 1, "failed": 0}
    assert calls == [(["77", "88"], conn)]
    assert conn.commits == 1 and conn.rollbacks == 0
