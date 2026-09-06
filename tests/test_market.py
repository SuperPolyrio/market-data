from __future__ import annotations

import argparse
from datetime import datetime, timezone

import pytest

from market_data import market


def gamma_market(**changes):
    value = {
        "id": "123",
        "conditionId": "0xABC",
        "questionID": "0xquestion",
        "slug": "will-it-rain",
        "question": "Will it rain?",
        "description": "Forecast",
        "clobTokenIds": '["token-no", "token-yes"]',
        "outcomes": '["No", "Yes"]',
        "outcomePrices": '["0.4", "0.6"]',
        "tags": [{"slug": "weather"}, {"label": "Forecasts"}],
        "negRisk": False,
        "active": True,
        "closed": False,
        "acceptingOrders": True,
        "enableOrderBook": True,
        "createdAt": "2026-09-06T01:00:00Z",
        "updatedAt": "2026-09-06T02:00:00Z",
        "endDate": "2026-09-07T00:00:00Z",
    }
    value.update(changes)
    return value


def test_normalize_market_preserves_source_alignment_and_maps_yes_no():
    row = market.normalize_market(gamma_market())

    assert row["gamma_market_id"] == "123"
    assert row["condition_id"] == "0xabc"
    assert row["clob_token_ids"] == ["token-no", "token-yes"]
    assert row["outcomes"] == ["No", "Yes"]
    assert row["outcome_prices"] == ["0.4", "0.6"]
    assert row["yes_token_id"] == "token-yes"
    assert row["no_token_id"] == "token-no"
    assert row["yes_source_index"] == 1
    assert row["no_source_index"] == 0
    assert row["yes_price"] == "0.6"
    assert row["no_price"] == "0.4"
    assert row["category"] == "weather"
    assert row["tags"] == ["weather", "forecasts"]
    assert row["active"] is True
    assert row["closed"] is False
    assert row["accepting_orders"] is True
    assert row["enable_order_book"] is True


def test_normalize_event_market_uses_source_slots_and_event_metadata():
    raw = gamma_market(
        clobTokenIds=["team-a-token", "team-b-token"],
        outcomes=["Team A", "Team B"],
        outcomePrices=[0.25, 0.75],
        tags=None,
    )
    event = {
        "id": 99,
        "slug": "championship",
        "title": "Championship",
        "tags": [{"slug": "sports"}, {"slug": "basketball"}],
        "enableNegRisk": True,
    }

    row = market.normalize_market(raw, event)

    assert row["yes_token_id"] == "team-a-token"
    assert row["no_token_id"] == "team-b-token"
    assert row["yes_price"] == "0.25"
    assert row["event_id"] == "99"
    assert row["event_slug"] == "championship"
    assert row["category"] == "sports"
    assert row["enable_neg_risk"] is True


def test_normalize_uses_official_tag_or_other_for_missing_category():
    tagged = market.normalize_market(gamma_market(tags=[{"slug": "niche-topic"}]))
    untagged = market.normalize_market(gamma_market(tags=[]))

    assert tagged["category"] == "niche-topic"
    assert untagged["category"] == "other"


def test_normalize_accepts_gamma_variable_fractional_timestamp():
    row = market.normalize_market(gamma_market(createdAt="2026-09-06T02:21:21.53177Z"))

    assert row["created_at"] == datetime(
        2026, 9, 6, 2, 21, 21, 531770, tzinfo=timezone.utc
    )


def test_gamma_session_honours_deployment_proxy_environment():
    session = market._http_session()

    assert session.trust_env is True


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"id": ""}, "gamma market id"),
        ({"conditionId": ""}, "condition id"),
        ({"clobTokenIds": "[]"}, "two distinct"),
        ({"outcomes": '["Yes", "Yes"]'}, "two distinct"),
        ({"outcomePrices": '["1.2", "-0.2"]'}, "outside"),
    ],
)
def test_normalize_market_rejects_noncanonical_rows(changes, message):
    with pytest.raises(ValueError, match=message):
        market.normalize_market(gamma_market(**changes))


def test_sync_gamma_markets_for_token_ids_uses_official_gamma_and_target_upsert(
    monkeypatch,
):
    class LookupSession:
        def __init__(self):
            self.calls = []

        def get(self, url, *, params, timeout):
            self.calls.append((url, params, timeout))
            return Response([gamma_market()])

    class LookupConn:
        def __init__(self):
            self.calls = []

        def commit(self):
            self.calls.append("commit")

        def rollback(self):
            self.calls.append("rollback")

        def close(self):
            self.calls.append("close")

    session = LookupSession()
    conn = LookupConn()
    persisted = []
    monkeypatch.setattr(market, "connect_postgres", lambda: conn)
    monkeypatch.setattr(
        market,
        "upsert_markets",
        lambda _conn, rows: persisted.extend(rows) or len(persisted),
    )

    written = market.sync_gamma_markets_for_token_ids(
        ["token-yes", "token-yes"], session=session
    )

    assert written == 1
    assert persisted[0]["condition_id"] == "0xabc"
    assert session.calls == [
        (
            f"{market.GAMMA_API_BASE}/markets",
            {"clob_token_ids": ["token-yes"], "limit": 100},
            60,
        )
    ]
    assert conn.calls == ["commit", "close"]


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class Session:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get(self, url, *, params, timeout):
        source = "events" if "/events/" in url else "markets"
        cursor = params.get("after_cursor")
        self.calls.append((source, cursor, params["limit"], timeout))
        return Response(self.pages[(source, cursor)])


def test_backfill_dry_run_reads_both_keysets_without_storage():
    event_market = gamma_market(id="456", conditionId="def", tags=None)
    session = Session(
        {
            ("markets", None): {"markets": [gamma_market()], "next_cursor": None},
            ("events", None): {
                "events": [
                    {
                        "id": "event-1",
                        "slug": "event-one",
                        "title": "Event One",
                        "tags": [{"slug": "weather"}],
                        "createdAt": "2026-09-06T01:00:00Z",
                        "markets": [event_market],
                    }
                ],
                "next_cursor": None,
            },
        }
    )

    result = market.run_once(
        mode="backfill",
        max_pages=1,
        dry_run=True,
        session=session,
        since=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert result["canonical"] == 2
    assert result["persisted"] == 0
    assert result["rejected"] == 0
    assert len(result["sample"]) == 2
    assert session.calls == [
        ("markets", None, 100, 60),
        ("events", None, 100, 60),
    ]


def test_scan_returns_auditable_normalization_failure():
    session = Session(
        {
            ("markets", None): {
                "markets": [gamma_market(outcomes='["Yes", "Yes"]')],
                "next_cursor": None,
            }
        }
    )

    _, summary, _, failures = market._scan(
        session,
        "markets",
        mode="sync",
        max_pages=1,
        state={},
        since=datetime(2026, 9, 1, tzinfo=timezone.utc),
        overlap=market.timedelta(0),
        lane="markets",
    )

    assert summary["rejected"] == 1
    assert len(failures) == 1
    failure = failures[0]
    assert failure["mode"] == "sync"
    assert failure["lane"] == "markets"
    assert failure["kind"] == "normalize_error"
    assert failure["source_identity"] == "123"
    assert failure["condition_id"] == "0xabc"
    assert "two distinct" in failure["reason"]
    assert '"token_ids": ["token-no", "token-yes"]' in failure["detail_json"]
    assert '"conditionId": "0xABC"' in failure["raw_json"]


def test_dry_run_never_writes_failure_ledger(monkeypatch):
    session = Session(
        {
            ("markets", None): {
                "markets": [gamma_market(id="")],
                "next_cursor": None,
            },
            ("events", None): {"events": [], "next_cursor": None},
        }
    )
    monkeypatch.setattr(
        market,
        "record_failures",
        lambda *_args, **_kwargs: pytest.fail("dry-run wrote the failure ledger"),
    )

    result = market.run_once(
        mode="backfill",
        max_pages=1,
        dry_run=True,
        session=session,
        since=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert result["rejected"] == 1
    assert result["failures_ledgered"] == 0


def test_created_lane_resumes_cursor_without_advancing_watermark_early():
    first = gamma_market(createdAt="2026-09-06T02:00:00Z")
    second = gamma_market(
        id="124", conditionId="0xDEF", createdAt="2026-09-06T01:30:00Z"
    )
    session = Session(
        {
            ("markets", None): {"markets": [first], "next_cursor": "next-1"},
            ("markets", "next-1"): {"markets": [second], "next_cursor": "next-2"},
        }
    )
    since = datetime(2026, 9, 5, tzinfo=timezone.utc)

    _, summary, state, _ = market._scan(
        session,
        "markets",
        mode="sync",
        max_pages=1,
        state={},
        since=since,
        overlap=market.timedelta(minutes=10),
    )
    assert summary["complete"] is False
    assert state["watermark"].startswith("2026-09-05")
    assert state["scan_high"].startswith("2026-09-06T02:00:00")
    assert state["cursor"] == "next-1"

    _, summary, resumed, _ = market._scan(
        session,
        "markets",
        mode="sync",
        max_pages=1,
        state=state,
        since=since,
        overlap=market.timedelta(minutes=10),
    )
    assert summary["complete"] is False
    assert resumed["cursor"] == "next-2"
    assert session.calls[-1][1] == "next-1"


class RevisitSession:
    def __init__(self):
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append(dict(params))
        cursor = params.get("after_cursor")
        if cursor is None:
            payload = {
                "events": [
                    {
                        "id": "old-event",
                        "createdAt": "2024-01-01T00:00:00Z",
                        "updatedAt": "2026-09-06T04:00:00Z",
                        "endDate": "2026-09-10T00:00:00Z",
                        "tags": [{"slug": "weather"}],
                        "markets": [
                            gamma_market(
                                id="late-market",
                                conditionId="0xlate",
                                createdAt="2026-09-06T03:00:00Z",
                            )
                        ],
                    }
                ],
                "next_cursor": "revisit-next",
            }
        else:
            payload = {"events": [], "next_cursor": None}
        return Response(payload)


def test_revisit_uses_end_date_lane_and_resumes_its_cursor():
    session = RevisitSession()
    since = datetime(2026, 9, 6, tzinfo=timezone.utc)
    query = {"order": "updatedAt", "ascending": "false", "closed": "false"}

    rows, summary, state, _ = market._scan(
        session,
        "events",
        mode="revisit",
        max_pages=1,
        state={},
        since=since,
        overlap=market.timedelta(0),
        query=query,
        lane="open",
    )

    assert [row["gamma_market_id"] for row in rows] == ["late-market"]
    assert summary["source"] == "open"
    assert state["cursor"] == "revisit-next"
    assert session.calls[0] == {
        "limit": 100,
        "order": "updatedAt",
        "ascending": "false",
        "include_tag": "true",
        "closed": "false",
    }

    _, summary, state, _ = market._scan(
        session,
        "events",
        mode="revisit",
        max_pages=1,
        state=state,
        since=since,
        overlap=market.timedelta(0),
        query=query,
        lane="open",
    )

    assert session.calls[1]["after_cursor"] == "revisit-next"
    assert summary["complete"] is True
    assert state["watermark"].startswith("2026-09-06T04:00:00")


def test_revisit_refreshes_trading_state_for_old_market():
    old_market = gamma_market(
        id="old-market",
        conditionId="0xold",
        createdAt="2024-01-01T00:00:00Z",
        active=False,
        closed=True,
        acceptingOrders=False,
        enableOrderBook=True,
    )
    session = Session(
        {
            ("events", None): {
                "events": [
                    {
                        "id": "event",
                        "createdAt": "2024-01-01T00:00:00Z",
                        "updatedAt": "2026-09-06T04:00:00Z",
                        "markets": [old_market],
                    }
                ],
                "next_cursor": None,
            }
        }
    )

    rows, _, _, _ = market._scan(
        session,
        "events",
        mode="revisit",
        max_pages=1,
        state={},
        since=datetime(2026, 9, 6, tzinfo=timezone.utc),
        overlap=market.timedelta(0),
        query={"closed": "true"},
    )

    assert len(rows) == 1
    assert rows[0]["active"] is False
    assert rows[0]["closed"] is True
    assert rows[0]["accepting_orders"] is False


class EmptyRevisitSession:
    def __init__(self):
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append(dict(params))
        return Response({"events": [], "next_cursor": None})


def test_revisit_runs_independent_open_and_closed_lanes():
    session = EmptyRevisitSession()
    result = market.run_once(
        mode="revisit",
        max_pages=1,
        dry_run=True,
        session=session,
        since=datetime(2026, 9, 6, tzinfo=timezone.utc),
        overlap_minutes=0,
    )

    assert [call["closed"] for call in session.calls] == ["false", "true"]
    assert all(call["order"] == "updatedAt" for call in session.calls)
    assert [lane["source"] for lane in result["lanes"]] == ["open", "closed"]
    assert market.REVISIT_STATE.format(source="open") != market.REVISIT_STATE.format(
        source="closed"
    )


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class TransactionConn:
    def __init__(self, calls):
        self.calls = calls

    def commit(self):
        self.calls.append("commit")

    def rollback(self):
        self.calls.append("rollback")


def test_completed_backfill_lane_is_not_restarted_from_the_head(monkeypatch):
    calls = []
    session = Session(
        {
            ("events", None): {"events": [], "next_cursor": None},
        }
    )

    def state(_conn, key):
        if key == market.BACKFILL_STATE.format(source="markets"):
            return {
                "complete": True,
                "cursor": None,
                "updated_at": "2026-09-06T00:00:00+00:00",
            }
        return {}

    monkeypatch.setattr(market, "_state", state)
    monkeypatch.setattr(market, "ensure_schema", lambda _conn: None)
    monkeypatch.setattr(
        market,
        "filter_token_ownership_conflicts",
        lambda _conn, rows: (list(rows), [], []),
    )
    monkeypatch.setattr(market, "upsert_markets", lambda *_args: 0)
    monkeypatch.setattr(market, "record_failures", lambda *_args: 0)
    monkeypatch.setattr(market, "record_promotions", lambda *_args: 0)
    monkeypatch.setattr(
        market,
        "set_state",
        lambda _conn, key, **_kwargs: calls.append(f"state:{key}"),
    )

    result = market.run_once(
        mode="backfill",
        max_pages=1,
        session=session,
        conn=TransactionConn(calls),
        since=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert session.calls == [("events", None, 100, 60)]
    assert result["lanes"][0] == {
        "source": "markets",
        "pages": 0,
        "raw_markets": 0,
        "canonical": 0,
        "rejected": 0,
        "complete": True,
        "next_cursor": None,
    }
    assert calls[-1] == "commit"


def test_revisit_resume_does_not_rescan_completed_sibling_lane(monkeypatch):
    calls = []
    session = Session(
        {
            ("events", "open-next"): {"events": [], "next_cursor": None},
        }
    )

    def state(_conn, key):
        if key == market.REVISIT_STATE.format(source="open"):
            return {
                "complete": False,
                "cursor": "open-next",
                "watermark": "2026-09-06T00:00:00+00:00",
                "base_watermark": "2026-09-06T00:00:00+00:00",
                "scan_high": "2026-09-06T01:00:00+00:00",
            }
        return {"complete": True, "watermark": "2026-09-06T01:00:00+00:00"}

    monkeypatch.setattr(market, "_state", state)
    monkeypatch.setattr(market, "ensure_schema", lambda _conn: None)
    monkeypatch.setattr(
        market,
        "filter_token_ownership_conflicts",
        lambda _conn, rows: (list(rows), [], []),
    )
    monkeypatch.setattr(market, "upsert_markets", lambda *_args: 0)
    monkeypatch.setattr(market, "record_failures", lambda *_args: 0)
    monkeypatch.setattr(market, "record_promotions", lambda *_args: 0)
    monkeypatch.setattr(market, "set_state", lambda *_args, **_kwargs: None)

    result = market.run_once(
        mode="revisit",
        max_pages=1,
        session=session,
        conn=TransactionConn(calls),
        since=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert session.calls == [("events", "open-next", 100, 60)]
    assert result["lanes"][0]["complete"] is True
    assert result["lanes"][1]["pages"] == 0
    assert result["lanes"][1]["complete"] is True


def test_normalization_failure_is_ledgered_before_cursor_commit(monkeypatch):
    calls = []
    captured = []
    session = Session(
        {
            ("markets", None): {
                "markets": [gamma_market(id="")],
                "next_cursor": None,
            },
            ("events", None): {"events": [], "next_cursor": None},
        }
    )
    monkeypatch.setattr(market, "_state", lambda *_args: {})
    monkeypatch.setattr(market, "ensure_schema", lambda _conn: None)
    monkeypatch.setattr(
        market,
        "filter_token_ownership_conflicts",
        lambda _conn, rows: (list(rows), [], []),
    )
    monkeypatch.setattr(market, "upsert_markets", lambda *_args: 0)

    def ledger(_conn, failures):
        captured.extend(failures)
        calls.append("ledger")
        return len(captured)

    monkeypatch.setattr(market, "record_failures", ledger)
    monkeypatch.setattr(
        market,
        "set_state",
        lambda _conn, key, **_kwargs: calls.append(f"state:{key}"),
    )

    result = market.run_once(
        mode="sync",
        max_pages=1,
        session=session,
        conn=TransactionConn(calls),
        since=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert result["failures_ledgered"] == 1
    assert captured[0]["kind"] == "normalize_error"
    assert calls.index("ledger") < calls.index("state:market.gamma.created.markets.v1")
    assert calls[-1] == "commit"


def test_historical_ownership_conflict_is_ledgered_with_identity(monkeypatch):
    calls = []
    captured = []
    session = Session(
        {
            ("markets", None): {"markets": [gamma_market()], "next_cursor": None},
            ("events", None): {"events": [], "next_cursor": None},
        }
    )
    monkeypatch.setattr(market, "_state", lambda *_args: {})
    monkeypatch.setattr(market, "ensure_schema", lambda _conn: None)
    monkeypatch.setattr(
        market,
        "filter_token_ownership_conflicts",
        lambda _conn, _rows: (
            [],
            [
                {
                    "token_id": "token-yes",
                    "existing": "0xowner",
                    "attempted": "0xabc",
                }
            ],
            [],
        ),
    )
    monkeypatch.setattr(market, "upsert_markets", lambda *_args: 0)

    def ledger(_conn, failures):
        captured.extend(failures)
        calls.append("ledger")
        return len(captured)

    monkeypatch.setattr(market, "record_failures", ledger)
    monkeypatch.setattr(
        market,
        "set_state",
        lambda _conn, key, **_kwargs: calls.append(f"state:{key}"),
    )

    result = market.run_once(
        mode="backfill",
        max_pages=1,
        session=session,
        conn=TransactionConn(calls),
        since=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert result["ownership_conflicts"] == 1
    assert result["failures_ledgered"] == 1
    failure = captured[0]
    assert failure["mode"] == "backfill"
    assert failure["lane"] == "markets"
    assert failure["kind"] == "token_ownership_conflict"
    assert failure["source_identity"] == "123"
    assert failure["condition_id"] == "0xabc"
    assert failure["token_id"] == "token-yes"
    assert '"existing": "0xowner"' in failure["detail_json"]
    assert calls.index("ledger") < calls.index("state:market.gamma.backfill.markets.v1")
    assert calls[-1] == "commit"


def test_failure_identity_is_stable_and_upsert_counts_reobservations(monkeypatch):
    failure = market._failure(
        mode="revisit",
        lane="closed",
        kind="normalize_error",
        reason="bad identity",
        source_identity="gamma-1",
        condition_id="0xcondition",
        raw={"id": "gamma-1"},
    )
    repeated = market._failure(
        mode="backfill",
        lane="markets",
        kind="normalize_error",
        reason="upstream wording changed",
        source_identity="gamma-1",
        condition_id="0xcondition",
        raw={"id": "gamma-1", "updatedAt": "later"},
    )
    captured = {}

    def execute(_conn, sql, rows):
        captured["sql"] = sql
        captured["rows"] = list(rows)
        return 1

    monkeypatch.setattr(market, "execute_values", execute)

    assert failure["failure_key"] == repeated["failure_key"]
    assert market.record_failures(object(), [failure]) == 1
    assert "ON CONFLICT (failure_key)" in captured["sql"]
    assert "WITH prior AS" in captured["sql"]
    assert "source_identity=%(source_identity)s" in captured["sql"]
    assert "occurrences + 1" in captured["sql"]
    assert "first_seen_at" in captured["sql"]
    assert "last_seen_at" in captured["sql"]
    assert (
        "left(ops.market_acquisition_failures.status, 9) = 'terminal_'"
        in captured["sql"]
    )


def test_live_ownership_conflict_stays_fail_closed(monkeypatch):
    calls = []
    session = Session(
        {
            ("markets", None): {"markets": [gamma_market()], "next_cursor": None},
            ("events", None): {"events": [], "next_cursor": None},
        }
    )
    monkeypatch.setattr(market, "_state", lambda *_args: {})
    monkeypatch.setattr(market, "ensure_schema", lambda _conn: None)
    monkeypatch.setattr(
        market,
        "filter_token_ownership_conflicts",
        lambda _conn, _rows: (
            [],
            [
                {
                    "kind": "token_ownership_conflict",
                    "token_id": "token-yes",
                    "existing": "0xother",
                    "attempted": "0xabc",
                }
            ],
            [],
        ),
    )
    monkeypatch.setattr(
        market,
        "record_failures",
        lambda _conn, rows: calls.append(f"ledger:{len(list(rows))}") or 1,
    )
    monkeypatch.setattr(
        market,
        "upsert_markets",
        lambda *_args: pytest.fail("blocked live conflict reached market upsert"),
    )
    monkeypatch.setattr(
        market,
        "set_state",
        lambda *_args, **_kwargs: pytest.fail(
            "failed live transaction advanced cursor"
        ),
    )

    with pytest.raises(market.TokenOwnershipConflict, match="blocked"):
        market.run_once(
            mode="sync",
            max_pages=1,
            session=session,
            conn=TransactionConn(calls),
            since=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )

    assert calls == ["ledger:1", "commit", "rollback"]


def test_live_safe_promotion_is_ledgered_before_cursor_commit(monkeypatch):
    calls = []
    captured = []
    session = Session(
        {
            ("markets", None): {"markets": [gamma_market()], "next_cursor": None},
            ("events", None): {"events": [], "next_cursor": None},
        }
    )
    action = {
        "kind": "market_identity",
        "gamma_market_id": "123",
        "token_id": "",
        "from_market_id": 7,
        "from_condition_id": "0xabc",
        "from_identity_kind": "protocol_structural",
        "to_condition_id": "0xabc",
        "reason": "strict Gamma identity supersedes a local shell",
    }
    monkeypatch.setattr(market, "_state", lambda *_args: {})
    monkeypatch.setattr(market, "ensure_schema", lambda _conn: None)
    monkeypatch.setattr(
        market,
        "filter_token_ownership_conflicts",
        lambda _conn, rows: (list(rows), [], [action]),
    )
    monkeypatch.setattr(
        market,
        "upsert_markets",
        lambda _conn, rows, promotions: (
            calls.append(("upsert", len(list(rows)), list(promotions))) or 1
        ),
    )

    def promotion_ledger(_conn, rows):
        captured.extend(rows)
        calls.append("promotion-ledger")
        return len(captured)

    monkeypatch.setattr(market, "record_promotions", promotion_ledger)
    monkeypatch.setattr(market, "record_failures", lambda *_args: 0)
    monkeypatch.setattr(
        market,
        "set_state",
        lambda _conn, key, **_kwargs: calls.append(f"state:{key}"),
    )

    result = market.run_once(
        mode="sync",
        max_pages=1,
        session=session,
        conn=TransactionConn(calls),
        since=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert result["canonical_promotions"] == 1
    assert captured[0]["kind"] == "market_identity"
    assert captured[0]["mode"] == "sync"
    assert captured[0]["lane"] == "markets"
    assert calls.index("promotion-ledger") < calls.index(
        "state:market.gamma.created.markets.v1"
    )
    assert calls[-1] == "commit"


class OwnershipConn:
    def execute(self, sql, params=None):
        if "id AS market_id" in sql:
            return Rows(
                [
                    {
                        "market_id": 7,
                        "condition_id": "0xabc",
                        "identity_kind": "canonical_gamma",
                        "gamma_market_id": "123",
                        "category": "weather",
                        "slug": "will-it-rain",
                    }
                ]
            )
        assert "FROM core.market_tokens" in sql
        return Rows(
            [
                {
                    "token_id": "token-yes",
                    "market_id": 8,
                    "token_condition_id": "0xother",
                    "market_condition_id": "0xother",
                    "identity_kind": "canonical_gamma",
                    "gamma_market_id": "other",
                    "category": "weather",
                    "slug": "other",
                }
            ]
        )


def test_upsert_refuses_to_rebind_token_owner():
    row = market.normalize_market(gamma_market())
    with pytest.raises(market.TokenOwnershipConflict, match="refusing to rebind"):
        market.upsert_markets(OwnershipConn(), [row])


def test_historical_filter_skips_only_conflicting_market():
    valid = market.normalize_market(gamma_market())
    conflicting = market.normalize_market(
        gamma_market(
            id="456",
            conditionId="0xdef",
            clobTokenIds='["token-other-no", "token-yes"]',
        )
    )

    class ExistingCanonicalOwner:
        def execute(self, sql, params=None):
            if "id AS market_id" in sql:
                return Rows(
                    [
                        {
                            "market_id": 7,
                            "condition_id": "0xabc",
                            "identity_kind": "canonical_gamma",
                            "gamma_market_id": "123",
                            "category": "weather",
                            "slug": "will-it-rain",
                        }
                    ]
                )
            assert "FROM core.market_tokens" in sql
            return Rows(
                [
                    {
                        "token_id": "token-yes",
                        "market_id": 7,
                        "token_condition_id": "0xabc",
                        "market_condition_id": "0xabc",
                        "identity_kind": "canonical_gamma",
                        "gamma_market_id": "123",
                        "category": "weather",
                        "slug": "will-it-rain",
                    }
                ]
            )

    accepted, conflicts, promotions = market.filter_token_ownership_conflicts(
        ExistingCanonicalOwner(), [valid, conflicting]
    )

    assert accepted == [valid]
    assert conflicts[0]["token_id"] == "token-yes"
    assert conflicts[0]["existing"] == "0xabc"
    assert conflicts[0]["attempted"] == "0xdef"
    assert promotions == []


class RegistryOwnerConn:
    def __init__(self, *, markets=(), tokens=()):
        self.markets = list(markets)
        self.tokens = list(tokens)
        self.promote_calls = []

    def execute(self, sql, params=None):
        if "id AS market_id" in sql:
            return Rows(self.markets)
        if "AS token_condition_id" in sql:
            return Rows(self.tokens)
        if sql == market.TOKEN_PROMOTE:
            self.promote_calls.append(dict(params))
            return Rows([{"token_id": params["token_id"]}])
        if "SELECT id, condition_id FROM core.markets" in sql:
            return Rows([{"id": 7, "condition_id": "0xabc"}])
        if "SELECT token_id, market_id, condition_id, outcome" in sql:
            return Rows(
                [
                    {
                        "token_id": "token-yes",
                        "market_id": 7,
                        "condition_id": "0xabc",
                        "outcome": "YES",
                        "outcome_index": 0,
                    },
                    {
                        "token_id": "token-no",
                        "market_id": 7,
                        "condition_id": "0xabc",
                        "outcome": "NO",
                        "outcome_index": 1,
                    },
                ]
            )
        raise AssertionError(sql)


def placeholder_owner(token_id, market_id, condition_id):
    return {
        "token_id": token_id,
        "market_id": market_id,
        "token_condition_id": condition_id,
        "market_condition_id": condition_id,
        "identity_kind": "canonical_gamma",
        "gamma_market_id": "",
        "category": "orderfilled-placeholder",
        "slug": f"trade-indexer-placeholder-{market_id}",
    }


def test_strict_gamma_promotes_two_tokens_from_separate_placeholders():
    row = market.normalize_market(gamma_market())
    conn = RegistryOwnerConn(
        tokens=[
            placeholder_owner("token-yes", 101, "0xplaceholder-yes"),
            placeholder_owner("token-no", 102, "0xplaceholder-no"),
        ]
    )

    accepted, conflicts, promotions = market.filter_token_ownership_conflicts(
        conn, [row]
    )

    assert accepted == [row]
    assert conflicts == []
    assert {
        (item["kind"], item["token_id"], item["from_market_id"]) for item in promotions
    } == {
        ("token_rehome", "token-yes", 101),
        ("token_rehome", "token-no", 102),
    }


def test_protocol_structural_same_condition_is_promoted_atomically(monkeypatch):
    row = market.normalize_market(gamma_market())
    conn = RegistryOwnerConn(
        markets=[
            {
                "market_id": 7,
                "condition_id": "0xabc",
                "identity_kind": "protocol_structural",
                "gamma_market_id": "",
                "category": "protocol-structural",
                "slug": "protocol-structural-abc",
            }
        ],
        tokens=[
            placeholder_owner("token-yes", 101, "0xplaceholder-yes"),
            placeholder_owner("token-no", 102, "0xplaceholder-no"),
        ],
    )
    accepted, conflicts, promotions = market.filter_token_ownership_conflicts(
        conn, [row]
    )
    writes = []

    def capture(_conn, sql, rows):
        materialized = list(rows)
        writes.append((sql, materialized))
        return len(materialized)

    monkeypatch.setattr(market, "execute_values", capture)

    assert conflicts == []
    assert {item["kind"] for item in promotions} == {
        "market_identity",
        "token_rehome",
    }
    assert market.upsert_markets(conn, accepted, promotions) == 1
    assert {
        (
            call["token_id"],
            call["from_market_id"],
            call["to_market_id"],
            call["to_condition_id"],
            call["outcome"],
        )
        for call in conn.promote_calls
    } == {
        ("token-yes", 101, 7, "0xabc", "YES"),
        ("token-no", 102, 7, "0xabc", "NO"),
    }
    assert all("DELETE" not in sql.upper() for sql, _rows in writes)


@pytest.mark.parametrize("identity_kind", ["canonical_gamma", "official_combo", ""])
def test_nonreplaceable_token_owner_remains_a_conflict(identity_kind):
    row = market.normalize_market(gamma_market())
    owner = placeholder_owner("token-yes", 99, "0xother")
    owner.update(
        identity_kind=identity_kind,
        gamma_market_id="other-gamma" if identity_kind == "canonical_gamma" else "",
        category="other",
        slug="other-owner",
    )

    accepted, conflicts, promotions = market.filter_token_ownership_conflicts(
        RegistryOwnerConn(tokens=[owner]), [row]
    )

    assert accepted == []
    assert conflicts[0]["existing_identity_kind"] == (identity_kind or "unknown")
    assert promotions == []


def test_promotion_evidence_key_is_idempotent_across_reobservation(monkeypatch):
    action = {
        "kind": "token_rehome",
        "gamma_market_id": "123",
        "token_id": "token-yes",
        "from_market_id": 101,
        "from_condition_id": "0xplaceholder",
        "from_identity_kind": "legacy_placeholder",
        "to_condition_id": "0xabc",
        "reason": "strict Gamma token supersedes a local shell owner",
    }
    first = market._promotion(action, mode="backfill", lane="markets")
    repeated = market._promotion(action, mode="sync", lane="events")
    captured = {}

    def execute(_conn, sql, rows):
        captured["sql"] = sql
        captured["rows"] = list(rows)
        return 1

    monkeypatch.setattr(market, "execute_values", execute)

    assert first["promotion_key"] == repeated["promotion_key"]
    assert market.record_promotions(object(), [first]) == 1
    assert "ON CONFLICT (promotion_key)" in captured["sql"]
    assert "occurrences + 1" in captured["sql"]


def test_upsert_refuses_batch_token_with_two_condition_owners():
    first = market.normalize_market(gamma_market())
    second = market.normalize_market(
        gamma_market(
            id="456",
            conditionId="0xdef",
            clobTokenIds='["other-token", "token-yes"]',
            outcomes='["No", "Yes"]',
        )
    )
    with pytest.raises(market.TokenOwnershipConflict, match="multiple conditions"):
        market.upsert_markets(None, [first, second])


class ExactConn:
    def execute(self, sql, params=None):
        if "id AS market_id" in sql:
            return Rows(
                [
                    {
                        "market_id": 7,
                        "condition_id": "0xabc",
                        "identity_kind": "canonical_gamma",
                        "gamma_market_id": "123",
                        "category": "weather",
                        "slug": "will-it-rain",
                    }
                ]
            )
        if "AS token_condition_id" in sql:
            return Rows(
                [
                    {
                        "token_id": token,
                        "market_id": 7,
                        "token_condition_id": "0xabc",
                        "market_condition_id": "0xabc",
                        "identity_kind": "canonical_gamma",
                        "gamma_market_id": "123",
                        "category": "weather",
                        "slug": "will-it-rain",
                    }
                    for token in ("token-yes", "token-no")
                ]
            )
        if "SELECT id, condition_id FROM core.markets" in sql:
            return Rows([{"id": 7, "condition_id": "0xabc"}])
        if "SELECT token_id, market_id" in sql:
            return Rows(
                [
                    {
                        "token_id": "token-yes",
                        "market_id": 7,
                        "condition_id": "0xabc",
                        "outcome": "YES",
                        "outcome_index": 0,
                    },
                    {
                        "token_id": "token-no",
                        "market_id": 7,
                        "condition_id": "0xabc",
                        "outcome": "NO",
                        "outcome_index": 1,
                    },
                ]
            )
        raise AssertionError(sql)


def test_upsert_uses_compatible_deterministic_negative_token_ids(monkeypatch):
    calls = []

    def capture(conn, sql, rows):
        materialized = list(rows)
        calls.append((sql, materialized))
        return len(materialized)

    monkeypatch.setattr(market, "execute_values", capture)
    row = market.normalize_market(gamma_market())

    assert market.upsert_markets(ExactConn(), [row]) == 1

    token_rows = calls[-1][1]
    assert [
        (item["id"], item["outcome"], item["outcome_index"]) for item in token_rows
    ] == [
        (-13, "YES", 0),
        (-14, "NO", 1),
    ]
    assert "lower(condition_id)=lower" in market.TOKEN_UPDATE
    assert "ON CONFLICT DO NOTHING" in market.TOKEN_INSERT


def test_closed_market_keeps_registry_tokens_active(monkeypatch):
    calls = []

    def capture(conn, sql, rows):
        materialized = list(rows)
        calls.append((sql, materialized))
        return len(materialized)

    monkeypatch.setattr(market, "execute_values", capture)
    row = market.normalize_market(gamma_market(active=False, closed=True))
    assert row["active"] is False
    assert row["closed"] is True

    market.upsert_markets(ExactConn(), [row])

    assert [item["active"] for item in calls[-1][1]] == [True, True]
    assert all(
        field in market.MARKET_UPSERT
        for field in ("active", "closed", "accepting_orders", "enable_order_book")
    )


def test_missing_gamma_trading_flags_fail_closed():
    row = market.normalize_market(
        gamma_market(
            active=None,
            closed=None,
            acceptingOrders=None,
            enableOrderBook=None,
        )
    )

    assert row["active"] is False
    assert row["closed"] is True
    assert row["accepting_orders"] is False
    assert row["enable_order_book"] is False


class IdentityConn:
    def __init__(self, scans, updated=None, gamma=None):
        self.scans = iter(scans)
        self.updated = updated or {"total": 0}
        self.gamma = gamma or {"noncanonical_with_gamma": 0}
        self.calls = []

    def execute(self, sql, _params=None):
        self.calls.append(sql)
        if sql == market.IDENTITY_SCAN_SQL:
            return Rows([next(self.scans)])
        if sql == market.GAMMA_KIND_SCAN_SQL:
            return Rows([self.gamma])
        if sql == market.IDENTITY_APPLY_SQL:
            return Rows([self.updated])
        return Rows([])

    def commit(self):
        self.calls.append("commit")

    def rollback(self):
        self.calls.append("rollback")


def identity_counts(**changes):
    counts = {
        "total": 712_166,
        "legacy_placeholder": 507_457,
        "legacy_combo": 203_737,
        "legacy_ghost": 957,
        "chain_v2": 14,
        "invalid_shell": 1,
        "unclassified": 0,
        "overlapping": 0,
    }
    counts.update(changes)
    return counts


def test_identity_reclassification_defaults_to_read_only_audit():
    conn = IdentityConn([identity_counts()])

    result = market.reclassify_legacy_identities(conn=conn)

    assert result["mode"] == "dry-run"
    assert result["source"]["total"] == 712_166
    assert result["noncanonical_with_gamma"] == 0
    assert result["updated"]["total"] == 0
    assert conn.calls == [market.IDENTITY_SCAN_SQL, market.GAMMA_KIND_SCAN_SQL]


def test_identity_reclassification_applies_cas_and_one_database_receipt(monkeypatch):
    updated = identity_counts()
    updated.pop("unclassified")
    updated.pop("overlapping")
    conn = IdentityConn([identity_counts()], updated)
    receipts = []
    monkeypatch.setattr(
        market,
        "set_state",
        lambda _conn, key, **values: receipts.append((key, values)),
    )

    result = market.reclassify_legacy_identities(apply=True, conn=conn)

    assert result["updated"]["total"] == 712_166
    assert result["remaining_canonical_without_gamma"] == 0
    assert result["constraints_validated"] is True
    assert conn.calls == [
        market.IDENTITY_CONSTRAINTS_SQL,
        market.IDENTITY_SCAN_SQL,
        market.GAMMA_KIND_SCAN_SQL,
        market.IDENTITY_APPLY_SQL,
        market.IDENTITY_VALIDATE_SQL,
        "commit",
    ]
    assert receipts == [
        (
            market.IDENTITY_RECLASSIFICATION_STATE,
            {"value": result, "monotonic_block": False},
        )
    ]
    assert "SET identity_kind = candidate.target_kind" in market.IDENTITY_APPLY_SQL
    assert "'legacy_combo'" in market.IDENTITY_APPLY_SQL


def test_identity_reclassification_rolls_back_unknown_or_overlapping_rows():
    conn = IdentityConn([identity_counts(unclassified=1)])

    with pytest.raises(RuntimeError, match="not exhaustive and disjoint"):
        market.reclassify_legacy_identities(apply=True, conn=conn)

    assert conn.calls[-1] == "rollback"


def test_identity_reclassification_rejects_unverified_gamma_kind():
    conn = IdentityConn(
        [identity_counts()],
        gamma={"noncanonical_with_gamma": 1},
    )

    with pytest.raises(RuntimeError, match="unverified noncanonical"):
        market.reclassify_legacy_identities(apply=True, conn=conn)

    assert conn.calls[-1] == "rollback"


def test_identity_constraints_require_real_gamma_source():
    assert "markets_canonical_gamma_source_check" in market.IDENTITY_CONSTRAINTS_SQL
    assert "NULLIF(btrim(gamma_market_id), '') IS NOT NULL" in (
        market.IDENTITY_CONSTRAINTS_SQL
    )
    assert "VALIDATE CONSTRAINT markets_canonical_gamma_source_check" in (
        market.IDENTITY_VALIDATE_SQL
    )
    assert "markets_gamma_source_kind_check" in market.IDENTITY_CONSTRAINTS_SQL


def test_cli_has_bounded_sync_and_backfill_modes():
    parser = argparse.ArgumentParser()
    market.add_cli(parser)

    sync = parser.parse_args(["sync", "--max-pages", "3", "--dry-run"])
    backfill = parser.parse_args(["backfill", "--max-pages", "2", "--watch"])
    revisit = parser.parse_args(["revisit", "--max-pages", "1", "--dry-run"])
    classify = parser.parse_args(["classify-identities"])
    classify_apply = parser.parse_args(["classify-identities", "--apply"])

    assert (sync.market_mode, sync.max_pages, sync.dry_run) == ("sync", 3, True)
    assert (backfill.market_mode, backfill.max_pages, backfill.watch) == (
        "backfill",
        2,
        True,
    )
    assert (revisit.market_mode, revisit.max_pages, revisit.dry_run) == (
        "revisit",
        1,
        True,
    )
    assert classify.apply is False
    assert classify_apply.apply is True
