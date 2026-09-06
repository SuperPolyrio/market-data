from __future__ import annotations

import json

import pytest
import requests

from market_data import market, market_recovery


def gamma_market(identity="123", **changes):
    return {
        "id": identity,
        "conditionId": f"0x{identity}",
        "clobTokenIds": [f"{identity}-yes", f"{identity}-no"],
        "outcomes": ["Yes", "No"],
        **changes,
    }


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class Response:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class Session:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, *, timeout):
        self.calls.append((url, timeout))
        response = self.responses[url.rsplit("/", 1)[-1]]
        if isinstance(response, Exception):
            raise response
        return response


class RecoveryConn:
    def __init__(self, identities, *, conflict=False):
        self.identities = identities
        self.conflict = conflict
        self.calls = []
        self.receipts = {}
        self.failure_receipts = {}
        self.persisted = []
        self.open_versions = {identity: {identity: 1} for identity in identities}

    def execute(self, sql, params=None):
        self.calls.append(sql)
        if sql == market_recovery.PENDING_SQL:
            return Rows([
                {"source_identity": identity, "failure_versions": dict(self.open_versions[identity])}
                for identity in self.identities
            ])
        if sql == market_recovery.REMAINING_SQL:
            return Rows([
                {"source_identity": identity, "failure_versions": dict(versions)}
                for identity, versions in self.open_versions.items() if versions
            ])
        if sql == market_recovery.RECEIPT_SQL:
            if params["status"] == "resolved":
                assert params["source_identity"] in self.persisted
            snapshot = json.loads(params["snapshot_json"])
            versions = self.open_versions[params["source_identity"]]
            matched = [key for key, version in versions.items() if snapshot.get(key) == version]
            receipt = {
                "status": params["status"],
                "detail": json.loads(params["detail_json"]),
            }
            if matched:
                self.receipts[params["source_identity"]] = receipt
            for key in matched:
                self.failure_receipts[key] = receipt
                if params["status"] == "resolved":
                    del versions[key]
            return Rows([{"failure_key": key} for key in matched])
        if "SELECT id AS market_id" in sql:
            return Rows([])
        if "mt.token_id, mt.market_id" in sql:
            return Rows([{
                "token_id": "123-yes", "market_id": 999,
                "token_condition_id": "0xowner", "market_condition_id": "0xowner",
                "identity_kind": "canonical_gamma", "gamma_market_id": "owner",
                "category": "sports", "slug": "real-owner",
            }] if self.conflict else [])
        raise AssertionError(f"unexpected SQL: {sql}")

    def commit(self):
        self.calls.append("commit")

    def rollback(self):
        self.calls.append("rollback")

    def close(self):
        self.calls.append("close")


def fake_persist(conn, rows, promotions):
    assert not promotions
    conn.persisted.extend(str(row["gamma_market_id"]) for row in rows)
    return len(rows)


@pytest.mark.parametrize("own_conn", [False, True])
def test_recovery_closes_only_fresh_verified_market_and_preserves_failures(
    monkeypatch, own_conn
):
    conn = RecoveryConn(["123", "124", "125", "126", "127"])
    session = Session({
        "123": Response(gamma_market()),
        "124": Response(gamma_market("124", conditionId="")),
        "125": Response({"error": "not found"}, 404),
        "126": Response(gamma_market("wrong-id")),
        "127": requests.Timeout("connection stalled"),
    })
    monkeypatch.setattr(market, "upsert_markets", fake_persist)
    monkeypatch.setattr(market, "connect_postgres", lambda: conn)

    result = market_recovery.retry_normalization_failures(
        session=session, conn=None if own_conn else conn
    )

    assert result["selected"] == 5
    assert result["persisted"] == result["resolved"] == 1
    assert result["remaining_open_selected"] == 4
    assert conn.persisted == ["123"]
    assert conn.receipts["123"]["status"] == "resolved"
    assert conn.receipts["123"]["detail"]["recovery_condition_id"] == "0x123"
    assert conn.receipts["123"]["detail"]["recovery_latest_raw"] == gamma_market()
    assert all(conn.receipts[identity]["status"] == "open" for identity in ["124", "125", "126", "127"])
    assert conn.receipts["125"]["detail"]["recovery_http_status"] == 404
    assert conn.receipts["127"]["detail"]["recovery_status"] == "request_error"
    assert all(timeout == 60 for _, timeout in session.calls)
    assert all("/markets/" in url for url, _ in session.calls)
    assert (conn.calls[-2:] == ["commit", "close"]) is own_conn
    assert "rollback" not in conn.calls


def test_recovery_keeps_nonreplaceable_token_owner_open(monkeypatch):
    conn = RecoveryConn(["123"], conflict=True)
    monkeypatch.setattr(market, "upsert_markets", fake_persist)

    result = market_recovery.retry_normalization_failures(
        session=Session({"123": Response(gamma_market())}), conn=conn
    )

    assert result["canonical"] == 1
    assert result["persisted"] == result["resolved"] == 0
    assert conn.persisted == []
    assert conn.receipts["123"]["status"] == "open"
    detail = conn.receipts["123"]["detail"]
    assert detail["recovery_status"] == "ownership_conflict"
    assert detail["recovery_conflicts"][0]["existing"] == "0xowner"


def test_recovery_dry_run_reads_existing_state_without_writes(monkeypatch):
    conn = RecoveryConn(["123"])

    def forbidden(*args, **kwargs):
        raise AssertionError("dry run attempted a write")

    monkeypatch.setattr(market, "upsert_markets", forbidden)
    monkeypatch.setattr(market, "record_promotions", forbidden)
    monkeypatch.setattr(market, "ensure_schema", forbidden)
    monkeypatch.setattr(market, "connect_postgres", lambda: conn)

    result = market_recovery.retry_normalization_failures(
        dry_run=True, session=Session({"123": Response(gamma_market())})
    )

    assert result["canonical"] == 1
    assert result["persisted"] == result["resolved"] == 0
    assert result["results"][0]["recovery_status"] == "canonical"
    assert conn.receipts == {}
    assert all(sql.lstrip().startswith("SELECT") for sql in conn.calls[:-1])
    assert conn.calls[-1] == "close"


@pytest.mark.parametrize("partial", [False, True])
@pytest.mark.parametrize("recoverable", [False, True])
def test_recovery_preserves_changed_and_new_failure_generations(
    monkeypatch, partial, recoverable
):
    conn = RecoveryConn(["123"])
    if partial:
        conn.open_versions["123"]["unchanged"] = 1

    class ConcurrentSession:
        def get(self, _url, *, timeout):
            assert timeout == 60
            conn.open_versions["123"].update({"123": 2, "new-failure": 1})
            return Response(gamma_market(conditionId="0x123" if recoverable else ""))

    monkeypatch.setattr(market, "upsert_markets", fake_persist)
    result = market_recovery.retry_normalization_failures(
        session=ConcurrentSession(), conn=conn
    )

    assert result["persisted"] == int(recoverable)
    assert result["resolved"] == int(partial and recoverable)
    assert result["resolved_identities"] == 0
    assert result["remaining_open_selected"] == 1
    assert result["results"][0]["recovery_status"] == "concurrent_reobservation"
    assert "123" not in conn.failure_receipts
    assert "new-failure" not in conn.failure_receipts
    assert conn.open_versions["123"]["123"] == 2
    assert conn.open_versions["123"]["new-failure"] == 1
    if partial:
        assert conn.failure_receipts["unchanged"]["status"] == (
            "resolved" if recoverable else "open"
        )
