from __future__ import annotations

from datetime import datetime, timezone

import pytest
import requests

from market_data import combo


CONDITION = "0x03d50a24b7e674454d1fc54e66a04100460000000000000000000000000000"
TOKEN_0 = "1733346977936095677598287638986609490683288367677675840260301941168951787520"
TOKEN_1 = "1733346977936095677598287638986609490683288367677675840260301941168951787521"
TX = "0x506a34eb3796d19a744ac87b9d7277b526fdd51f985492c4622dbfc298a6be04"
USER_0 = "0xca9cfd4aca864886c958328a36dca598d1026b9a"
USER_1 = "0x583582716cc49794789cacdf7aeffbdbd0b79404"
TITLE = (
    "Washington Nationals vs. Los Angeles Dodgers AND Athletics vs. Seattle "
    "Mariners AND Will FC Barcelona win on 2026-09-06?"
)


def raw(token_id: str, user: str, *, contract: str = combo.E333_EXCHANGE):
    return {
        "contract": contract,
        "tx_hash": TX,
        "token_id": token_id,
        "maker": user,
        "taker": combo.E333_EXCHANGE,
        "block_time": datetime(2026, 9, 6, 3, 26, 27, tzinfo=timezone.utc),
    }


def activity(token_id: str):
    return {
        "transactionHash": TX,
        "asset": token_id,
        "conditionId": CONDITION,
        "isCombo": True,
        "outcome": "",
        "outcomeIndex": 0 if token_id == TOKEN_0 else 1,
        "title": TITLE,
        "side": "BUY",
    }


def combo_detail():
    return {
        "tx_hash": TX,
        "combo_position_id": TOKEN_0,
        "combo_condition_id": CONDITION,
        "module_id": 3,
        "module_kind": "Combinatorial",
        "legs": [
            {
                "leg_outcome_label": "No",
                "market": {"title": "Washington Nationals vs. Los Angeles Dodgers"},
            },
            {
                "leg_outcome_label": "Yes",
                "market": {"title": "Athletics vs. Seattle Mariners"},
            },
        ],
    }


class Response:
    def __init__(self, payload):
        self.payload = payload
        self.closed = False

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload

    def close(self):
        self.closed = True


class ApiSession:
    def __init__(self):
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append((url, dict(params), timeout))
        if url.endswith("/activity"):
            token = TOKEN_0 if params["user"] == USER_0 else TOKEN_1
            return Response([activity(token)])
        if url.endswith("/v1/activity/combos"):
            return Response(
                {
                    "activity": [combo_detail()],
                    "pagination": {"has_more": False, "next_cursor": None},
                }
            )
        raise AssertionError(url)


class FailingSession:
    def __init__(self):
        self.calls = 0

    def get(self, url, *, params, timeout):
        self.calls += 1
        raise requests.ConnectionError("direct route unavailable")


def client(direct=None, fallback=None):
    return combo.OfficialComboClient(
        direct_session=direct or ApiSession(),
        fallback_session=fallback,
        allow_env_proxy_fallback=fallback is not None,
    )


def test_decode_real_combo_pair_and_condition_without_yes_no_claim():
    position_0 = combo.decode_combo_position(TOKEN_0)
    position_1 = combo.decode_combo_position(TOKEN_1)

    assert position_0.condition_id == CONDITION
    assert position_1.condition_id == CONDITION
    assert position_0.pair == (TOKEN_0, TOKEN_1)
    assert position_1.pair == (TOKEN_0, TOKEN_1)
    assert (position_0.structural_slot, position_1.structural_slot) == (0, 1)


def test_resolve_uses_both_official_endpoints_when_detail_is_available():
    session = ApiSession()
    resolved = combo.resolve_official_combos(
        [raw(TOKEN_0, USER_0), raw(TOKEN_1, USER_1)], client=client(session)
    )

    assert len(resolved) == 1
    identity = resolved[0]
    assert identity["identity_kind"] == "official_combo"
    assert identity["condition_id"] == CONDITION
    assert identity["position_0_token_id"] == TOKEN_0
    assert identity["position_1_token_id"] == TOKEN_1
    assert identity["logical_semantics_status"] == "unproven"
    paths = [call[0].removeprefix(combo.DATA_API_BASE) for call in session.calls]
    assert paths[:2] == ["/activity", "/activity"]
    assert paths[2] == "/v1/activity/combos"
    assert session.calls[2][1]["market_id"] == CONDITION
    activity_params = session.calls[0][1]
    assert activity_params == {
        "user": USER_0,
        "type": "TRADE",
        "limit": 500,
        "start": 1788664587,
        "end": 1788665787,
    }


def test_combo_lifecycle_transaction_does_not_have_to_equal_fill_transaction():
    session = ApiSession()
    original = session.get

    def lifecycle_tx(url, *, params, timeout):
        response = original(url, params=params, timeout=timeout)
        if url.endswith("/v1/activity/combos"):
            response.payload["activity"][0]["tx_hash"] = "0x" + "ab" * 32
        return response

    session.get = lifecycle_tx
    resolved = combo.resolve_official_combos(
        [raw(TOKEN_0, USER_0), raw(TOKEN_1, USER_1)], client=client(session)
    )

    assert resolved[0]["identity_evidence_status"] == "activity_and_combo_detail"
    assert resolved[0]["combo_activity"]["tx_hash"] == "0x" + "ab" * 32


class PendingComboDetailSession(ApiSession):
    def get(self, url, *, params, timeout):
        if url.endswith("/v1/activity/combos"):
            self.calls.append((url, dict(params), timeout))
            return Response(
                {
                    "activity": [],
                    "pagination": {"has_more": False, "next_cursor": None},
                }
            )
        return super().get(url, params=params, timeout=timeout)


def test_exact_combo_activity_is_enough_when_detail_index_lags():
    session = PendingComboDetailSession()

    resolved = combo.resolve_official_combos(
        [raw(TOKEN_0, USER_0), raw(TOKEN_1, USER_1)], client=client(session)
    )

    assert len(resolved) == 1
    identity = resolved[0]
    assert identity["condition_id"] == CONDITION
    assert identity["identity_evidence_status"] == "exact_activity_combo_detail_pending"
    assert identity["combo_activity"] == {
        "evidence_status": "not_indexed",
        "condition_id": CONDITION,
    }
    assert identity["logical_semantics_status"] == "unproven"


def test_direct_connection_failure_uses_configured_environment_route_once():
    direct = FailingSession()
    fallback = ApiSession()
    resolved = combo.resolve_official_combos(
        [raw(TOKEN_0, USER_0), raw(TOKEN_1, USER_1)],
        client=client(direct, fallback),
    )

    assert len(resolved) == 1
    assert direct.calls == 1
    assert len(fallback.calls) == 3


def test_rejects_non_e333_and_condition_mismatch():
    with pytest.raises(combo.ComboResolutionError, match="only e333"):
        combo.resolve_official_combos(
            [raw(TOKEN_0, USER_0, contract="0xe111180000d2663c0091e4f400237545b87b996b")],
            client=client(),
        )

    session = ApiSession()
    original = session.get

    def wrong_condition(url, *, params, timeout):
        response = original(url, params=params, timeout=timeout)
        if url.endswith("/activity"):
            response.payload[0]["conditionId"] = "0x" + "ab" * 31
        return response

    session.get = wrong_condition
    with pytest.raises(combo.ComboResolutionError, match="condition conflicts"):
        combo.resolve_official_combos([raw(TOKEN_0, USER_0)], client=client(session))


class Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class MemoryConn:
    def __init__(self):
        self.markets = {}
        self.tokens = {}
        self.next_market_id = 100

    def execute(self, sql, params=None):
        compact = " ".join(sql.split())
        if "SELECT pg_get_constraintdef" in compact:
            return Result([{"definition": "CHECK identity_kind IN (official_combo)"}])
        if compact.startswith("SELECT token_id, market_id, condition_id FROM core.market_tokens"):
            wanted = set(params[0])
            return Result(
                [
                    {k: row[k] for k in ("token_id", "market_id", "condition_id")}
                    for token_id, row in self.tokens.items()
                    if token_id in wanted
                ]
            )
        if compact.startswith("INSERT INTO core.markets"):
            condition = params[1]
            if condition in self.markets:
                return Result()
            market_id = self.next_market_id
            self.next_market_id += 1
            self.markets[condition] = {
                "id": market_id,
                "identity_kind": "official_combo",
                "gamma_market_id": None,
                "question_id": None,
                "condition_id": condition,
                "yes_token_id": params[2],
                "no_token_id": params[3],
            }
            return Result([{"id": market_id}])
        if compact.startswith("SELECT id, identity_kind"):
            return Result([self.markets[params[0]]]) if params[0] in self.markets else Result()
        if compact.startswith("UPDATE core.markets SET identity_kind='official_combo'"):
            market_id, condition, yes_token, no_token = params
            row = self.markets.get(condition)
            if (
                row
                and row["id"] == market_id
                and row["identity_kind"] == "legacy_combo"
                and row["yes_token_id"] == yes_token
                and row["no_token_id"] == no_token
            ):
                row["identity_kind"] = "official_combo"
                return Result([{"id": market_id}])
            return Result()
        if compact.startswith("UPDATE core.markets"):
            return Result()
        if compact.startswith("INSERT INTO core.market_tokens"):
            token_id = params[3]
            expected = {
                    "token_id": token_id,
                    "market_id": params[1],
                    "condition_id": params[2],
                    "outcome": params[4],
                    "outcome_index": params[5],
                }
            current = self.tokens.get(token_id)
            if current is None:
                self.tokens[token_id] = expected
            elif (
                current["market_id"] == expected["market_id"]
                and current["condition_id"].lower() == expected["condition_id"].lower()
            ):
                current.update(
                    outcome=expected["outcome"], outcome_index=expected["outcome_index"]
                )
            return Result()
        if compact.startswith("SELECT token_id, market_id, condition_id, outcome"):
            wanted = set(params[0])
            return Result([row for token, row in self.tokens.items() if token in wanted])
        if compact.startswith(("CREATE ", "ALTER ")):
            return Result()
        raise AssertionError(compact)


def test_resolve_and_install_is_idempotent_and_never_writes_yes_no(monkeypatch):
    monkeypatch.setattr(combo, "ensure_market_schema", lambda conn: None)
    conn = MemoryConn()
    rows = [raw(TOKEN_0, USER_0), raw(TOKEN_1, USER_1)]

    first = combo.resolve_and_install(conn, rows, client=client())
    second = combo.resolve_and_install(conn, rows, client=client())

    assert first["markets_created"] == 1
    assert second["markets_created"] == 0
    assert first["tokens_installed"] == second["tokens_installed"] == 2
    assert {row["outcome"] for row in conn.tokens.values()} == {
        "position_0",
        "position_1",
    }
    market = next(iter(conn.markets.values()))
    assert market["identity_kind"] == "official_combo"
    assert market["gamma_market_id"] is None
    assert market["question_id"] is None


def test_install_refuses_existing_foreign_token_owner(monkeypatch):
    monkeypatch.setattr(combo, "ensure_market_schema", lambda conn: None)
    conn = MemoryConn()
    conn.tokens[TOKEN_0] = {
        "token_id": TOKEN_0,
        "market_id": 7,
        "condition_id": "0x" + "12" * 31,
        "outcome": "YES",
        "outcome_index": 0,
    }

    with pytest.raises(combo.ComboIdentityConflict, match="already belongs"):
        combo.resolve_and_install(conn, [raw(TOKEN_0, USER_0)], client=client())


def test_install_repairs_yes_no_labels_on_existing_official_combo(monkeypatch):
    monkeypatch.setattr(combo, "ensure_market_schema", lambda conn: None)
    conn = MemoryConn()
    conn.markets[CONDITION] = {
        "id": 100,
        "identity_kind": "official_combo",
        "gamma_market_id": None,
        "question_id": None,
        "condition_id": CONDITION,
        "yes_token_id": TOKEN_0,
        "no_token_id": TOKEN_1,
    }
    for slot, token_id in enumerate((TOKEN_0, TOKEN_1)):
        conn.tokens[token_id] = {
            "token_id": token_id,
            "market_id": 100,
            "condition_id": CONDITION,
            "outcome": "YES" if slot == 0 else "NO",
            "outcome_index": slot,
        }

    receipt = combo.resolve_and_install(
        conn, [raw(TOKEN_0, USER_0), raw(TOKEN_1, USER_1)], client=client()
    )

    assert receipt["markets_created"] == 0
    assert [conn.tokens[token]["outcome"] for token in (TOKEN_0, TOKEN_1)] == [
        "position_0",
        "position_1",
    ]


def test_exact_official_evidence_promotes_matching_legacy_combo(monkeypatch):
    monkeypatch.setattr(combo, "ensure_market_schema", lambda conn: None)
    conn = MemoryConn()
    conn.markets[CONDITION] = {
        "id": 100,
        "identity_kind": "legacy_combo",
        "gamma_market_id": None,
        "question_id": None,
        "condition_id": CONDITION,
        "yes_token_id": TOKEN_0,
        "no_token_id": TOKEN_1,
    }

    receipt = combo.resolve_and_install(
        conn, [raw(TOKEN_0, USER_0), raw(TOKEN_1, USER_1)], client=client()
    )

    assert receipt["markets_created"] == 0
    assert conn.markets[CONDITION]["identity_kind"] == "official_combo"
