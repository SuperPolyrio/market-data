"""Official Data API identity resolution for 2026 Exchange combo assets.

This module handles only the e333 Combinatorial Exchange.  A combo token
contains a 31-byte condition identity and a final structural position bit.
Neither that bit nor the Data API's blank top-level ``outcome`` field proves
logical YES/NO semantics, so installed token labels remain ``position_0`` and
``position_1``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from market_data.market import ensure_schema as ensure_market_schema


E333_EXCHANGE = "0xe3333700ca9d93003f00f0f71f8515005f6c00aa"
DATA_API_BASE = "https://data-api.polymarket.com"
COMBO_MODULE_ID = 3
TX_HASH_RE = re.compile(r"0x[0-9a-f]{64}")


class ComboResolutionError(RuntimeError):
    """Official combo evidence is absent, malformed, or contradictory."""


class ComboIdentityConflict(RuntimeError):
    """A condition or token is already owned by a different identity."""


@dataclass(frozen=True)
class ComboPosition:
    token_id: str
    condition_id: str
    structural_slot: int
    position_0_token_id: str
    position_1_token_id: str

    @property
    def pair(self) -> tuple[str, str]:
        return self.position_0_token_id, self.position_1_token_id


def decode_combo_position(token_id: Any) -> ComboPosition:
    """Decode and strictly validate the e333 ``Ids.sol`` position layout."""

    if isinstance(token_id, bool):
        raise ComboResolutionError("combo token is not a canonical uint256")
    text = str(token_id or "").strip()
    if not text.isdigit() or (len(text) > 1 and text.startswith("0")):
        raise ComboResolutionError("combo token is not a canonical decimal uint256")
    value = int(text)
    if value <= 0 or value >= 1 << 256:
        raise ComboResolutionError("combo token is outside uint256 range")
    raw = value.to_bytes(32, "big")
    if raw[0] != COMBO_MODULE_ID:
        raise ComboResolutionError("token is not a combinatorial-module position")
    if raw[-1] not in (0, 1):
        raise ComboResolutionError("combo structural position bit is invalid")
    # Ids.sol: module(1), baseHash(16), arity(2), reserved(10),
    # conditionIndex(2), outcome(1).  e333 emits arity/index zero here.
    if any(raw[17:31]):
        raise ComboResolutionError("combo token has a noncanonical structured id")
    slot_0 = int.from_bytes(raw[:-1] + b"\x00", "big")
    slot_1 = int.from_bytes(raw[:-1] + b"\x01", "big")
    return ComboPosition(
        token_id=text,
        condition_id="0x" + raw[:-1].hex(),
        structural_slot=raw[-1],
        position_0_token_id=str(slot_0),
        position_1_token_id=str(slot_1),
    )


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _hex(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text and not text.startswith("0x"):
        text = "0x" + text
    return text


def _block_epoch(value: Any) -> int:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    else:
        text = str(value or "").strip()
        if not text:
            raise ComboResolutionError("raw combo row has no block_time")
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ComboResolutionError("raw combo row has invalid block_time") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _participants(raw: Mapping[str, Any]) -> tuple[str, ...]:
    users: list[str] = []
    for field in ("maker", "taker"):
        address = _hex(raw.get(field))
        if address and address != E333_EXCHANGE and address not in users:
            users.append(address)
    if not users:
        raise ComboResolutionError("raw combo row has no non-exchange participant")
    return tuple(users)


def _session(*, trust_env: bool, retries: int) -> requests.Session:
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=0.25,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(("GET",)),
        raise_on_status=False,
    )
    session = requests.Session()
    session.trust_env = trust_env
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class OfficialComboClient:
    """Direct-first Data API client with an optional environment-proxy retry."""

    def __init__(
        self,
        *,
        base_url: str = DATA_API_BASE,
        timeout: float = 10.0,
        max_combo_pages: int = 2,
        allow_env_proxy_fallback: bool = True,
        direct_session: requests.Session | None = None,
        fallback_session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_combo_pages = max(1, max_combo_pages)
        self.allow_env_proxy_fallback = allow_env_proxy_fallback
        # Direct is a quick probe.  Retrying the same unavailable route before
        # trying the configured proxy would stall live projection.
        self.direct = direct_session or _session(trust_env=False, retries=0)
        self.fallback = fallback_session or (
            _session(trust_env=True, retries=3) if allow_env_proxy_fallback else None
        )
        self._direct_unavailable = False
        self._cache: dict[tuple[str, tuple[tuple[str, str], ...]], Any] = {}

    def _request(self, path: str, params: Mapping[str, Any]) -> Any:
        cache_key = (
            path,
            tuple(sorted((str(key), str(value)) for key, value in params.items())),
        )
        if cache_key in self._cache:
            return self._cache[cache_key]
        sessions: list[requests.Session] = []
        if not self._direct_unavailable:
            sessions.append(self.direct)
        if self.fallback is not None and self.fallback not in sessions:
            sessions.append(self.fallback)
        last_error: Exception | None = None
        for index, session in enumerate(sessions):
            try:
                response = session.get(
                    f"{self.base_url}{path}",
                    params=dict(params),
                    timeout=(min(self.timeout, 2.0), self.timeout),
                )
                response.raise_for_status()
                payload = response.json()
                response.close()
                self._cache[cache_key] = payload
                return payload
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if index == 0 and session is self.direct:
                    self._direct_unavailable = True
        raise ComboResolutionError(f"Data API request failed for {path}: {last_error}")

    def activity_trade(
        self, raw: Mapping[str, Any], position: ComboPosition
    ) -> Mapping[str, Any]:
        tx_hash = _hex(raw.get("tx_hash"))
        if not TX_HASH_RE.fullmatch(tx_hash):
            raise ComboResolutionError("raw combo row has invalid tx_hash")
        timestamp = _block_epoch(raw.get("block_time"))
        for user in _participants(raw):
            payload = self._request(
                "/activity",
                {
                    "user": user,
                    "type": "TRADE",
                    "limit": 500,
                    "start": max(0, timestamp - 600),
                    "end": timestamp + 600,
                },
            )
            if not isinstance(payload, list):
                raise ComboResolutionError("Data API /activity returned a non-list payload")
            for item in payload:
                if not isinstance(item, Mapping):
                    continue
                if _hex(item.get("transactionHash")) != tx_hash:
                    continue
                if str(item.get("asset") or "").strip() != position.token_id:
                    continue
                if item.get("isCombo") is not True:
                    raise ComboResolutionError("exact Data API trade is not marked as combo")
                condition = _hex(item.get("conditionId"))
                if condition != position.condition_id:
                    raise ComboResolutionError("Data API activity condition conflicts with token encoding")
                if _text(item.get("outcome")):
                    raise ComboResolutionError("unexpected logical combo outcome requires review")
                if not _text(item.get("title")):
                    raise ComboResolutionError("Data API combo trade has no title")
                return item
        raise ComboResolutionError(
            f"no exact Data API activity trade for {tx_hash}/{position.token_id}"
        )

    def combo_activity(
        self, raws: Sequence[Mapping[str, Any]], position: ComboPosition
    ) -> Mapping[str, Any]:
        users: list[str] = []
        for raw in raws:
            for user in _participants(raw):
                if user not in users:
                    users.append(user)
        for user in users:
            cursor = ""
            for _ in range(self.max_combo_pages):
                # Combo activity records lifecycle events (for example SPLIT),
                # not necessarily the OrderFilled transaction itself.  Bind
                # the lookup to the encoded condition instead of incorrectly
                # requiring both transaction hashes to be equal.
                params: dict[str, Any] = {
                    "user": user,
                    "market_id": position.condition_id,
                    "limit": 500,
                }
                if cursor:
                    params["cursor"] = cursor
                payload = self._request("/v1/activity/combos", params)
                if not isinstance(payload, Mapping) or not isinstance(
                    payload.get("activity"), list
                ):
                    raise ComboResolutionError(
                        "Data API /v1/activity/combos returned an invalid payload"
                    )
                for item in payload["activity"]:
                    if not isinstance(item, Mapping):
                        continue
                    if str(item.get("combo_position_id") or "").strip() not in position.pair:
                        continue
                    if int(item.get("module_id") or 0) != COMBO_MODULE_ID:
                        raise ComboResolutionError("exact combo activity has wrong module_id")
                    if _hex(item.get("combo_condition_id")) != position.condition_id:
                        raise ComboResolutionError("combo detail condition conflicts with token encoding")
                    if not isinstance(item.get("legs"), list) or not item["legs"]:
                        raise ComboResolutionError("official combo detail has no legs")
                    return item
                pagination = payload.get("pagination")
                pagination = pagination if isinstance(pagination, Mapping) else {}
                cursor = _text(pagination.get("next_cursor"))
                if not cursor:
                    break
        raise ComboResolutionError(
            f"no exact Data API combo detail for {position.condition_id}"
        )


def _title_from_legs(combo: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for leg in combo.get("legs") or []:
        if not isinstance(leg, Mapping):
            continue
        market = leg.get("market") if isinstance(leg.get("market"), Mapping) else {}
        title = _text(market.get("title"))
        outcome = _text(market.get("outcome") or leg.get("leg_outcome_label"))
        if title and outcome and outcome.casefold() not in title.casefold():
            title = f"{title}: {outcome}"
        if title:
            parts.append(title)
    return " AND ".join(parts)


def resolve_official_combos(
    raw_rows: Iterable[Mapping[str, Any]], *, client: OfficialComboClient | None = None
) -> list[dict[str, Any]]:
    """Resolve raw e333 rows into source-proven, structurally labelled identities."""

    client = client or OfficialComboClient()
    grouped: dict[str, list[tuple[Mapping[str, Any], ComboPosition]]] = {}
    for raw in raw_rows:
        if _hex(raw.get("contract")) != E333_EXCHANGE:
            raise ComboResolutionError("official combo resolver accepts only e333 rows")
        position = decode_combo_position(raw.get("token_id"))
        grouped.setdefault(position.condition_id, []).append((raw, position))
    resolved: list[dict[str, Any]] = []
    for condition_id in sorted(grouped):
        observations = grouped[condition_id]
        exemplar = observations[0][1]
        by_key: dict[tuple[str, str], tuple[Mapping[str, Any], ComboPosition]] = {}
        for raw, position in observations:
            by_key.setdefault((_hex(raw.get("tx_hash")), position.token_id), (raw, position))
        activities = [client.activity_trade(raw, position) for raw, position in by_key.values()]
        titles = {_text(item.get("title")) for item in activities}
        if len(titles) != 1:
            raise ComboResolutionError("official activity rows disagree on combo title")
        try:
            combo = client.combo_activity([raw for raw, _ in observations], exemplar)
            evidence_status = "activity_and_combo_detail"
        except ComboResolutionError as exc:
            # The exact /activity row already binds tx + asset + encoded
            # condition and explicitly marks the trade as a combo.  The
            # secondary combo-detail index can lag that source.  Its absence
            # does not weaken structural identity; it only means leg metadata
            # is not available yet.  Contradictory or malformed detail still
            # fails closed above and is never accepted here.
            if not str(exc).startswith("no exact Data API combo detail for "):
                raise
            combo = {
                "evidence_status": "not_indexed",
                "condition_id": condition_id,
            }
            evidence_status = "exact_activity_combo_detail_pending"
        title = next(iter(titles)) or _title_from_legs(combo)
        created_at = min(
            datetime.fromtimestamp(_block_epoch(raw.get("block_time")), tz=timezone.utc)
            for raw, _ in observations
        )
        resolved.append(
            {
                "identity_kind": "official_combo",
                "condition_id": condition_id,
                "position_0_token_id": exemplar.position_0_token_id,
                "position_1_token_id": exemplar.position_1_token_id,
                "title": title,
                "created_at": created_at,
                "source_tx_hashes": sorted({_hex(raw.get("tx_hash")) for raw, _ in observations}),
                "source_assets": sorted({position.token_id for _, position in observations}),
                "activity": [dict(item) for item in activities],
                "combo_activity": dict(combo),
                "identity_evidence_status": evidence_status,
                "logical_semantics_status": "unproven",
            }
        )
    return resolved


def ensure_schema(conn: Any) -> None:
    """Ensure the shared registry can distinguish official combo identities."""

    ensure_market_schema(conn)
    row = conn.execute(
        """
        SELECT pg_get_constraintdef(oid) AS definition
        FROM pg_constraint
        WHERE conrelid='core.markets'::regclass
          AND conname='markets_identity_kind_check'
        """
    ).fetchone()
    definition = ""
    if row is not None:
        definition = str(
            row.get("definition", "") if isinstance(row, Mapping) else row[0]
        )
    if "official_combo" not in definition:
        conn.execute(
            "ALTER TABLE core.markets DROP CONSTRAINT IF EXISTS markets_identity_kind_check"
        )
        conn.execute(
            """
            ALTER TABLE core.markets ADD CONSTRAINT markets_identity_kind_check
            CHECK (identity_kind IN ('canonical_gamma', 'protocol_structural', 'official_combo'))
            NOT VALID
            """
        )


def _row_value(row: Any, name: str, index: int) -> Any:
    return row[name] if isinstance(row, Mapping) else row[index]


def _install_one(conn: Any, combo: Mapping[str, Any]) -> tuple[int, bool]:
    condition = str(combo["condition_id"])
    pair = [str(combo["position_0_token_id"]), str(combo["position_1_token_id"])]
    owners = conn.execute(
        """
        SELECT token_id, market_id, condition_id
        FROM core.market_tokens WHERE token_id = ANY(%s)
        """,
        (pair,),
    ).fetchall()
    for owner in owners:
        if str(_row_value(owner, "condition_id", 2)).lower() != condition.lower():
            raise ComboIdentityConflict(
                f"token {str(_row_value(owner, 'token_id', 0))} already belongs to "
                f"{str(_row_value(owner, 'condition_id', 2))}"
            )

    inserted = conn.execute(
        """
        INSERT INTO core.markets (
            identity_kind, gamma_market_id, event_id, event_slug, event_title,
            slug, condition_id, question_id, oracle, yes_token_id, no_token_id,
            title, description, enable_neg_risk, created_at, raw_created_at,
            category, tags, clob_token_ids, migrated_at
        ) VALUES (
            'official_combo', NULL, NULL, NULL, NULL, %s, %s, NULL, NULL, %s, %s,
            %s, %s, FALSE, %s, %s, 'combo', %s::jsonb, %s::jsonb, now()
        ) ON CONFLICT (condition_id) DO NOTHING RETURNING id
        """,
        (
            f"official-combo-{condition[2:18]}",
            condition,
            pair[0],
            pair[1],
            combo["title"],
            "Official Polymarket Data API combo; structural positions only, logical YES/NO unproven.",
            combo["created_at"],
            combo["created_at"].isoformat(),
            json.dumps(["combo", "official-combo", "data-api", "logical-outcome-unproven"]),
            json.dumps(pair),
        ),
    ).fetchone()
    created = inserted is not None
    market = conn.execute(
        """
        SELECT id, identity_kind, gamma_market_id, question_id, condition_id,
               yes_token_id, no_token_id
        FROM core.markets WHERE lower(condition_id)=lower(%s)
        """,
        (condition,),
    ).fetchone()
    if market is None:
        raise ComboIdentityConflict("combo market insert did not produce an identity")
    market_id = int(_row_value(market, "id", 0))
    actual_pair = [
        str(_row_value(market, "yes_token_id", 5)),
        str(_row_value(market, "no_token_id", 6)),
    ]
    identity_kind = str(_row_value(market, "identity_kind", 1))
    if (
        identity_kind == "legacy_combo"
        and not _text(_row_value(market, "gamma_market_id", 2))
        and not _text(_row_value(market, "question_id", 3))
        and str(_row_value(market, "condition_id", 4)).lower()
        == condition.lower()
        and actual_pair == pair
    ):
        promoted = conn.execute(
            """
            UPDATE core.markets SET identity_kind='official_combo', migrated_at=now()
            WHERE id=%s AND identity_kind='legacy_combo'
              AND NULLIF(btrim(gamma_market_id), '') IS NULL
              AND NULLIF(btrim(question_id), '') IS NULL
              AND lower(condition_id)=lower(%s)
              AND yes_token_id=%s AND no_token_id=%s
            RETURNING id
            """,
            (market_id, condition, pair[0], pair[1]),
        ).fetchone()
        if promoted is None:
            raise ComboIdentityConflict("legacy combo identity changed during promotion")
        identity_kind = "official_combo"
    if (
        identity_kind != "official_combo"
        or _text(_row_value(market, "gamma_market_id", 2))
        or _text(_row_value(market, "question_id", 3))
        or str(_row_value(market, "condition_id", 4)).lower() != condition.lower()
        or actual_pair != pair
    ):
        raise ComboIdentityConflict(
            f"condition {condition} is already occupied by a non-exact identity"
        )
    conn.execute(
        """
        UPDATE core.markets SET title=%s, created_at=COALESCE(created_at, %s),
            raw_created_at=COALESCE(raw_created_at, %s), migrated_at=now()
        WHERE id=%s AND identity_kind='official_combo'
        """,
        (combo["title"], combo["created_at"], combo["created_at"].isoformat(), market_id),
    )

    for slot, token_id in enumerate(pair):
        conn.execute(
            """
            INSERT INTO core.market_tokens (
                id, market_id, condition_id, token_id, outcome, outcome_index,
                active, created_at, raw_created_at, updated_at, raw_updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s, now(), %s)
            ON CONFLICT (token_id) DO UPDATE SET
                outcome=EXCLUDED.outcome,
                outcome_index=EXCLUDED.outcome_index,
                active=TRUE,
                updated_at=now(),
                raw_updated_at=EXCLUDED.raw_updated_at
            WHERE core.market_tokens.market_id=EXCLUDED.market_id
              AND lower(core.market_tokens.condition_id)=lower(EXCLUDED.condition_id)
            """,
            (
                -((market_id * 2) - 1) if slot == 0 else -(market_id * 2),
                market_id,
                condition,
                token_id,
                f"position_{slot}",
                slot,
                combo["created_at"],
                combo["created_at"].isoformat(),
                combo["created_at"].isoformat(),
            ),
        )
    stored = conn.execute(
        """
        SELECT token_id, market_id, condition_id, outcome, outcome_index
        FROM core.market_tokens WHERE token_id = ANY(%s)
        """,
        (pair,),
    ).fetchall()
    actual = {
        str(_row_value(row, "token_id", 0)): (
            int(_row_value(row, "market_id", 1)),
            str(_row_value(row, "condition_id", 2)).lower(),
            str(_row_value(row, "outcome", 3)),
            int(_row_value(row, "outcome_index", 4)),
        )
        for row in stored
    }
    expected = {
        token_id: (market_id, condition.lower(), f"position_{slot}", slot)
        for slot, token_id in enumerate(pair)
    }
    if actual != expected:
        raise ComboIdentityConflict("combo token pair was not installed exactly")
    return market_id, created


def resolve_and_install(
    conn: Any,
    raw_rows: Iterable[Mapping[str, Any]],
    *,
    client: OfficialComboClient | None = None,
) -> dict[str, Any]:
    """Resolve official identities and install them without committing the caller's transaction."""

    rows = list(raw_rows)
    if not rows:
        return {
            "status": "complete",
            "raw_rows": 0,
            "conditions": 0,
            "markets_created": 0,
            "tokens_installed": 0,
            "identities": [],
        }
    combos = resolve_official_combos(rows, client=client)
    ensure_schema(conn)
    identities: list[dict[str, Any]] = []
    created = 0
    for combo in combos:
        market_id, was_created = _install_one(conn, combo)
        created += int(was_created)
        identities.append(
            {
                "market_id": market_id,
                "identity_kind": "official_combo",
                "condition_id": combo["condition_id"],
                "position_0_token_id": combo["position_0_token_id"],
                "position_1_token_id": combo["position_1_token_id"],
                "logical_semantics_status": "unproven",
                "source_tx_hashes": combo["source_tx_hashes"],
            }
        )
    return {
        "status": "complete",
        "raw_rows": len(rows),
        "conditions": len(combos),
        "markets_created": created,
        "tokens_installed": len(combos) * 2,
        "identities": identities,
    }
