"""Small Gamma-only canonical market collector.

The live lane follows newly created markets.  The independent backfill lane
walks both Gamma keysets to terminal over bounded invocations, which also
revisits markets embedded in older events.  No placeholder, combo fallback,
on-chain reconstruction, or taxonomy repair is performed here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from market_data.config import GAMMA_API_BASE
from market_data.storage import connect_postgres, execute_values, get_state, set_state

PAGE_SIZE = 100
TOKEN_LOOKUP_BATCH_SIZE = 6
CREATED_STATE = "market.gamma.created.{source}.v1"
BACKFILL_STATE = "market.gamma.backfill.{source}.v1"
REVISIT_STATE = "market.gamma.revisit.updated.{source}.v2"
LEGACY_DISCOVERY_STATE = "last_market_discovery_at"
IDENTITY_RECLASSIFICATION_STATE = "market.identity_kind.reclassification.v1"
SOURCES = ("markets", "events")
PRIMARY_CATEGORIES = (
    "politics",
    "sports",
    "crypto",
    "weather",
    "economy",
    "economics",
    "finance",
    "tech",
    "science",
    "pop-culture",
)


class TokenOwnershipConflict(RuntimeError):
    """A globally unique token is already owned by another condition."""


MARKET_UPSERT = """
INSERT INTO core.markets (
    identity_kind, gamma_market_id, event_id, event_slug, event_title,
    slug, condition_id, question_id, oracle, yes_token_id, no_token_id,
    title, description, enable_neg_risk, end_date, raw_end_date,
    created_at, raw_created_at, category, tags, clob_token_ids,
    active, closed, accepting_orders, enable_order_book,
    gamma_updated_at, market_state_observed_at, migrated_at
) VALUES (
    'canonical_gamma', %(gamma_market_id)s, %(event_id)s, %(event_slug)s,
    %(event_title)s, %(slug)s, %(condition_id)s, %(question_id)s, %(oracle)s,
    %(yes_token_id)s, %(no_token_id)s, %(title)s, %(description)s,
    %(enable_neg_risk)s, %(end_date)s, %(raw_end_date)s, %(created_at)s,
    %(raw_created_at)s, %(category)s, %(tags_json)s::jsonb,
    %(clob_token_ids_json)s::jsonb, %(active)s, %(closed)s,
    %(accepting_orders)s, %(enable_order_book)s, %(updated_at)s, now(), now()
)
ON CONFLICT (condition_id) DO UPDATE SET
    identity_kind='canonical_gamma', gamma_market_id=EXCLUDED.gamma_market_id,
    event_id=COALESCE(NULLIF(EXCLUDED.event_id, ''), core.markets.event_id),
    event_slug=COALESCE(NULLIF(EXCLUDED.event_slug, ''), core.markets.event_slug),
    event_title=COALESCE(NULLIF(EXCLUDED.event_title, ''), core.markets.event_title),
    slug=EXCLUDED.slug,
    question_id=COALESCE(NULLIF(EXCLUDED.question_id, ''), core.markets.question_id),
    oracle=COALESCE(NULLIF(EXCLUDED.oracle, ''), core.markets.oracle),
    yes_token_id=EXCLUDED.yes_token_id, no_token_id=EXCLUDED.no_token_id,
    title=EXCLUDED.title, description=EXCLUDED.description,
    enable_neg_risk=EXCLUDED.enable_neg_risk,
    end_date=COALESCE(EXCLUDED.end_date, core.markets.end_date),
    raw_end_date=COALESCE(EXCLUDED.raw_end_date, core.markets.raw_end_date),
    created_at=COALESCE(EXCLUDED.created_at, core.markets.created_at),
    raw_created_at=COALESCE(EXCLUDED.raw_created_at, core.markets.raw_created_at),
    category=COALESCE(NULLIF(EXCLUDED.category, ''), core.markets.category),
    tags=CASE WHEN EXCLUDED.tags = '[]'::jsonb THEN core.markets.tags ELSE EXCLUDED.tags END,
    clob_token_ids=EXCLUDED.clob_token_ids,
    active=EXCLUDED.active, closed=EXCLUDED.closed,
    accepting_orders=EXCLUDED.accepting_orders,
    enable_order_book=EXCLUDED.enable_order_book,
    gamma_updated_at=COALESCE(EXCLUDED.gamma_updated_at, core.markets.gamma_updated_at),
    market_state_observed_at=now(), migrated_at=now()
"""

TOKEN_UPDATE = """
UPDATE core.market_tokens SET
    market_id=%(market_id)s, outcome=%(outcome)s,
    outcome_index=%(outcome_index)s, active=%(active)s,
    end_date=%(end_date)s, raw_end_date=%(raw_end_date)s,
    created_at=COALESCE(created_at, %(created_at)s),
    raw_created_at=COALESCE(raw_created_at, %(raw_created_at)s),
    updated_at=now(), raw_updated_at=%(raw_updated_at)s
WHERE token_id=%(token_id)s
  AND lower(condition_id)=lower(%(condition_id)s)
"""

TOKEN_INSERT = """
INSERT INTO core.market_tokens (
    id, market_id, condition_id, token_id, outcome, outcome_index, active,
    end_date, raw_end_date, created_at, raw_created_at, updated_at, raw_updated_at
) VALUES (
    %(id)s, %(market_id)s, %(condition_id)s, %(token_id)s, %(outcome)s,
    %(outcome_index)s, %(active)s, %(end_date)s, %(raw_end_date)s,
    %(created_at)s, %(raw_created_at)s, now(), %(raw_updated_at)s
)
ON CONFLICT DO NOTHING
"""

FAILURE_UPSERT = """
WITH prior AS (
    SELECT failure_key, status
    FROM ops.market_acquisition_failures
    WHERE %(source_identity)s <> ''
      AND kind=%(kind)s
      AND source_identity=%(source_identity)s
      AND condition_id=%(condition_id)s
      AND token_id=%(token_id)s
    ORDER BY
        CASE WHEN left(status, 9) = 'terminal_' THEN 0 ELSE 1 END,
        last_seen_at DESC
    LIMIT 1
)
INSERT INTO ops.market_acquisition_failures (
    failure_key, mode, lane, kind, reason, source_identity,
    condition_id, token_id, raw_json, detail, status,
    first_seen_at, last_seen_at, occurrences
) VALUES (
    COALESCE((SELECT failure_key FROM prior), %(failure_key)s),
    %(mode)s, %(lane)s, %(kind)s, %(reason)s,
    %(source_identity)s, %(condition_id)s, %(token_id)s,
    %(raw_json)s::jsonb, %(detail_json)s::jsonb,
    COALESCE(
        (SELECT status FROM prior WHERE left(status, 9) = 'terminal_'),
        'open'
    ),
    now(), now(), 1
)
ON CONFLICT (failure_key) DO UPDATE SET
    reason=EXCLUDED.reason,
    raw_json=EXCLUDED.raw_json,
    detail=EXCLUDED.detail,
    status=CASE
        WHEN left(ops.market_acquisition_failures.status, 9) = 'terminal_'
        THEN ops.market_acquisition_failures.status
        ELSE 'open'
    END,
    last_seen_at=now(),
    occurrences=ops.market_acquisition_failures.occurrences + 1
"""

PROMOTION_UPSERT = """
INSERT INTO ops.market_canonical_promotions (
    promotion_key, mode, lane, kind, gamma_market_id, token_id,
    from_market_id, from_condition_id, from_identity_kind,
    to_market_id, to_condition_id, reason, detail, status,
    first_promoted_at, last_observed_at, occurrences
) VALUES (
    %(promotion_key)s, %(mode)s, %(lane)s, %(kind)s,
    %(gamma_market_id)s, %(token_id)s, %(from_market_id)s,
    %(from_condition_id)s, %(from_identity_kind)s,
    (SELECT id FROM core.markets WHERE lower(condition_id)=lower(%(to_condition_id)s)),
    %(to_condition_id)s, %(reason)s, %(detail_json)s::jsonb,
    'promoted', now(), now(), 1
)
ON CONFLICT (promotion_key) DO UPDATE SET
    detail=EXCLUDED.detail,
    status='promoted',
    last_observed_at=now(),
    occurrences=ops.market_canonical_promotions.occurrences + 1
"""

TOKEN_PROMOTE = """
UPDATE core.market_tokens SET
    market_id=%(to_market_id)s,
    condition_id=%(to_condition_id)s,
    outcome=%(outcome)s,
    outcome_index=%(outcome_index)s,
    active=TRUE,
    updated_at=now()
WHERE token_id=%(token_id)s
  AND (
      (market_id=%(from_market_id)s
       AND lower(condition_id)=lower(%(from_condition_id)s))
      OR
      (market_id=%(to_market_id)s
       AND lower(condition_id)=lower(%(to_condition_id)s))
  )
RETURNING token_id
"""

IDENTITY_CANDIDATES_SQL = """
SELECT id,
       CASE
           WHEN placeholder_marker THEN 'legacy_placeholder'
           WHEN combo_marker THEN 'legacy_combo'
           WHEN ghost_marker THEN 'legacy_ghost'
           WHEN v2_marker THEN 'chain_v2'
           WHEN invalid_marker THEN 'invalid_shell'
       END AS target_kind,
       placeholder_marker::int + combo_marker::int + ghost_marker::int
           + v2_marker::int + invalid_marker::int AS marker_count
FROM (
    SELECT id,
           lower(COALESCE(category, '')) = 'orderfilled-placeholder'
               OR lower(COALESCE(slug, '')) LIKE 'trade-indexer-placeholder-%'
               AS placeholder_marker,
           lower(COALESCE(tags::text, '')) LIKE '%data-api-combo%'
               OR (
                   lower(COALESCE(slug, '')) LIKE 'combo-%'
                   AND lower(COALESCE(tags::text, '')) LIKE '%combo%'
               ) AS combo_marker,
           lower(COALESCE(category, '')) IN ('ghost', '[ghost]')
               OR lower(COALESCE(slug, '')) LIKE 'ghost-market-%'
               OR (
                   lower(COALESCE(tags::text, '')) LIKE '%orphaned%'
                   AND lower(COALESCE(tags::text, '')) LIKE '%unknown%'
               ) AS ghost_marker,
           lower(COALESCE(slug, '')) LIKE 'onchain-condition-v2-%'
               AS v2_marker,
           lower(btrim(COALESCE(condition_id, '')))
               = '0x' || repeat('0', 64) AS invalid_marker
    FROM core.markets
    WHERE identity_kind = 'canonical_gamma'
      AND NULLIF(btrim(gamma_market_id), '') IS NULL
) AS marked
"""

IDENTITY_SCAN_SQL = f"""
WITH candidates AS MATERIALIZED ({IDENTITY_CANDIDATES_SQL})
SELECT count(*) AS total,
       count(*) FILTER (WHERE target_kind = 'legacy_placeholder')
           AS legacy_placeholder,
       count(*) FILTER (WHERE target_kind = 'legacy_combo') AS legacy_combo,
       count(*) FILTER (WHERE target_kind = 'legacy_ghost') AS legacy_ghost,
       count(*) FILTER (WHERE target_kind = 'chain_v2') AS chain_v2,
       count(*) FILTER (WHERE target_kind = 'invalid_shell') AS invalid_shell,
       count(*) FILTER (WHERE marker_count = 0) AS unclassified,
       count(*) FILTER (WHERE marker_count > 1) AS overlapping
FROM candidates
"""

IDENTITY_APPLY_SQL = f"""
WITH candidates AS MATERIALIZED ({IDENTITY_CANDIDATES_SQL}),
updated AS (
    UPDATE core.markets AS market
       SET identity_kind = candidate.target_kind
      FROM candidates AS candidate
     WHERE market.id = candidate.id
       AND candidate.marker_count = 1
       AND candidate.target_kind IS NOT NULL
       AND market.identity_kind = 'canonical_gamma'
       AND NULLIF(btrim(market.gamma_market_id), '') IS NULL
    RETURNING candidate.target_kind
)
SELECT count(*) AS total,
       count(*) FILTER (WHERE target_kind = 'legacy_placeholder')
           AS legacy_placeholder,
       count(*) FILTER (WHERE target_kind = 'legacy_combo') AS legacy_combo,
       count(*) FILTER (WHERE target_kind = 'legacy_ghost') AS legacy_ghost,
       count(*) FILTER (WHERE target_kind = 'chain_v2') AS chain_v2,
       count(*) FILTER (WHERE target_kind = 'invalid_shell') AS invalid_shell
FROM updated
"""

GAMMA_KIND_SCAN_SQL = """
SELECT count(*) AS noncanonical_with_gamma
FROM core.markets
WHERE NULLIF(btrim(gamma_market_id), '') IS NOT NULL
  AND identity_kind NOT IN ('canonical_gamma', 'canonical_clob')
"""

IDENTITY_CONSTRAINTS_SQL = """
ALTER TABLE core.markets
    DROP CONSTRAINT IF EXISTS markets_identity_kind_check,
    DROP CONSTRAINT IF EXISTS markets_canonical_gamma_source_check,
    DROP CONSTRAINT IF EXISTS markets_gamma_source_kind_check,
    ADD CONSTRAINT markets_identity_kind_check CHECK (
        identity_kind IN (
            'canonical_gamma', 'protocol_structural', 'official_combo',
            'canonical_clob', 'legacy_placeholder', 'legacy_combo',
            'legacy_ghost', 'chain_v2', 'invalid_shell'
        )
    ) NOT VALID,
    ADD CONSTRAINT markets_canonical_gamma_source_check CHECK (
        identity_kind <> 'canonical_gamma'
        OR NULLIF(btrim(gamma_market_id), '') IS NOT NULL
    ) NOT VALID,
    ADD CONSTRAINT markets_gamma_source_kind_check CHECK (
        NULLIF(btrim(gamma_market_id), '') IS NULL
        OR identity_kind IN ('canonical_gamma', 'canonical_clob')
    ) NOT VALID
"""

IDENTITY_VALIDATE_SQL = """
ALTER TABLE core.markets
    VALIDATE CONSTRAINT markets_identity_kind_check,
    VALIDATE CONSTRAINT markets_canonical_gamma_source_check,
    VALIDATE CONSTRAINT markets_gamma_source_kind_check
"""


def _http_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(("GET",)),
        raise_on_status=False,
    )
    session = requests.Session()
    # Honour the standard proxy variables.  Production supplies them through
    # the reviewed systemd drop-in; disabling ``trust_env`` here made that
    # route ineffective and left Gamma scans stuck on an unreachable direct
    # connection.
    session.trust_env = True
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _sequence(value: Any, field: str, *, required: bool = False) -> list[Any]:
    if value in (None, ""):
        if required:
            raise ValueError(f"{field} is required")
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{field} is not a JSON list") from exc
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{field} is not a list")
    return list(value)


def _timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        text = re.sub(
            r"\.(\d+)(?=Z|[+-]\d{2}:\d{2}|$)",
            lambda match: "." + match.group(1)[:6].ljust(6, "0"),
            text,
        )
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"invalid timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _tags(*values: Any) -> list[str]:
    result: list[str] = []
    for value in values:
        if value in (None, ""):
            continue
        try:
            if isinstance(value, Mapping):
                items = [value]
            else:
                items = (
                    _sequence(value, "tags") if isinstance(value, str) else list(value)
                )
        except (TypeError, ValueError):
            items = [value]
        for item in items:
            if isinstance(item, Mapping):
                item = item.get("slug") or item.get("label")
            tag = _text(item).lower()
            if tag and tag not in result:
                result.append(tag)
    return result


def _category(
    market: Mapping[str, Any], event: Mapping[str, Any], tags: list[str]
) -> str:
    candidates: list[Any] = [
        market.get("category"),
        event.get("category"),
        event.get("subcategory"),
    ]
    categories = event.get("categories") or market.get("categories") or []
    if isinstance(categories, list):
        candidates.extend(categories)
    for candidate in candidates:
        if isinstance(candidate, Mapping):
            candidate = candidate.get("slug") or candidate.get("label")
        category = _text(candidate).lower()
        if category:
            return category
    primary = next((name for name in PRIMARY_CATEGORIES if name in tags), "")
    # Gamma does not assign one of its broad categories to every market.  Keep
    # the first official tag when available and use an explicit neutral bucket
    # otherwise, so downstream consumers do not confuse an upstream taxonomy
    # omission with a failed enrichment write.
    return primary or (tags[0] if tags else "other")


def _event_for(
    market: Mapping[str, Any], supplied: Mapping[str, Any] | None
) -> Mapping[str, Any]:
    if supplied:
        return supplied
    events = market.get("events")
    return (
        events[0]
        if isinstance(events, list) and events and isinstance(events[0], Mapping)
        else {}
    )


def normalize_market(
    raw: Mapping[str, Any], event: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Normalize one official Gamma binary market, failing closed on identity."""
    gamma_id = _text(raw.get("id") or raw.get("gamma_market_id"))
    condition = _text(raw.get("conditionId") or raw.get("condition_id")).lower()
    if not gamma_id:
        raise ValueError("gamma market id is required")
    if not condition:
        raise ValueError("condition id is required")
    if not condition.startswith("0x"):
        condition = f"0x{condition}"

    token_ids = [
        _text(item)
        for item in _sequence(
            raw.get("clobTokenIds") or raw.get("clob_token_ids"),
            "clobTokenIds",
            required=True,
        )
    ]
    outcomes = [
        _text(item)
        for item in _sequence(raw.get("outcomes"), "outcomes", required=True)
    ]
    if len(token_ids) != 2 or not all(token_ids) or token_ids[0] == token_ids[1]:
        raise ValueError("clobTokenIds must contain two distinct non-empty tokens")
    if (
        len(outcomes) != 2
        or not all(outcomes)
        or outcomes[0].casefold() == outcomes[1].casefold()
    ):
        raise ValueError("outcomes must contain two distinct non-empty labels")

    labels = [label.upper() for label in outcomes]
    if set(labels) == {"YES", "NO"}:
        yes_index, no_index = labels.index("YES"), labels.index("NO")
    elif set(labels) == {"UP", "DOWN"}:
        yes_index, no_index = labels.index("UP"), labels.index("DOWN")
    else:
        yes_index, no_index = 0, 1

    prices_raw = _sequence(
        raw.get("outcomePrices") or raw.get("outcome_prices"), "outcomePrices"
    )
    prices: list[str] = []
    if prices_raw:
        if len(prices_raw) != 2:
            raise ValueError("outcomePrices must align with the two source outcomes")
        for value in prices_raw:
            try:
                price = Decimal(str(value))
            except (InvalidOperation, ValueError) as exc:
                raise ValueError(f"invalid outcome price: {value!r}") from exc
            if not Decimal("0") <= price <= Decimal("1"):
                raise ValueError(f"outcome price outside [0, 1]: {value!r}")
            prices.append(format(price, "f"))

    event_data = _event_for(raw, event)
    tags = _tags(raw.get("tags"), event_data.get("tags"))
    raw_created = raw.get("createdAt") or raw.get("created_at")
    raw_end = raw.get("endDate") or raw.get("endDateIso") or raw.get("end_date")
    oracle = raw.get("oracle")
    if isinstance(oracle, Mapping):
        oracle = oracle.get("address")
    oracle = _text(oracle or raw.get("resolvedBy") or raw.get("resolved_by"))
    question_id = _text(
        raw.get("questionID") or raw.get("questionId") or raw.get("question_id")
    )
    active = _boolean(raw.get("active"), default=False)
    closed = _boolean(raw.get("closed"), default=True)
    accepting_orders = _boolean(
        raw.get("acceptingOrders", raw.get("accepting_orders")), default=False
    )
    enable_order_book = _boolean(
        raw.get("enableOrderBook", raw.get("enable_order_book")), default=False
    )

    return {
        "_normalized_gamma": True,
        "gamma_market_id": gamma_id,
        "event_id": _text(event_data.get("id")),
        "event_slug": _text(event_data.get("slug")),
        "event_title": _text(event_data.get("title")),
        "slug": _text(raw.get("slug")) or f"market-{gamma_id}",
        "condition_id": condition,
        "question_id": question_id,
        "oracle": oracle,
        "yes_token_id": token_ids[yes_index],
        "no_token_id": token_ids[no_index],
        "yes_source_index": yes_index,
        "no_source_index": no_index,
        "title": _text(raw.get("question") or raw.get("title")),
        "description": str(raw.get("description") or "").strip(),
        "enable_neg_risk": bool(
            raw.get("negRisk")
            or event_data.get("negRisk")
            or event_data.get("enableNegRisk")
        ),
        "end_date": _timestamp(raw_end),
        "raw_end_date": _text(raw_end),
        "created_at": _timestamp(raw_created),
        "raw_created_at": _text(raw_created),
        "updated_at": _timestamp(raw.get("updatedAt") or raw.get("updated_at")),
        "active": active,
        "closed": closed,
        "accepting_orders": accepting_orders,
        "enable_order_book": enable_order_book,
        "category": _category(raw, event_data, tags),
        "tags": tags,
        "clob_token_ids": token_ids,
        "outcomes": outcomes,
        "outcome_prices": prices,
        "yes_price": prices[yes_index] if prices else None,
        "no_price": prices[no_index] if prices else None,
        "tags_json": json.dumps(tags, ensure_ascii=False),
        "clob_token_ids_json": json.dumps(token_ids),
    }


def _boolean(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, str)):
        normalized = str(value).strip().casefold()
        if normalized in {"1", "true"}:
            return True
        if normalized in {"0", "false"}:
            return False
    raise ValueError(f"invalid boolean: {value!r}")


def sync_gamma_markets_for_token_ids(
    token_ids: Iterable[Any],
    *,
    session: requests.Session | None = None,
    conn: Any | None = None,
) -> int:
    """Resolve token IDs through official Gamma and persist canonical markets."""
    requested = list(dict.fromkeys(_text(value) for value in token_ids if _text(value)))
    if not requested:
        return 0

    session = session or _http_session()
    rows: dict[str, dict[str, Any]] = {}
    requested_set = set(requested)
    for start in range(0, len(requested), TOKEN_LOOKUP_BATCH_SIZE):
        chunk = requested[start : start + TOKEN_LOOKUP_BATCH_SIZE]
        response = session.get(
            f"{GAMMA_API_BASE}/markets",
            params={"clob_token_ids": chunk, "limit": 100},
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        markets = payload.get("markets") if isinstance(payload, Mapping) else payload
        if not isinstance(markets, list):
            raise RuntimeError("Gamma /markets returned an invalid payload")
        for raw in markets:
            if not isinstance(raw, Mapping):
                continue
            try:
                row = normalize_market(raw)
            except ValueError:
                continue
            if requested_set.isdisjoint((row["yes_token_id"], row["no_token_id"])):
                continue
            rows[row["condition_id"]] = row

    if not rows:
        return 0

    own_conn = conn is None
    if own_conn:
        conn = connect_postgres()
    assert conn is not None
    try:
        # Shell promotion remains collector-owned so it always gets an audit
        # receipt. This fast path only inserts or refreshes a Gamma owner.
        persisted = upsert_markets(conn, rows.values())
        conn.commit()
        return persisted
    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def ensure_schema(conn: Any) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS core")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS core.markets_id_seq")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS core.markets (
            id BIGINT PRIMARY KEY DEFAULT nextval('core.markets_id_seq'),
            identity_kind TEXT NOT NULL DEFAULT 'canonical_gamma',
            gamma_market_id TEXT, event_id TEXT, event_slug TEXT, event_title TEXT,
            slug TEXT NOT NULL, condition_id TEXT NOT NULL UNIQUE, question_id TEXT,
            oracle TEXT, yes_token_id TEXT NOT NULL, no_token_id TEXT NOT NULL,
            title TEXT, description TEXT, enable_neg_risk BOOLEAN NOT NULL DEFAULT FALSE,
            end_date TIMESTAMPTZ, raw_end_date TEXT, created_at TIMESTAMPTZ,
            raw_created_at TEXT, category TEXT, tags JSONB NOT NULL DEFAULT '[]'::jsonb,
            clob_token_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
            active BOOLEAN NOT NULL DEFAULT FALSE,
            closed BOOLEAN NOT NULL DEFAULT TRUE,
            accepting_orders BOOLEAN NOT NULL DEFAULT FALSE,
            enable_order_book BOOLEAN NOT NULL DEFAULT FALSE,
            gamma_updated_at TIMESTAMPTZ,
            market_state_observed_at TIMESTAMPTZ,
            migrated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT markets_identity_kind_check CHECK (
                identity_kind IN (
                    'canonical_gamma', 'protocol_structural', 'official_combo',
                    'canonical_clob', 'legacy_placeholder', 'legacy_combo',
                    'legacy_ghost', 'chain_v2', 'invalid_shell'
                )
            ),
            CONSTRAINT markets_canonical_gamma_source_check CHECK (
                identity_kind <> 'canonical_gamma'
                OR NULLIF(btrim(gamma_market_id), '') IS NOT NULL
            ),
            CONSTRAINT markets_gamma_source_kind_check CHECK (
                NULLIF(btrim(gamma_market_id), '') IS NULL
                OR identity_kind IN ('canonical_gamma', 'canonical_clob')
            )
        )
        """
    )
    present = {
        str(row["column_name"] if isinstance(row, Mapping) else row[0])
        for row in conn.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='core' AND table_name='markets'
              AND column_name IN (
                'active', 'closed', 'accepting_orders', 'enable_order_book',
                'gamma_updated_at', 'market_state_observed_at'
              )
            """
        ).fetchall()
    }
    definitions = {
        "active": "BOOLEAN NOT NULL DEFAULT FALSE",
        "closed": "BOOLEAN NOT NULL DEFAULT TRUE",
        "accepting_orders": "BOOLEAN NOT NULL DEFAULT FALSE",
        "enable_order_book": "BOOLEAN NOT NULL DEFAULT FALSE",
        "gamma_updated_at": "TIMESTAMPTZ",
        "market_state_observed_at": "TIMESTAMPTZ",
    }
    missing = [
        f"ADD COLUMN {name} {definition}"
        for name, definition in definitions.items()
        if name not in present
    ]
    if missing:
        conn.execute("ALTER TABLE core.markets " + ", ".join(missing))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS core.market_tokens (
            id BIGINT PRIMARY KEY, market_id BIGINT NOT NULL REFERENCES core.markets(id),
            condition_id TEXT NOT NULL, token_id TEXT NOT NULL UNIQUE, outcome TEXT NOT NULL,
            outcome_index INTEGER NOT NULL, active BOOLEAN NOT NULL DEFAULT TRUE,
            end_date TIMESTAMPTZ, raw_end_date TEXT, created_at TIMESTAMPTZ,
            raw_created_at TEXT, updated_at TIMESTAMPTZ, raw_updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.market_acquisition_failures (
            failure_key TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            lane TEXT NOT NULL,
            kind TEXT NOT NULL,
            reason TEXT NOT NULL,
            source_identity TEXT NOT NULL DEFAULT '',
            condition_id TEXT NOT NULL DEFAULT '',
            token_id TEXT NOT NULL DEFAULT '',
            raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            detail JSONB NOT NULL DEFAULT '{}'::jsonb,
            status TEXT NOT NULL DEFAULT 'open',
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            occurrences BIGINT NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.market_canonical_promotions (
            promotion_key TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            lane TEXT NOT NULL,
            kind TEXT NOT NULL,
            gamma_market_id TEXT NOT NULL,
            token_id TEXT NOT NULL DEFAULT '',
            from_market_id BIGINT NOT NULL,
            from_condition_id TEXT NOT NULL,
            from_identity_kind TEXT NOT NULL,
            to_market_id BIGINT NOT NULL REFERENCES core.markets(id),
            to_condition_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            detail JSONB NOT NULL DEFAULT '{}'::jsonb,
            status TEXT NOT NULL DEFAULT 'promoted',
            first_promoted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            occurrences BIGINT NOT NULL DEFAULT 1
        )
        """
    )


def _failure(
    *,
    mode: str,
    lane: str,
    kind: str,
    reason: str,
    source_identity: str = "",
    condition_id: str = "",
    token_id: str = "",
    raw: Any = None,
    detail: Any = None,
) -> dict[str, str]:
    stable_identity = [kind, source_identity, condition_id, token_id]
    # A source ID is the durable acquisition identity.  Upstream payloads and
    # wording can change between scans and must not open a second ledger row.
    # Fall back to scan context only for payloads that expose no identity at all.
    if not any((source_identity, condition_id, token_id)):
        stable_identity.extend((mode, lane, reason))
    identity = json.dumps(stable_identity, ensure_ascii=False, sort_keys=True)
    return {
        "failure_key": hashlib.sha256(identity.encode()).hexdigest(),
        "mode": mode,
        "lane": lane,
        "kind": kind,
        "reason": reason,
        "source_identity": source_identity,
        "condition_id": condition_id,
        "token_id": token_id,
        "raw_json": json.dumps(
            raw or {}, ensure_ascii=False, sort_keys=True, default=str
        ),
        "detail_json": json.dumps(
            detail or {}, ensure_ascii=False, sort_keys=True, default=str
        ),
    }


def record_failures(conn: Any, failures: Iterable[Mapping[str, Any]]) -> int:
    """Upsert failures without committing; the caller owns the cursor transaction."""
    return execute_values(conn, FAILURE_UPSERT, failures)


def _promotion(
    action: Mapping[str, Any], *, mode: str, lane: str, detail: Any = None
) -> dict[str, Any]:
    identity = json.dumps(
        [
            action["kind"],
            action.get("token_id", ""),
            action["from_market_id"],
            action["from_condition_id"],
            action["to_condition_id"],
            action["gamma_market_id"],
        ],
        sort_keys=True,
    )
    return {
        "promotion_key": hashlib.sha256(identity.encode()).hexdigest(),
        "mode": mode,
        "lane": lane,
        **action,
        "detail_json": json.dumps(
            detail or {}, ensure_ascii=False, sort_keys=True, default=str
        ),
    }


def record_promotions(conn: Any, promotions: Iterable[Mapping[str, Any]]) -> int:
    """Record committed canonical promotions in the caller's transaction."""
    return execute_values(conn, PROMOTION_UPSERT, promotions)


def _value(row: Any, name: str, index: int, default: Any = "") -> Any:
    if isinstance(row, Mapping):
        return row.get(name, default)
    return row[index] if len(row) > index else default


def _replaceable_shell(owner: Mapping[str, Any]) -> bool:
    kind = _text(owner.get("identity_kind")).lower()
    if kind == "protocol_structural":
        return True
    return not _text(owner.get("gamma_market_id")) and (
        _text(owner.get("category")).lower() == "orderfilled-placeholder"
        or _text(owner.get("slug")).lower().startswith("trade-indexer-placeholder-")
    )


def _strict_gamma(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("_normalized_gamma") is True
        and _text(row.get("gamma_market_id"))
        and _text(row.get("condition_id"))
        and _text(row.get("yes_token_id"))
        and _text(row.get("no_token_id"))
        and row.get("yes_token_id") != row.get("no_token_id")
    )


def _token_owner(row: Any) -> dict[str, Any]:
    return {
        "token_id": str(_value(row, "token_id", 0)),
        "market_id": int(_value(row, "market_id", 1, 0) or 0),
        "condition_id": _text(_value(row, "token_condition_id", 2)).lower(),
        "market_condition_id": _text(_value(row, "market_condition_id", 3)).lower(),
        "identity_kind": _text(_value(row, "identity_kind", 4)).lower(),
        "gamma_market_id": _text(_value(row, "gamma_market_id", 5)),
        "category": _text(_value(row, "category", 6)).lower(),
        "slug": _text(_value(row, "slug", 7)).lower(),
    }


def _market_owner(row: Any) -> dict[str, Any]:
    return {
        "market_id": int(_value(row, "market_id", 0, 0) or 0),
        "condition_id": _text(_value(row, "condition_id", 1)).lower(),
        "identity_kind": _text(_value(row, "identity_kind", 2)).lower(),
        "gamma_market_id": _text(_value(row, "gamma_market_id", 3)),
        "category": _text(_value(row, "category", 4)).lower(),
        "slug": _text(_value(row, "slug", 5)).lower(),
    }


def upsert_markets(
    conn: Any,
    markets: Iterable[Mapping[str, Any]],
    promotions: Iterable[Mapping[str, Any]] = (),
) -> int:
    conn = getattr(conn, "_pg_conn", conn)
    rows = list(markets)
    if not rows:
        return 0
    promotion_rows = list(promotions)
    token_promotions = {
        str(item["token_id"]): item
        for item in promotion_rows
        if item["kind"] == "token_rehome"
    }
    market_promotions = {
        str(item["to_condition_id"]).lower(): item
        for item in promotion_rows
        if item["kind"] == "market_identity"
    }
    expected_owner: dict[str, str] = {}
    batch_conflicts: list[dict[str, str]] = []
    for row in rows:
        attempted = str(row["condition_id"]).lower()
        for raw_token in (row["yes_token_id"], row["no_token_id"]):
            token = str(raw_token)
            owner = expected_owner.get(token)
            if owner is not None and owner != attempted:
                batch_conflicts.append(
                    {"token_id": token, "existing": owner, "attempted": attempted}
                )
            expected_owner[token] = attempted
    if batch_conflicts:
        raise TokenOwnershipConflict(
            f"batch assigns {len(batch_conflicts)} token(s) to multiple conditions: "
            f"{json.dumps(batch_conflicts[:5], sort_keys=True)}"
        )
    rows_by_condition = {str(row["condition_id"]).lower(): row for row in rows}
    existing_markets = conn.execute(
        """
        SELECT id AS market_id, condition_id, identity_kind, gamma_market_id,
               category, slug
        FROM core.markets WHERE lower(condition_id) = ANY(%s)
        """,
        (list(rows_by_condition),),
    ).fetchall()
    condition_conflicts = []
    for raw_owner in existing_markets:
        owner = _market_owner(raw_owner)
        attempted = rows_by_condition[owner["condition_id"]]
        if owner["identity_kind"] == "canonical_gamma" and owner[
            "gamma_market_id"
        ] == _text(attempted["gamma_market_id"]):
            continue
        promotion = market_promotions.get(owner["condition_id"])
        if (
            promotion is not None
            and _strict_gamma(attempted)
            and _replaceable_shell(owner)
            and int(promotion["from_market_id"]) == owner["market_id"]
            and _text(promotion["from_condition_id"]).lower() == owner["condition_id"]
        ):
            continue
        condition_conflicts.append(
            {
                "condition_id": owner["condition_id"],
                "identity_kind": owner["identity_kind"] or "unknown",
                "gamma_market_id": owner["gamma_market_id"],
            }
        )
    if condition_conflicts:
        raise TokenOwnershipConflict(
            f"refusing to replace {len(condition_conflicts)} condition owner(s): "
            f"{json.dumps(condition_conflicts[:5], sort_keys=True)}"
        )
    existing_tokens = conn.execute(
        """
        SELECT mt.token_id, mt.market_id,
               mt.condition_id AS token_condition_id,
               m.condition_id AS market_condition_id,
               m.identity_kind, m.gamma_market_id, m.category, m.slug
        FROM core.market_tokens mt
        LEFT JOIN core.markets m ON m.id=mt.market_id
        WHERE mt.token_id = ANY(%s)
        """,
        (list(expected_owner),),
    ).fetchall()
    ownership_conflicts = []
    needed_token_promotions: list[tuple[dict[str, Any], Mapping[str, Any]]] = []
    for raw_owner in existing_tokens:
        owner = _token_owner(raw_owner)
        token = owner["token_id"]
        condition = owner["condition_id"]
        attempted_condition = expected_owner[token]
        if (
            condition != attempted_condition
            or owner["market_condition_id"] != attempted_condition
        ):
            promotion = token_promotions.get(token)
            attempted = rows_by_condition[attempted_condition]
            if (
                promotion is not None
                and _strict_gamma(attempted)
                and _replaceable_shell(owner)
                and int(promotion["from_market_id"]) == owner["market_id"]
                and _text(promotion["from_condition_id"]).lower() == condition
                and _text(promotion["to_condition_id"]).lower() == attempted_condition
            ):
                needed_token_promotions.append((owner, promotion))
                continue
            ownership_conflicts.append(
                {
                    "token_id": token,
                    "existing": condition,
                    "attempted": attempted_condition,
                }
            )
    if ownership_conflicts:
        raise TokenOwnershipConflict(
            f"refusing to rebind {len(ownership_conflicts)} token(s): "
            f"{json.dumps(ownership_conflicts[:5], sort_keys=True)}"
        )
    execute_values(conn, MARKET_UPSERT, rows)
    condition_ids = [row["condition_id"] for row in rows]
    stored = conn.execute(
        "SELECT id, condition_id FROM core.markets WHERE condition_id = ANY(%s)",
        (condition_ids,),
    ).fetchall()
    market_ids = {
        str(row["condition_id"] if isinstance(row, Mapping) else row[1]).lower(): int(
            row["id"] if isinstance(row, Mapping) else row[0]
        )
        for row in stored
    }
    token_rows: list[dict[str, Any]] = []
    for row in rows:
        market_id = market_ids.get(str(row["condition_id"]).lower())
        if market_id is None:
            raise RuntimeError(f"market upsert did not return {row['condition_id']}")
        for side, logical_index, runtime_id in (
            ("YES", 0, -((market_id * 2) - 1)),
            ("NO", 1, -(market_id * 2)),
        ):
            token_rows.append(
                {
                    "id": runtime_id,
                    "market_id": market_id,
                    "condition_id": row["condition_id"],
                    "token_id": row[f"{side.lower()}_token_id"],
                    "outcome": side,
                    "outcome_index": logical_index,
                    # Registry ownership survives market close; trading status
                    # must not disable historical OrderFilled token mapping.
                    "active": True,
                    "end_date": row["end_date"],
                    "raw_end_date": row["raw_end_date"],
                    "created_at": row["created_at"],
                    "raw_created_at": row["raw_created_at"],
                    "raw_updated_at": row["updated_at"].isoformat()
                    if row["updated_at"]
                    else "",
                }
            )
    for owner, promotion in needed_token_promotions:
        condition = _text(promotion["to_condition_id"]).lower()
        expected = next(
            row
            for row in token_rows
            if row["token_id"] == owner["token_id"]
            and str(row["condition_id"]).lower() == condition
        )
        promoted = conn.execute(
            TOKEN_PROMOTE,
            {
                "token_id": owner["token_id"],
                "from_market_id": owner["market_id"],
                "from_condition_id": owner["condition_id"],
                "to_market_id": expected["market_id"],
                "to_condition_id": expected["condition_id"],
                "outcome": expected["outcome"],
                "outcome_index": expected["outcome_index"],
            },
        ).fetchone()
        if promoted is None:
            raise TokenOwnershipConflict(
                f"token promotion compare-and-swap failed for {owner['token_id']}"
            )
    execute_values(conn, TOKEN_UPDATE, token_rows)
    execute_values(conn, TOKEN_INSERT, token_rows)
    actual_tokens = conn.execute(
        """
        SELECT token_id, market_id, condition_id, outcome, outcome_index
        FROM core.market_tokens WHERE token_id = ANY(%s)
        """,
        ([row["token_id"] for row in token_rows],),
    ).fetchall()
    actual_by_token = {
        str(row["token_id"] if isinstance(row, Mapping) else row[0]): row
        for row in actual_tokens
    }
    incomplete = []
    for expected in token_rows:
        actual = actual_by_token.get(expected["token_id"])
        if actual is None:
            incomplete.append(expected["token_id"])
            continue
        get = (
            (lambda name, index: actual[name])
            if isinstance(actual, Mapping)
            else (lambda name, index: actual[index])
        )
        if (
            int(get("market_id", 1)) != expected["market_id"]
            or str(get("condition_id", 2)).lower()
            != str(expected["condition_id"]).lower()
            or str(get("outcome", 3)) != expected["outcome"]
            or int(get("outcome_index", 4)) != expected["outcome_index"]
        ):
            incomplete.append(expected["token_id"])
    if incomplete:
        raise TokenOwnershipConflict(
            f"token pair write was not exact for {len(incomplete)} token(s): {incomplete[:5]}"
        )
    return len(rows)


def filter_token_ownership_conflicts(
    conn: Any, markets: Iterable[Mapping[str, Any]]
) -> tuple[
    list[Mapping[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Partition canonical rows into accepted, blocked, and safe promotions."""

    rows = list(markets)
    rows_by_condition = {str(row["condition_id"]).lower(): row for row in rows}
    attempted_by_token: dict[str, set[str]] = {}
    for row in rows:
        condition = str(row["condition_id"]).lower()
        for token in (str(row["yes_token_id"]), str(row["no_token_id"])):
            attempted_by_token.setdefault(token, set()).add(condition)
    if not attempted_by_token:
        return rows, [], []

    existing_markets = conn.execute(
        """
        SELECT id AS market_id, condition_id, identity_kind, gamma_market_id,
               category, slug
        FROM core.markets WHERE lower(condition_id) = ANY(%s)
        """,
        (list(rows_by_condition),),
    ).fetchall()
    existing_tokens = conn.execute(
        """
        SELECT mt.token_id, mt.market_id,
               mt.condition_id AS token_condition_id,
               m.condition_id AS market_condition_id,
               m.identity_kind, m.gamma_market_id, m.category, m.slug
        FROM core.market_tokens mt
        LEFT JOIN core.markets m ON m.id=mt.market_id
        WHERE mt.token_id = ANY(%s)
        """,
        (list(attempted_by_token),),
    ).fetchall()
    existing_by_token = {
        owner["token_id"]: owner
        for item in existing_tokens
        if (owner := _token_owner(item))
    }
    evidence: list[dict[str, Any]] = []
    promotions: list[dict[str, Any]] = []
    rejected_conditions: set[str] = set()

    for raw_owner in existing_markets:
        owner = _market_owner(raw_owner)
        attempted = rows_by_condition[owner["condition_id"]]
        if owner["identity_kind"] == "canonical_gamma" and owner[
            "gamma_market_id"
        ] == _text(attempted["gamma_market_id"]):
            continue
        if _strict_gamma(attempted) and _replaceable_shell(owner):
            promotions.append(
                {
                    "kind": "market_identity",
                    "gamma_market_id": attempted["gamma_market_id"],
                    "token_id": "",
                    "from_market_id": owner["market_id"],
                    "from_condition_id": owner["condition_id"],
                    "from_identity_kind": owner["identity_kind"]
                    or "legacy_placeholder",
                    "to_condition_id": owner["condition_id"],
                    "reason": "strict Gamma identity supersedes a local shell",
                }
            )
            continue
        rejected_conditions.add(owner["condition_id"])
        evidence.append(
            {
                "kind": "condition_identity_conflict",
                "token_id": "",
                "existing": owner["condition_id"],
                "attempted": owner["condition_id"],
                "existing_market_id": owner["market_id"],
                "existing_identity_kind": owner["identity_kind"] or "unknown",
                "existing_gamma_market_id": owner["gamma_market_id"],
            }
        )

    for token, attempted_conditions in attempted_by_token.items():
        owner = existing_by_token.get(token)
        existing_condition = owner["condition_id"] if owner else None
        existing_market_condition = owner["market_condition_id"] if owner else None
        if len(attempted_conditions) > 1:
            rejected = (
                attempted_conditions - {existing_condition}
                if existing_condition in attempted_conditions
                else attempted_conditions
            )
            rejected_conditions.update(rejected)
            for attempted in sorted(rejected):
                evidence.append(
                    {
                        "kind": "token_ownership_conflict",
                        "token_id": token,
                        "existing": existing_condition or "batch",
                        "attempted": attempted,
                    }
                )
            continue
        attempted = next(iter(attempted_conditions))
        if owner is not None and (
            existing_condition != attempted or existing_market_condition != attempted
        ):
            row = rows_by_condition[attempted]
            if _strict_gamma(row) and owner is not None and _replaceable_shell(owner):
                promotions.append(
                    {
                        "kind": "token_rehome",
                        "gamma_market_id": row["gamma_market_id"],
                        "token_id": token,
                        "from_market_id": owner["market_id"],
                        "from_condition_id": existing_condition,
                        "from_identity_kind": owner["identity_kind"]
                        or "legacy_placeholder",
                        "to_condition_id": attempted,
                        "reason": "strict Gamma token supersedes a local shell owner",
                    }
                )
                continue
            rejected_conditions.add(attempted)
            evidence.append(
                {
                    "kind": "token_ownership_conflict",
                    "token_id": token,
                    "existing": existing_market_condition or existing_condition,
                    "attempted": attempted,
                    "existing_market_id": owner["market_id"] if owner else 0,
                    "existing_identity_kind": (
                        owner["identity_kind"] or "unknown" if owner else "unknown"
                    ),
                }
            )
    return (
        [
            row
            for row in rows
            if str(row["condition_id"]).lower() not in rejected_conditions
        ],
        evidence,
        [
            item
            for item in promotions
            if str(item["to_condition_id"]).lower() not in rejected_conditions
        ],
    )


def _state(conn: Any, key: str) -> dict[str, Any]:
    row = get_state(conn, key)
    if not row or not row.get("value"):
        return {}
    try:
        value = json.loads(row["value"])
    except (TypeError, json.JSONDecodeError):
        return {"watermark": str(row["value"])}
    return value if isinstance(value, dict) else {}


def _page(
    session: requests.Session,
    source: str,
    cursor: str | None,
    query: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    params: dict[str, Any] = {
        "limit": PAGE_SIZE,
        "order": "createdAt",
        "ascending": "false",
        "include_tag": "true",
    }
    params.update(query or {})
    if cursor:
        params["after_cursor"] = cursor
    response = session.get(
        f"{GAMMA_API_BASE}/{source}/keyset", params=params, timeout=60
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, Mapping) or not isinstance(payload.get(source), list):
        raise RuntimeError(f"Gamma /{source}/keyset returned an invalid payload")
    next_cursor = payload.get("next_cursor") or payload.get("nextCursor")
    return payload[source], _text(next_cursor) or None


def _root_created(row: Mapping[str, Any]) -> datetime | None:
    return _timestamp(row.get("createdAt") or row.get("created_at"))


def _root_updated(row: Mapping[str, Any]) -> datetime | None:
    return _timestamp(row.get("updatedAt") or row.get("updated_at"))


def _raw_markets(
    source: str, roots: list[dict[str, Any]]
) -> Iterable[tuple[Mapping[str, Any], Mapping[str, Any] | None]]:
    if source == "markets":
        for market in roots:
            if isinstance(market, Mapping):
                yield market, None
        return
    for event in roots:
        if not isinstance(event, Mapping):
            continue
        for market in event.get("markets") or []:
            if isinstance(market, Mapping):
                yield market, event


def _scan(
    session: requests.Session,
    source: str,
    *,
    mode: str,
    max_pages: int,
    state: Mapping[str, Any],
    since: datetime,
    overlap: timedelta,
    query: Mapping[str, Any] | None = None,
    lane: str | None = None,
) -> tuple[
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
    list[dict[str, str]],
]:
    resume = bool(state.get("cursor")) and not bool(state.get("complete"))
    cursor = _text(state.get("cursor")) or None if resume else None
    base = _timestamp(state.get("base_watermark") or state.get("watermark")) or since
    scan_high = _timestamp(state.get("scan_high")) or base
    cutoff = base - overlap
    accepted: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, str]] = []
    rejected = raw_count = pages = 0
    complete = False

    while pages < max_pages:
        page_query = dict(query or {})
        roots, next_cursor = _page(session, source, cursor, page_query)
        pages += 1
        if not roots:
            complete = True
            cursor = None
            break
        root_times = [
            (_root_updated(row) if mode == "revisit" else _root_created(row))
            for row in roots
        ]
        known_times = [value for value in root_times if value is not None]
        if known_times:
            scan_high = max(scan_high, max(known_times))
        selected = roots
        boundary = False
        if mode in {"sync", "revisit"}:
            selected = [
                row
                for row, created in zip(roots, root_times)
                if created is not None and created >= cutoff
            ]
            boundary = len(known_times) == len(roots) and min(known_times) < cutoff
        for raw, event in _raw_markets(source, selected):
            raw_count += 1
            try:
                market = normalize_market(raw, event)
            except ValueError as exc:
                rejected += 1
                raw_tokens = raw.get("clobTokenIds") or raw.get("clob_token_ids")
                try:
                    tokens = [_text(value) for value in _sequence(raw_tokens, "tokens")]
                except ValueError:
                    tokens = []
                failures.append(
                    _failure(
                        mode=mode,
                        lane=lane or source,
                        kind="normalize_error",
                        reason=str(exc),
                        source_identity=_text(
                            raw.get("id")
                            or raw.get("gamma_market_id")
                            or (event or {}).get("id")
                        ),
                        condition_id=_text(
                            raw.get("conditionId") or raw.get("condition_id")
                        ).lower(),
                        token_id=tokens[0] if len(tokens) == 1 else "",
                        raw=dict(raw),
                        detail={
                            "source": source,
                            "event_id": _text((event or {}).get("id")),
                            "token_ids": tokens,
                        },
                    )
                )
                continue
            accepted[market["condition_id"]] = market
        cursor = next_cursor
        if boundary or not cursor:
            complete = True
            cursor = None
            break

    now = datetime.now(timezone.utc).isoformat()
    if mode in {"sync", "revisit"}:
        next_state = (
            {"watermark": scan_high.isoformat(), "complete": True, "updated_at": now}
            if complete
            else {
                "watermark": base.isoformat(),
                "base_watermark": base.isoformat(),
                "scan_high": scan_high.isoformat(),
                "cursor": cursor,
                "complete": False,
                "updated_at": now,
            }
        )
    else:
        next_state = {"cursor": cursor, "complete": complete, "updated_at": now}
    summary = {
        "source": lane or source,
        "pages": pages,
        "raw_markets": raw_count,
        "canonical": len(accepted),
        "rejected": rejected,
        "complete": complete,
        "next_cursor": cursor,
    }
    return list(accepted.values()), summary, next_state, failures


def run_once(
    *,
    mode: str = "sync",
    max_pages: int = 10,
    dry_run: bool = False,
    since: datetime | None = None,
    overlap_minutes: int = 10,
    session: requests.Session | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    if mode not in {"sync", "backfill", "revisit"}:
        raise ValueError("mode must be sync, backfill, or revisit")
    if max_pages < 1:
        raise ValueError("max_pages must be at least 1")
    since = since or (datetime.now(timezone.utc) - timedelta(hours=24))
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    session = session or _http_session()
    own_conn = conn is None and not dry_run
    if own_conn:
        conn = connect_postgres()
    if mode == "revisit":
        specs = (
            (
                "open",
                "events",
                {"order": "updatedAt", "ascending": "false", "closed": "false"},
            ),
            (
                "closed",
                "events",
                {"order": "updatedAt", "ascending": "false", "closed": "true"},
            ),
        )
        state_template = REVISIT_STATE
    else:
        specs = tuple((source, source, {}) for source in SOURCES)
        state_template = CREATED_STATE if mode == "sync" else BACKFILL_STATE
    states: dict[str, dict[str, Any]] = {}
    try:
        for lane, _source, _query in specs:
            key = state_template.format(source=lane)
            states[lane] = _state(conn, key) if conn is not None else {}
        all_markets: dict[str, dict[str, Any]] = {}
        market_lanes: dict[str, set[str]] = {}
        failures: list[dict[str, str]] = []
        lane_summaries: list[dict[str, Any]] = []
        next_states: dict[str, dict[str, Any]] = {}
        resuming_revisit = mode == "revisit" and any(
            state and not bool(state.get("complete")) for state in states.values()
        )
        for lane, source, query in specs:
            skip_completed = bool(states[lane].get("complete")) and (
                mode == "backfill" or resuming_revisit
            )
            if skip_completed:
                # A historical keyset lane is immutable once its terminal
                # boundary has been reached.  Starting it again from the head
                # can overwrite a valid terminal receipt with a partial scan;
                # current additions are owned by sync/revisit instead.  During
                # a revisit continuation, also avoid redoing a sibling lane
                # that already completed this same cycle.
                next_states[lane] = dict(states[lane])
                lane_summaries.append(
                    {
                        "source": lane,
                        "pages": 0,
                        "raw_markets": 0,
                        "canonical": 0,
                        "rejected": 0,
                        "complete": True,
                        "next_cursor": None,
                    }
                )
                continue
            markets, summary, next_state, scan_failures = _scan(
                session,
                source,
                mode=mode,
                max_pages=max_pages,
                state=states[lane],
                since=since,
                overlap=timedelta(minutes=max(0, overlap_minutes)),
                query=query,
                lane=lane,
            )
            for row in markets:
                condition = str(row["condition_id"]).lower()
                all_markets[condition] = row
                market_lanes.setdefault(condition, set()).add(lane)
            failures.extend(scan_failures)
            lane_summaries.append(summary)
            next_states[lane] = next_state

        persisted = 0
        failures_ledgered = 0
        promotions_ledgered = 0
        ownership_conflicts: list[dict[str, Any]] = []
        if not dry_run:
            assert conn is not None
            ensure_schema(conn)
            persist_rows: list[Mapping[str, Any]] = list(all_markets.values())
            persist_rows, ownership_conflicts, promotion_actions = (
                filter_token_ownership_conflicts(conn, persist_rows)
            )
            for conflict in ownership_conflicts:
                condition = str(conflict["attempted"]).lower()
                row = all_markets.get(condition, {})
                for conflict_lane in market_lanes.get(condition, {"unknown"}):
                    failures.append(
                        _failure(
                            mode=mode,
                            lane=conflict_lane,
                            kind=str(
                                conflict.get("kind") or "token_ownership_conflict"
                            ),
                            reason="canonical identity conflicts with a non-replaceable owner",
                            source_identity=_text(row.get("gamma_market_id")),
                            condition_id=condition,
                            token_id=str(conflict["token_id"]),
                            raw=row,
                            detail=dict(conflict),
                        )
                    )
            if mode == "sync" and ownership_conflicts:
                failures_ledgered = record_failures(conn, failures)
                conn.commit()
                raise TokenOwnershipConflict(
                    f"live canonical promotion blocked by {len(ownership_conflicts)} "
                    "non-replaceable owner(s)"
                )
            promotion_records = []
            for action in promotion_actions:
                condition = str(action["to_condition_id"]).lower()
                row = all_markets[condition]
                lanes = sorted(market_lanes.get(condition, {"unknown"}))
                promotion_records.append(
                    _promotion(
                        action,
                        mode=mode,
                        lane=lanes[0],
                        detail={
                            "observed_lanes": lanes,
                            "yes_token_id": row["yes_token_id"],
                            "no_token_id": row["no_token_id"],
                        },
                    )
                )
            persisted = upsert_markets(conn, persist_rows, promotion_actions)
            promotions_ledgered = record_promotions(conn, promotion_records)
            failures_ledgered = record_failures(conn, failures)
            for lane, _source, _query in specs:
                key = state_template.format(source=lane)
                set_state(conn, key, value=next_states[lane], monotonic_block=False)
            if mode == "sync" and all(item["complete"] for item in lane_summaries):
                watermarks = [
                    value
                    for state in next_states.values()
                    if (value := _timestamp(state.get("watermark"))) is not None
                ]
                legacy = _state(conn, LEGACY_DISCOVERY_STATE)
                legacy_watermark = (
                    _timestamp(legacy.get("watermark")) if legacy else None
                )
                if legacy_watermark is not None:
                    watermarks.append(legacy_watermark)
                if watermarks:
                    set_state(
                        conn,
                        LEGACY_DISCOVERY_STATE,
                        value=max(watermarks).isoformat(),
                        monotonic_block=False,
                    )
            conn.commit()
        result: dict[str, Any] = {
            "mode": mode,
            "dry_run": dry_run,
            "canonical": len(all_markets),
            "persisted": persisted,
            "rejected": sum(item["rejected"] for item in lane_summaries),
            "ownership_conflicts": len(ownership_conflicts),
            "failures_ledgered": failures_ledgered,
            "canonical_promotions": promotions_ledgered,
            "lanes": lane_summaries,
        }
        if ownership_conflicts:
            result["ownership_conflict_sample"] = ownership_conflicts[:5]
        if dry_run:
            result["sample"] = [
                {
                    key: row[key]
                    for key in (
                        "gamma_market_id",
                        "condition_id",
                        "slug",
                        "yes_token_id",
                        "no_token_id",
                        "outcomes",
                        "outcome_prices",
                        "category",
                        "tags",
                    )
                }
                for row in list(all_markets.values())[:3]
            ]
        return result
    except Exception:
        if conn is not None and not dry_run:
            conn.rollback()
        raise
    finally:
        if own_conn and conn is not None:
            conn.close()


def _parse_since(value: str | None) -> datetime | None:
    return _timestamp(value) if value else None


_IDENTITY_KINDS = (
    "legacy_placeholder",
    "legacy_combo",
    "legacy_ghost",
    "chain_v2",
    "invalid_shell",
)


def _identity_counts(row: Any) -> dict[str, int]:
    names = ("total", *_IDENTITY_KINDS, "unclassified", "overlapping")
    return {
        name: int(_value(row, name, index, 0) or 0) for index, name in enumerate(names)
    }


def _gamma_kind_counts(row: Any) -> dict[str, int]:
    return {
        "noncanonical_with_gamma": int(
            _value(row, "noncanonical_with_gamma", 0, 0) or 0
        ),
    }


def reclassify_legacy_identities(
    *, apply: bool = False, conn: Any | None = None
) -> dict[str, Any]:
    """Truthfully relabel old no-Gamma shells; never infer official identity."""
    own_conn = conn is None
    if own_conn:
        conn = connect_postgres()
    assert conn is not None
    try:
        if apply:
            # Both constraints are transactional. NOT VALID permits the CAS
            # repair; validation below remains the commit gate.
            conn.execute(IDENTITY_CONSTRAINTS_SQL)
        before = _identity_counts(conn.execute(IDENTITY_SCAN_SQL).fetchone())
        gamma_before = _gamma_kind_counts(conn.execute(GAMMA_KIND_SCAN_SQL).fetchone())
        if before["unclassified"] or before["overlapping"]:
            raise RuntimeError(
                "identity classification is not exhaustive and disjoint: "
                f"unclassified={before['unclassified']} "
                f"overlapping={before['overlapping']}"
            )
        if gamma_before["noncanonical_with_gamma"]:
            raise RuntimeError(
                "unverified noncanonical rows carry Gamma identity: "
                f"{gamma_before['noncanonical_with_gamma']}"
            )

        updated = {name: 0 for name in ("total", *_IDENTITY_KINDS)}
        if apply:
            updated = _identity_counts(conn.execute(IDENTITY_APPLY_SQL).fetchone())
            if updated["total"] != before["total"]:
                raise RuntimeError(
                    "legacy identity CAS did not update its complete source set: "
                    f"expected={before['total']} updated={updated['total']}"
                )
            conn.execute(IDENTITY_VALIDATE_SQL)

        result = {
            "schema": "market-identity-reclassification-v1",
            "mode": "apply" if apply else "dry-run",
            "source": before,
            "noncanonical_with_gamma": gamma_before["noncanonical_with_gamma"],
            "updated": updated,
            "remaining_canonical_without_gamma": 0 if apply else before["total"],
            "constraints_validated": apply,
        }
        if apply:
            set_state(
                conn,
                IDENTITY_RECLASSIFICATION_STATE,
                value=result,
                monotonic_block=False,
            )
            conn.commit()
        return result
    except Exception:
        if apply:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def _handle_identity_classification(args: argparse.Namespace) -> int:
    result = reclassify_legacy_identities(apply=args.apply)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


def _handle(args: argparse.Namespace) -> int:
    def collect() -> dict[str, Any]:
        result = run_once(
            mode=args.market_mode,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            since=_parse_since(getattr(args, "since", None)),
            overlap_minutes=args.overlap_minutes,
        )
        print(
            json.dumps(result, ensure_ascii=False, sort_keys=True, default=str),
            flush=True,
        )
        return result

    if args.watch:
        while True:
            try:
                result = collect()
                incomplete = any(not lane["complete"] for lane in result["lanes"])
                time.sleep(args.catchup_interval if incomplete else args.interval)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"collector error: {type(exc).__name__}: {exc}", flush=True)
                time.sleep(args.error_interval)
    else:
        collect()
    return 0


def add_cli(parser: argparse.ArgumentParser) -> None:
    modes = parser.add_subparsers(dest="market_mode", required=True)
    for name, help_text in (
        ("sync", "follow newly created canonical Gamma markets"),
        ("backfill", "bounded resumable full keyset revisit"),
        ("revisit", "find new markets appended to older open and closed events"),
    ):
        command = modes.add_parser(name, help=help_text)
        command.add_argument("--max-pages", type=int, default=10)
        command.add_argument("--dry-run", action="store_true")
        command.add_argument("--watch", action="store_true")
        command.add_argument("--interval", type=float, default=60.0)
        command.add_argument("--catchup-interval", type=float, default=5.0)
        command.add_argument("--error-interval", type=float, default=300.0)
        command.add_argument("--overlap-minutes", type=int, default=10)
        if name in {"sync", "revisit"}:
            command.add_argument(
                "--since", help="initial UTC ISO watermark; default is 24h ago"
            )
        command.set_defaults(handler=_handle)
    classify = modes.add_parser(
        "classify-identities",
        help="dry-run or apply truthful legacy shell identity labels",
    )
    classify.add_argument(
        "--apply",
        action="store_true",
        help="transactionally apply and validate; default is read-only dry-run",
    )
    classify.set_defaults(handler=_handle_identity_classification)
