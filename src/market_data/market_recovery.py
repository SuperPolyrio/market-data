"""Retry a bounded, oldest-first slice of Gamma normalization failures."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

from market_data import market


PENDING_SQL = """
SELECT source_identity, jsonb_object_agg(failure_key, occurrences) AS failure_versions
FROM ops.market_acquisition_failures
WHERE status='open' AND kind='normalize_error' AND source_identity <> ''
GROUP BY source_identity
ORDER BY min(COALESCE(
    NULLIF(detail->>'recovery_checked_at', '')::timestamptz, first_seen_at
)), source_identity
LIMIT %s
"""

REMAINING_SQL = """
SELECT source_identity, jsonb_object_agg(failure_key, occurrences) AS failure_versions
FROM ops.market_acquisition_failures
WHERE status='open' AND kind='normalize_error' AND source_identity = ANY(%s)
GROUP BY source_identity
"""

RECEIPT_SQL = """
UPDATE ops.market_acquisition_failures AS failure
SET status=%(status)s,
    detail=COALESCE(detail, '{}'::jsonb)
        || jsonb_build_object('recovery_original_raw', COALESCE(
            detail->'recovery_original_raw', raw_json
        )) || %(detail_json)s::jsonb
WHERE status='open' AND kind='normalize_error'
  AND source_identity=%(source_identity)s
  AND failure.occurrences = (
      %(snapshot_json)s::jsonb ->> failure.failure_key
  )::bigint
  AND (%(status)s='open' OR EXISTS (
      SELECT 1 FROM core.markets m
      JOIN core.market_tokens yes_token ON yes_token.token_id=m.yes_token_id
      JOIN core.market_tokens no_token ON no_token.token_id=m.no_token_id
      WHERE m.identity_kind='canonical_gamma'
        AND m.gamma_market_id=%(source_identity)s
        AND lower(m.condition_id)=%(condition_id)s
        AND m.yes_token_id=%(yes_token_id)s AND m.no_token_id=%(no_token_id)s
        AND yes_token.market_id=m.id AND no_token.market_id=m.id
        AND lower(yes_token.condition_id)=lower(m.condition_id)
        AND lower(no_token.condition_id)=lower(m.condition_id)
        AND yes_token.outcome='YES' AND yes_token.outcome_index=0
        AND no_token.outcome='NO' AND no_token.outcome_index=1
  ))
RETURNING failure_key
"""


def retry_normalization_failures(
    limit: int = 200,
    dry_run: bool = False,
    session: requests.Session | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Resolve only freshly verified Gamma identities and their exact token pairs.

    A supplied connection owns its transaction. Dry runs read the existing
    ledger and registry, but perform no schema changes, writes, or commits.
    """
    if limit < 1:
        raise ValueError("limit must be at least 1")
    own_conn = conn is None
    own_session = session is None
    conn = conn if conn is not None else market.connect_postgres()
    session = session if session is not None else market._http_session()
    results: dict[str, dict[str, Any]] = {}
    snapshots: dict[str, dict[str, int]] = {}
    normalized: list[Mapping[str, Any]] = []
    try:
        pending = conn.execute(PENDING_SQL, (limit,)).fetchall()
        for item in pending:
            identity = str(market._value(item, "source_identity", 0))
            snapshots[identity] = dict(market._value(item, "failure_versions", 1))
            receipt: dict[str, Any] = {
                "source_identity": identity,
                "recovery_checked_at": datetime.now(timezone.utc).isoformat(),
                "recovery_source": f"{market.GAMMA_API_BASE}/markets/{quote(identity, safe='')}",
                "recovery_status": "request_error",
                "recovery_latest_raw": None,
            }
            results[identity] = receipt
            try:
                response = session.get(receipt["recovery_source"], timeout=60)
                receipt["recovery_http_status"] = response.status_code
                try:
                    raw = response.json()
                except ValueError:
                    raw = None
                receipt["recovery_latest_raw"] = raw
                if response.status_code != 200:
                    receipt["recovery_status"] = "http_error"
                    receipt["recovery_reason"] = (
                        f"Gamma returned HTTP {response.status_code}; identity remains unverified"
                    )
                    continue
                if not isinstance(raw, Mapping):
                    raise ValueError("Gamma market response must be an object")
                if str(raw.get("id") or "") != identity:
                    raise ValueError("Gamma response id does not match requested market id")
                row = market.normalize_market(raw)
            except requests.RequestException as exc:
                receipt["recovery_reason"] = type(exc).__name__
                continue
            except ValueError as exc:
                receipt["recovery_status"] = "normalize_error"
                receipt["recovery_reason"] = str(exc)
                continue
            normalized.append(row)
            receipt["recovery_status"] = "canonical"
            receipt["recovery_reason"] = "official Gamma market normalized successfully"

        accepted, conflicts, promotions = market.filter_token_ownership_conflicts(
            conn, normalized
        )
        accepted_by_id = {str(row["gamma_market_id"]): row for row in accepted}
        for row in normalized:
            identity = str(row["gamma_market_id"])
            if identity not in accepted_by_id:
                results[identity].update(
                    recovery_status="ownership_conflict",
                    recovery_reason="canonical identity conflicts with a non-replaceable owner",
                    recovery_conflicts=[
                        conflict for conflict in conflicts
                        if conflict["attempted"] == row["condition_id"]
                    ],
                )

        persisted = resolved = resolved_identities = promotions_ledgered = 0
        resolved_sources: set[str] = set()
        receipt_params: dict[str, dict[str, Any]] = {}
        if not dry_run:
            persisted = market.upsert_markets(conn, accepted, promotions)
            if persisted != len(accepted):
                raise RuntimeError("recovery market persistence count was incomplete")
            promotions_ledgered = market.record_promotions(
                conn,
                [market._promotion(
                    action, mode="recovery", lane="normalize_error",
                    detail={"source": "official Gamma market by id"},
                ) for action in promotions],
            )
            for identity, receipt in results.items():
                row = accepted_by_id.get(identity, {})
                params = {
                    "source_identity": identity,
                    "snapshot_json": json.dumps(snapshots[identity]),
                    "condition_id": row.get("condition_id", ""),
                    "yes_token_id": row.get("yes_token_id", ""),
                    "no_token_id": row.get("no_token_id", ""),
                    "status": "resolved" if row else "open",
                }
                if row:
                    receipt.update(
                        recovery_status="resolved",
                        recovery_reason="official Gamma identity and exact token registry persisted",
                        recovery_resolved_at=datetime.now(timezone.utc).isoformat(),
                        recovery_condition_id=row["condition_id"],
                    )
                params["detail_json"] = json.dumps(receipt, ensure_ascii=False)
                receipt_params[identity] = params
                written = conn.execute(RECEIPT_SQL, params).fetchall()
                if row and written:
                    resolved += len(written)
                    resolved_sources.add(identity)

        remaining = {
            str(market._value(item, "source_identity", 0)):
                dict(market._value(item, "failure_versions", 1))
            for item in conn.execute(REMAINING_SQL, (list(results),)).fetchall()
        }
        for identity, receipt in results.items():
            current_versions = remaining.get(identity, {})
            changed = any(
                snapshots[identity].get(key) != version
                for key, version in current_versions.items()
            )
            if changed or (not dry_run and current_versions and identity in accepted_by_id):
                receipt.pop("recovery_resolved_at", None)
                if changed:
                    receipt.update(
                        recovery_status="concurrent_reobservation",
                        recovery_reason="new or changed failure generations remain open for a fresh retry",
                    )
                else:
                    receipt.update(
                        recovery_status="registry_unverified",
                        recovery_reason="exact persisted Gamma identity and token pair were not found",
                    )
                if not dry_run:
                    params = receipt_params[identity]
                    params.update(
                        status="open", detail_json=json.dumps(receipt, ensure_ascii=False)
                    )
                    conn.execute(RECEIPT_SQL, params)
        resolved_identities = len(resolved_sources - remaining.keys())
        if not dry_run and own_conn:
            conn.commit()
        return {
            "selected": len(results),
            "canonical": len(normalized),
            "persisted": persisted,
            "resolved": resolved,
            "resolved_identities": resolved_identities,
            "remaining_open_selected": len(remaining),
            "promotions_ledgered": promotions_ledgered,
            "dry_run": dry_run,
            "results": [
                {key: value for key, value in receipt.items() if key != "recovery_latest_raw"}
                for receipt in results.values()
            ],
        }
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()
        if own_session:
            session.close()
