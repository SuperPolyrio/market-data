"""Repair and verify CTF history without advancing either legacy Oracle cursor.

Run ``python -m market_data.oracle_repair --help``. Read-only is the default;
``--apply`` commits each verified window and its independent resume receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Sequence

from market_data import oracle
from market_data.config import polygon_rpc_urls
from market_data.rpc import RpcClient
from market_data.storage import connect_postgres, set_state


CURSOR_KEY = "oracle_ctf_backfill_v2"
# Polymarket/polymarket-subgraph, networks.yaml, matic.ConditionalTokens.
MANIFEST_COMMIT = "7a92ba026a9466c07381e0d245a323ba23ee8701"
CTF_START_BLOCK = 4_023_686
CTF_DEPLOYMENT_TX = "0xf822536aff16fdb8df59bc9c0b5854c5bec4b9a76484ea9d6944908ced563389"
FIELDS = (
    "block_number", "event_status", "source_oracle", "source_adapter",
    "condition_id", "question_id", "adapter_question_id", "payout", "string_raw",
)


def _key(row: Mapping[str, Any]) -> tuple[str, int]:
    return str(row["tx_hash"]).lower(), int(row["log_index"])


def _signature(row: Mapping[str, Any]) -> tuple[Any, ...]:
    result: list[Any] = []
    for field in FIELDS:
        value = row.get(field)
        if field == "block_number":
            value = int(value)
        elif field in {"payout", "string_raw"}:
            try:
                value = json.dumps(json.loads(value), sort_keys=True, separators=(",", ":")) if value else ""
            except (TypeError, ValueError):
                # Malformed stored projections are differences to repair.
                value = str(value)
        else:
            value = str(value or "").lower()
        result.append(value)
    return tuple(result)


def _index(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, int], tuple[Any, ...]]:
    indexed = {_key(row): _signature(row) for row in rows}
    if len(indexed) != len(rows):
        raise RuntimeError("duplicate CTF transaction/log identity")
    return indexed


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _read_rows(conn: Any, start: int, end: int) -> list[Mapping[str, Any]]:
    return conn.execute(
        f"""SELECT tx_hash, log_index, event_time, {', '.join(FIELDS)}
            FROM oracle.oracle_events
            WHERE block_number BETWEEN %s AND %s AND lower(source_oracle)=%s""",
        (start, end, oracle.CTF),
    ).fetchall()


def _difference(expected: Mapping, actual: Mapping) -> dict[str, int]:
    return {
        "missing": len(expected.keys() - actual.keys()),
        "extra": len(actual.keys() - expected.keys()),
        "mismatched": sum(expected[key] != actual[key] for key in expected.keys() & actual.keys()),
    }


def _fetch_windows(rpc: RpcClient, start: int, end: int) -> Iterator[tuple[int, int, list]]:
    """Only provider result/range limits justify splitting, never RPC failures."""
    try:
        logs = oracle._fetch(rpc, start, end, (oracle.CTF,), oracle.CTF_BY_TOPIC, split_limits=False)
    except (ConnectionError, RuntimeError) as exc:
        message = str(exc).lower()
        limited = any(part in message for part in (
            "query returned more than", "too many results", "response size exceeded",
            "log response size exceeded", "block range is too", "block range limit",
            "exceeds the block range", "maximum block range", "please limit the query",
        ))
        if not limited or start == end:
            raise
        middle = (start + end) // 2
        yield from _fetch_windows(rpc, start, middle)
        yield from _fetch_windows(rpc, middle + 1, end)
        return
    yield start, end, logs


def repair_window(
    rpc: RpcClient, conn: Any, start: int, end: int,
    *, apply: bool = False, logs: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Stage one window; caller commits its receipt together with these writes."""
    try:
        if logs is None:
            logs = [log for _, _, chunk in _fetch_windows(rpc, start, end) for log in chunk]
        raw_keys = {(oracle._hex(log["transactionHash"]), oracle._int(log["logIndex"])) for log in logs}
        if len(raw_keys) != len(logs):
            raise RuntimeError("duplicate CTF transaction/log identity from RPC")
        if any(not start <= oracle._int(log["blockNumber"]) <= end for log in logs):
            raise RuntimeError("RPC returned CTF event outside requested window")
        if any(str(log.get("address", "")).lower() != oracle.CTF for log in logs):
            raise RuntimeError("RPC returned event from a different CTF contract")
        # Timestamps are fetched only for rows that need repair. The comparison
        # checks chain semantics independently of mutable market enrichment.
        placeholder = datetime(1970, 1, 1, tzinfo=timezone.utc)
        times = {oracle._int(log["blockNumber"]): placeholder for log in logs}
        records = oracle._updown_records(conn, logs, times)
        expected = _index(records)
        if expected.keys() != raw_keys:
            raise RuntimeError("CTF decoder omitted chain events; refusing partial coverage")
        before_rows = _read_rows(conn, start, end)
        actual = _index(before_rows)
        before = _difference(expected, actual)
        missing_times = {_key(row) for row in before_rows if row["event_time"] is None}
        changed_keys = {key for key in expected if expected[key] != actual.get(key)} | (missing_times & expected.keys())
        written = 0
        if apply and changed_keys:
            changed_logs = [log for log in logs if (oracle._hex(log["transactionHash"]), oracle._int(log["logIndex"])) in changed_keys]
            times = oracle._timestamps(rpc, changed_logs)
            changed = [row for row in records if _key(row) in changed_keys]
            for row in changed:
                row["event_time"] = times[row["block_number"]]
            oracle._write_oracle_events(conn, changed)
            written = len(changed)
        after_rows = _read_rows(conn, start, end) if apply else before_rows
        after = _index(after_rows)
        difference = _difference(expected, after)
        after_missing_times = sum(row["event_time"] is None for row in after_rows)
        timestamp_mismatches = sum(
            row["event_time"] != times[row["block_number"]]
            for row in after_rows if _key(row) in changed_keys
        ) if apply else 0
        verified = not any(difference.values()) and not after_missing_times and not timestamp_mismatches
        receipt = {
            "from_block": start, "to_block": end, "chain_events": len(expected),
            "database_events": len(after), "before": before, "after": difference,
            "written": written, "verified": verified,
            "before_missing_timestamps": len(missing_times),
            "after_missing_timestamps": after_missing_times,
            "after_timestamp_mismatches": timestamp_mismatches,
            "events_sha256": _digest(sorted(expected.items())),
        }
        if apply and not verified:
            raise RuntimeError(f"CTF window verification failed: {json.dumps(receipt, sort_keys=True)}")
        return receipt
    except BaseException:
        conn.rollback()
        raise


def _read_state(conn: Any) -> Mapping[str, Any] | None:
    # storage.get_state also creates tables; read-only audits must not run DDL.
    if conn.execute("SELECT to_regclass('ops.sync_state') AS relation").fetchone()["relation"] is None:
        return None
    return conn.execute("SELECT value, last_block FROM ops.sync_state WHERE key=%s", (CURSOR_KEY,)).fetchone()


def _ensure_receipts(conn: Any) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS ops.oracle_ctf_repair_windows (
            cursor_key TEXT NOT NULL,
            from_block BIGINT NOT NULL CHECK (from_block >= 0),
            to_block BIGINT NOT NULL CHECK (to_block >= from_block),
            chain_events BIGINT NOT NULL CHECK (chain_events >= 0),
            database_events BIGINT NOT NULL CHECK (database_events = chain_events),
            events_sha256 TEXT NOT NULL,
            receipt_json JSONB NOT NULL,
            verified_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cursor_key, from_block, to_block)
        )"""
    )


def _write_receipt(conn: Any, receipt: Mapping[str, Any]) -> None:
    if receipt.get("verified") is not True:
        raise RuntimeError("refusing to persist an unverified CTF repair window")
    conn.execute(
        """INSERT INTO ops.oracle_ctf_repair_windows (
            cursor_key, from_block, to_block, chain_events, database_events,
            events_sha256, receipt_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (cursor_key, from_block, to_block) DO UPDATE SET
            chain_events=EXCLUDED.chain_events,
            database_events=EXCLUDED.database_events,
            events_sha256=EXCLUDED.events_sha256,
            receipt_json=EXCLUDED.receipt_json,
            verified_at=CURRENT_TIMESTAMP""",
        (
            CURSOR_KEY, receipt["from_block"], receipt["to_block"],
            receipt["chain_events"], receipt["database_events"],
            receipt["events_sha256"], json.dumps(receipt, sort_keys=True),
        ),
    )


def run_repair(
    rpc: RpcClient, conn: Any, *, from_block: int | None = None,
    to_block: int | None = None, window_blocks: int = 100_000,
    max_windows: int | None = None, apply: bool = False,
) -> dict[str, Any]:
    """Run a contiguous repair; no legacy/live cursor is read or written."""
    if window_blocks < 1 or (max_windows is not None and max_windows < 1):
        raise ValueError("window_blocks and max_windows must be positive")
    if from_block is not None and from_block < 0:
        raise ValueError("from_block must be nonnegative")
    locked = False
    try:
        if oracle._int(rpc.call("eth_chainId")) != oracle.POLYGON_CHAIN_ID:
            raise RuntimeError("CTF repair requires Polygon chain ID 137")
        safe_head = rpc.head() - oracle.CONFIRMATIONS
        target = safe_head if to_block is None else to_block
        if target < 0 or target > safe_head:
            raise ValueError(f"to_block must be between 0 and confirmed head {safe_head}")
        if apply:
            locked = bool(conn.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS locked", (CURSOR_KEY,)).fetchone()["locked"])
            if not locked:
                raise RuntimeError("another CTF historical repair is running")
        state = _read_state(conn) if apply else None
        previous = json.loads(state["value"]) if state else {}
        origin = from_block if from_block is not None else int(previous.get("from_block", CTF_START_BLOCK))
        if previous and (previous.get("version") != 2 or previous.get("from_block") != origin or previous.get("source_oracle") != oracle.CTF):
            raise RuntimeError("CTF repair cursor origin/contract differs; refusing a gap or reset")
        start = int(state["last_block"]) + 1 if state else origin
        if origin > target:
            raise ValueError("from_block exceeds to_block")
        if apply:
            _ensure_receipts(conn)
        stats = {
            "version": 2, "source_oracle": oracle.CTF, "chain_id": oracle.POLYGON_CHAIN_ID,
            "manifest_commit": MANIFEST_COMMIT, "deployment_block": CTF_START_BLOCK,
            "from_block": origin, "target_block": target, "last_block": start - 1,
            "verified_windows": int(previous.get("verified_windows", 0)),
            "verified_events": int(previous.get("verified_events", 0)),
            "written_events": int(previous.get("written_events", 0)),
            "receipt_sha256": previous.get("receipt_sha256", ""),
            "apply": apply, "windows_this_run": 0, "unverified_windows": 0,
        }
        while start <= target and (max_windows is None or stats["windows_this_run"] < max_windows):
            end = min(start + window_blocks - 1, target)
            for window_start, window_end, logs in _fetch_windows(rpc, start, end):
                receipt = repair_window(rpc, conn, window_start, window_end, apply=apply, logs=logs)
                stats["windows_this_run"] += 1
                stats["last_block"] = window_end
                stats["last_window"] = receipt
                stats["verified_windows"] += int(receipt["verified"])
                stats["unverified_windows"] += int(not receipt["verified"])
                stats["verified_events"] += receipt["chain_events"] if receipt["verified"] else 0
                stats["written_events"] += receipt["written"]
                stats["receipt_sha256"] = _digest([stats["receipt_sha256"], receipt])
                stats["complete"] = window_end >= target and not stats["unverified_windows"]
                if apply:
                    _write_receipt(conn, receipt)
                    set_state(conn, CURSOR_KEY, last_block=window_end, value=stats)
                    conn.commit()
                else:
                    conn.rollback()
                print(json.dumps({"cursor": CURSOR_KEY, "apply": apply, **receipt}, sort_keys=True), flush=True)
                start = window_end + 1
                if max_windows is not None and stats["windows_this_run"] >= max_windows:
                    break
        stats["complete"] = stats["last_block"] >= target and not stats["unverified_windows"]
        return stats
    except BaseException:
        conn.rollback()
        raise
    finally:
        if locked:
            conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (CURSOR_KEY,))
            conn.commit()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-block", type=int, help="default: deployment block, or repair cursor origin")
    parser.add_argument("--to-block", type=int, help="default: current confirmed head")
    parser.add_argument("--window-blocks", type=int, default=100_000)
    parser.add_argument("--max-windows", type=int)
    parser.add_argument("--rpc-url", help="optional endpoint for this history job only")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="write repairs and verified independent cursor")
    mode.add_argument("--dry-run", action="store_true", help="read-only comparison (default)")
    mode.add_argument("--audit-only", action="store_true", help="read-only exact chain/database comparison")
    args = parser.parse_args(argv)
    rpc = RpcClient((args.rpc_url,) if args.rpc_url else polygon_rpc_urls("oracle"))
    with connect_postgres() as conn:
        conn.commit()
        conn.read_only = not args.apply
        stats = run_repair(
            rpc, conn, from_block=args.from_block, to_block=args.to_block,
            window_blocks=args.window_blocks, max_windows=args.max_windows, apply=args.apply,
        )
    print(json.dumps(stats, sort_keys=True), flush=True)
    return 1 if stats["unverified_windows"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
