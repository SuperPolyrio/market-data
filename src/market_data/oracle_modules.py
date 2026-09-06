"""Acquire Polygon V2 module lifecycle evidence, independently of market metadata.

ABI sources are exact-match Sourcify implementations of the official proxies.
An ABI source is decoding provenance, not proof of a past proxy implementation.
Upgrade logs are retained so implementation epochs can be audited separately.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Sequence

from eth_abi import encode
from web3 import Web3

from market_data import oracle
from market_data.config import polygon_rpc_urls
from market_data.rpc import RpcClient
from market_data.storage import connect_postgres, set_state


BINARY_MODULE = "0x1000008dd9001b968442c1000017eae6e0da00ba"
NEG_RISK_MODULE = "0x200000900045e3b6259600682756002200028933"
COMBINATORIAL_MODULE = "0x30000034706c7d8e12009dab006be20000c031a8"
CURSOR_KEY = "oracle_modules_backfill_v1"
RESULT_DENOMINATOR = 1_000_000
IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
MODULES = {
    BINARY_MODULE: (1, "0x492fec596ec347459e1ebe30b9245eb3b49b1bba", "609a6df4ab6845681020bb7ee0fc546c220ee43ef10b0a96e364260cc674b406"),
    NEG_RISK_MODULE: (2, "0xa61e7ca374f721d5b9fd5b0fee6fb90f27d448d7", "57eeb7b6cc280832f4d066d2d91fc01cae002ea997ae3ab9eeaa3b98c1614252"),
    COMBINATORIAL_MODULE: (3, "0x572cd48cce93b2e58f1cc0253a7fdd4b4952a9c2", "f5026b37370655f4b880751dfd41b47fc50d972c00adb43ca07b02ab7ddb8b1c"),
}
# Verified empty-code previous blocks, first proxy code, and factory CREATE traces.
# docs/artifacts/oracle_repair_20260906/v2_proxy_deployment_origins.json
DEPLOYMENT_PROOF_SHA256 = "c1fa03b74aeb3bcf1f1c89620a0e475b6fe6b0f40ff5c73af6eb5bb72924d5dc"
MODULE_STARTS = {
    BINARY_MODULE: 87_479_502,
    NEG_RISK_MODULE: 87_479_533,
    COMBINATORIAL_MODULE: 87_479_439,
}

CONDITION_RESOLVED = oracle._event("ConditionResolved", (
    ("conditionId", "bytes31", True), ("result", "uint256[]", False),
))
RESULT_REPORTED = oracle._event("ResultReported", (
    ("resolver", "address", True), ("conditionId", "bytes31", True),
    ("result", "uint256[]", False),
))
COMBINATORIAL_PREPARED = oracle._event("CombinatorialConditionPrepared", (
    ("conditionId", "bytes31", True), ("legs", "uint256[]", False),
))
EVENT_PREPARED = oracle._event("EventPrepared", (
    ("eventId", "bytes29", True), ("conditionCount", "uint256", False),
    ("legacyEventId", "bytes32", False),
))
MIGRATION_REGISTERED = oracle._event("MigrationConditionRegistered", (
    ("v2ConditionId", "bytes31", True), ("legacyConditionId", "bytes32", True),
))
MIGRATION_RESOLVED = oracle._event("MigrationResolved", (
    ("conditionId", "bytes31", True), ("legacyConditionId", "bytes32", True),
    ("legacyPayout0", "uint256", False), ("legacyPayout1", "uint256", False),
    ("result0", "uint256", False), ("result1", "uint256", False),
))
UPGRADED = oracle._event("Upgraded", (("implementation", "address", True),))
EVENTS = {
    oracle._topic(abi): (status, abi) for status, abi in (
        ("settle", CONDITION_RESOLVED), ("report", RESULT_REPORTED),
        ("prepare", COMBINATORIAL_PREPARED), ("event_prepare", EVENT_PREPARED),
        ("prepare", MIGRATION_REGISTERED), ("migration_settle", MIGRATION_RESOLVED),
        ("upgrade", UPGRADED),
    )
}
FIELDS = (
    "tx_hash", "log_index", "block_number", "event_time", "module_address",
    "condition_id", "event_name", "event_status", "payouts_json", "legs_json", "payload_json",
)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _plain(value: Any) -> Any:
    if isinstance(value, bytes):
        return oracle._hex(value)
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def combo_condition_id(positions: Sequence[int]) -> str:
    data = encode(["uint256[]"], [list(positions)])
    base_hash = int.from_bytes(Web3.keccak(encode(["uint256", "bytes"], [3, data])), "big")
    value = (3 << 248) | ((base_hash & ((1 << 128) - 1)) << 120)
    return "0x" + value.to_bytes(32, "big")[:31].hex()


def _legs(positions: Sequence[int], condition_id: str) -> list[dict[str, Any]]:
    if not 1 <= len(positions) <= 50:
        raise ValueError("combinatorial condition requires 1..50 legs")
    result = []
    previous = -1
    for position in positions:
        if not 0 <= position < 2**256 or position >> 248 not in {1, 2} or position & 255 not in {0, 1}:
            raise ValueError("invalid combinatorial atomic leg")
        if position <= previous or (previous >= 0 and position >> 8 == previous >> 8):
            raise ValueError("combinatorial legs must be ascending with distinct conditions")
        result.append({
            "position_id": str(position), "condition_id": "0x" + position.to_bytes(32, "big")[:31].hex(),
            "module_id": position >> 248, "outcome_index": position & 255,
        })
        previous = position
    if combo_condition_id(positions) != condition_id:
        raise ValueError("combinatorial condition ID does not match canonical legs")
    return result


def _decode(log: Mapping[str, Any], event_time: datetime) -> dict[str, Any]:
    address = str(log["address"]).lower()
    module_id, implementation, abi_sha256 = MODULES[address]
    status, abi = EVENTS[oracle._topic0(log)]
    name = abi["name"]
    if (address == COMBINATORIAL_MODULE and name not in {"CombinatorialConditionPrepared", "Upgraded"}) or (
        address != COMBINATORIAL_MODULE and name == "CombinatorialConditionPrepared"
    ):
        raise ValueError("event is not part of this module ABI")
    args = oracle._decode(abi, log)
    condition = oracle._hex(args.get("conditionId", args.get("v2ConditionId"))) or None
    if condition and int(condition[2:4], 16) != module_id:
        raise ValueError("condition module ID differs from emitting module")
    payouts = list(args.get("result", []))
    if name == "MigrationResolved":
        payouts = [args["result0"], args["result1"]]
    if payouts or status in {"settle", "report", "migration_settle"}:
        if len(payouts) != 2 or any(value < 0 for value in payouts) or sum(payouts) != RESULT_DENOMINATOR:
            raise ValueError("module payout must have two entries summing to 1000000")
    legs = _legs(list(args["legs"]), condition) if name == "CombinatorialConditionPrepared" else []
    payload = {
        "args": _plain(args), "block_hash": oracle._hex(log.get("blockHash")),
        "transaction_index": oracle._int(log.get("transactionIndex", 0)),
        "topics": [oracle._hex(topic) for topic in log["topics"]],
        "data": oracle._hex(log["data"]),
        "abi_source_url": f"https://sourcify.dev/server/v2/contract/137/{implementation}?fields=abi",
        "abi_sha256": abi_sha256,
    }
    return {
        "tx_hash": oracle._hex(log["transactionHash"]), "log_index": oracle._int(log["logIndex"]),
        "block_number": oracle._int(log["blockNumber"]), "event_time": event_time,
        "module_address": address, "condition_id": condition,
        "event_name": name, "event_status": status,
        "payouts_json": payouts, "legs_json": legs, "payload_json": payload,
    }


def ensure_schema(conn: Any) -> None:
    conn.execute("SELECT pg_advisory_xact_lock(hashtext('market_data.oracle_modules.schema.v1'))")
    conn.execute("CREATE SCHEMA IF NOT EXISTS oracle")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
    conn.execute("""CREATE TABLE IF NOT EXISTS oracle.module_events (
        id BIGSERIAL PRIMARY KEY, tx_hash TEXT NOT NULL, log_index BIGINT NOT NULL,
        block_number BIGINT NOT NULL, event_time TIMESTAMPTZ NOT NULL,
        module_address TEXT NOT NULL, condition_id TEXT,
        event_name TEXT NOT NULL, event_status TEXT NOT NULL,
        payouts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        legs_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (tx_hash, log_index)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS module_events_condition_idx ON oracle.module_events(condition_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS module_events_block_idx ON oracle.module_events(block_number)")
    conn.execute("""CREATE INDEX IF NOT EXISTS module_events_upgrade_idx
        ON oracle.module_events(module_address, block_number DESC, log_index DESC)
        WHERE event_name='Upgraded'""")
    conn.execute("""CREATE TABLE IF NOT EXISTS oracle.module_implementation_observations (
        module_address TEXT PRIMARY KEY, block_number BIGINT NOT NULL,
        block_hash TEXT NOT NULL, implementation TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'rpc_storage_observation'
            CHECK (source='rpc_storage_observation'),
        observed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS ops.oracle_module_backfill_windows (
        cursor_key TEXT NOT NULL, from_block BIGINT NOT NULL, to_block BIGINT NOT NULL,
        events_sha256 TEXT NOT NULL, receipt_json JSONB NOT NULL,
        verified_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (cursor_key, from_block, to_block)
    )""")


def observe_implementations(rpc: RpcClient, conn: Any, block_number: int) -> list[dict[str, Any]]:
    """Stage current proxy storage observations; these are never event records."""
    if not 0 <= block_number <= rpc.head() - oracle.CONFIRMATIONS:
        raise ValueError("implementation observation requires a confirmed block")
    header = rpc.block(block_number)
    block_hash = str(header.get("hash", "")).lower() if header else ""
    if not header or oracle._int(header.get("number", -1)) != block_number or not re.fullmatch(r"0x[0-9a-f]{64}", block_hash) or int(block_hash, 16) == 0:
        raise RuntimeError("implementation observation lacks a valid block header/hash")
    addresses = list(MODULES)
    values = rpc.batch_call("eth_getStorageAt", ([address, IMPLEMENTATION_SLOT, hex(block_number)] for address in addresses))
    if len(values) != len(addresses):
        raise RuntimeError("RPC omitted module implementation storage results")
    rows = []
    for address, value in zip(addresses, values):
        if not isinstance(value, str) or not re.fullmatch(r"0x0{24}[0-9a-fA-F]{40}", value) or int(value, 16) == 0:
            raise RuntimeError(f"invalid or zero module implementation storage for {address}")
        rows.append({
            "module_address": address, "block_number": block_number, "block_hash": block_hash,
            "implementation": "0x" + value[-40:].lower(), "source": "rpc_storage_observation",
        })
    after = rpc.block(block_number)
    if not after or str(after.get("hash", "")).lower() != block_hash or oracle._int(after.get("number", -1)) != block_number:
        raise RuntimeError("implementation observation block changed during RPC reads")
    for row in rows:
        conn.execute("""INSERT INTO oracle.module_implementation_observations
            (module_address, block_number, block_hash, implementation, source)
            VALUES (%(module_address)s, %(block_number)s, %(block_hash)s, %(implementation)s, %(source)s)
            ON CONFLICT (module_address) DO UPDATE SET
                block_number=EXCLUDED.block_number, block_hash=EXCLUDED.block_hash,
                implementation=EXCLUDED.implementation, source=EXCLUDED.source,
                observed_at=CURRENT_TIMESTAMP
            WHERE oracle.module_implementation_observations.block_number <= EXCLUDED.block_number""", row)
    return rows


def _write(conn: Any, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    values = ", ".join(f"%({field})s" + ("::jsonb" if field.endswith("_json") else "") for field in FIELDS)
    updates = ", ".join(f"{field}=EXCLUDED.{field}" for field in FIELDS[2:])
    with conn.cursor() as cursor:
        cursor.executemany(
            f"INSERT INTO oracle.module_events ({', '.join(FIELDS)}) VALUES ({values}) "
            f"ON CONFLICT (tx_hash, log_index) DO UPDATE SET {updates}",
            [{field: _json(row[field]) if field.endswith("_json") else row[field] for field in FIELDS} for row in rows],
        )


def _index(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, int], str]:
    result = {}
    for row in rows:
        key = str(row["tx_hash"]).lower(), int(row["log_index"])
        if key in result:
            raise RuntimeError("duplicate module event transaction/log")
        value = {field: row[field] for field in FIELDS[2:]}
        value["event_time"] = row["event_time"].astimezone(timezone.utc).isoformat() if row["event_time"] else None
        result[key] = _json(value)
    return result


def _fetch_windows(rpc: RpcClient, start: int, end: int) -> Iterator[tuple[int, int, list]]:
    try:
        logs = oracle._fetch(rpc, start, end, MODULES, EVENTS, MODULE_STARTS, split_limits=False)
    except (ConnectionError, RuntimeError) as exc:
        message = str(exc).lower()
        if start == end or not any(part in message for part in (
            "query returned more than", "too many results", "response size exceeded",
            "block range limit", "maximum block range", "please limit the query",
        )):
            raise
        middle = (start + end) // 2
        yield from _fetch_windows(rpc, start, middle)
        yield from _fetch_windows(rpc, middle + 1, end)
        return
    yield start, end, logs


def collect_window(
    rpc: RpcClient, conn: Any, start: int, end: int, *, dry_run: bool = False,
    logs: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Stage exact module events; caller owns the commit and live cursor."""
    if logs is None:
        logs = [log for _, _, chunk in _fetch_windows(rpc, start, end) for log in chunk]
    if any(not start <= oracle._int(log["blockNumber"]) <= end for log in logs):
        raise RuntimeError("module RPC event lies outside requested block range")
    times = oracle._timestamps(rpc, logs)
    rows = [_decode(log, times[oracle._int(log["blockNumber"])]) for log in logs]
    expected = _index(rows)
    if not dry_run:
        _write(conn, rows)
    exists = conn.execute("SELECT to_regclass('oracle.module_events') AS relation").fetchone()["relation"]
    stored = conn.execute(
        f"SELECT {', '.join(FIELDS)} FROM oracle.module_events WHERE block_number BETWEEN %s AND %s",
        (start, end),
    ).fetchall() if exists else []
    actual = _index(stored)
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    mismatched = {key for key in expected.keys() & actual.keys() if expected[key] != actual[key]}
    receipt = {
        "from_block": start, "to_block": end, "module_events": len(rows),
        "prepare_events": sum(row["event_status"] == "prepare" for row in rows),
        "settle_events": sum(row["event_status"] == "settle" for row in rows),
        "database_events": len(actual), "missing": len(missing), "extra": len(extra),
        "mismatched": len(mismatched), "verified": not (missing or extra or mismatched),
        "events_sha256": hashlib.sha256(_json(sorted(expected.items())).encode()).hexdigest(),
    }
    if not dry_run and not receipt["verified"]:
        raise RuntimeError(f"module window verification failed: {_json(receipt)}")
    return receipt


def run(
    rpc: RpcClient, conn: Any, *, from_block: int | None = None, to_block: int | None = None,
    window_blocks: int = 100_000, max_windows: int | None = None, apply: bool = False,
) -> dict[str, Any]:
    if window_blocks < 1 or (max_windows is not None and max_windows < 1):
        raise ValueError("window_blocks and max_windows must be positive")
    locked = False
    try:
        if oracle._int(rpc.call("eth_chainId")) != oracle.POLYGON_CHAIN_ID:
            raise RuntimeError("module acquisition requires Polygon chain ID 137")
        head = rpc.head() - oracle.CONFIRMATIONS
        target = head if to_block is None else to_block
        if not 0 <= target <= head:
            raise ValueError("module target must not exceed confirmed head")
        previous = {}
        if apply:
            locked = conn.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS locked", (CURSOR_KEY,)).fetchone()["locked"]
            if not locked:
                raise RuntimeError("another module backfill is running")
            ensure_schema(conn)
            state_exists = conn.execute("SELECT to_regclass('ops.sync_state') AS relation").fetchone()["relation"]
            state = conn.execute("SELECT value FROM ops.sync_state WHERE key=%s", (CURSOR_KEY,)).fetchone() if state_exists else None
            previous = json.loads(state["value"]) if state else {}
        origin = from_block if from_block is not None else previous.get("from_block", min(MODULE_STARTS.values()))
        if not 0 <= origin <= target:
            raise ValueError("module from_block is outside requested range")
        if previous and (previous.get("version") != 1 or previous.get("from_block") != origin):
            raise RuntimeError("module backfill origin changed; refusing a gap or reset")
        if apply:
            observe_implementations(rpc, conn, target)
        start = previous.get("last_block", origin - 1) + 1
        stats = {
            "version": 1, "from_block": origin, "target_block": target,
            "deployment_proof_sha256": DEPLOYMENT_PROOF_SHA256,
            "last_block": start - 1, "windows": previous.get("windows", 0),
            "module_events": previous.get("module_events", 0), "windows_this_run": 0,
            "unverified_windows": 0, "apply": apply,
        }
        while start <= target and (max_windows is None or stats["windows_this_run"] < max_windows):
            for low, high, logs in _fetch_windows(rpc, start, min(start + window_blocks - 1, target)):
                receipt = collect_window(rpc, conn, low, high, dry_run=not apply, logs=logs)
                stats["last_block"] = high
                stats["last_window"] = receipt
                stats["windows"] += 1
                stats["windows_this_run"] += 1
                stats["module_events"] += receipt["module_events"]
                stats["unverified_windows"] += int(not receipt["verified"])
                stats["complete"] = high >= target and not stats["unverified_windows"]
                if apply:
                    conn.execute("""INSERT INTO ops.oracle_module_backfill_windows
                        (cursor_key, from_block, to_block, events_sha256, receipt_json)
                        VALUES (%s,%s,%s,%s,%s::jsonb)
                        ON CONFLICT (cursor_key, from_block, to_block) DO UPDATE SET
                            events_sha256=EXCLUDED.events_sha256, receipt_json=EXCLUDED.receipt_json,
                            verified_at=CURRENT_TIMESTAMP""",
                        (CURSOR_KEY, low, high, receipt["events_sha256"], _json(receipt)))
                    set_state(conn, CURSOR_KEY, value=stats, last_block=high)
                    conn.commit()
                else:
                    conn.rollback()
                print(_json(receipt), flush=True)
                start = high + 1
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
    parser.add_argument("--from-block", type=int)
    parser.add_argument("--to-block", type=int)
    parser.add_argument("--window-blocks", type=int, default=100_000)
    parser.add_argument("--max-windows", type=int)
    parser.add_argument("--rpc-url", action="append", help="use only these RPC endpoints; repeat for explicit fallbacks")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--dry-run", action="store_true", help="default: read-only exact audit")
    args = parser.parse_args(argv)
    with connect_postgres() as conn:
        conn.commit()
        conn.read_only = not args.apply
        stats = run(
            RpcClient(tuple(args.rpc_url) if args.rpc_url else polygon_rpc_urls("oracle")), conn, from_block=args.from_block,
            to_block=args.to_block, window_blocks=args.window_blocks,
            max_windows=args.max_windows, apply=args.apply,
        )
    print(_json(stats), flush=True)
    return int(bool(stats["unverified_windows"]))


if __name__ == "__main__":
    raise SystemExit(main())
