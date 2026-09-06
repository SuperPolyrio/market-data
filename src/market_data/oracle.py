"""Polygon Oracle acquisition for Polymarket UMA and Up/Down markets.

The contract manifest is pinned to Polymarket's resolution subgraph revision
``75d1818547862a5bd3477ed2e6b16f693d42dab6``.  This module intentionally does
only acquisition, deterministic enrichment and idempotent storage; historical
repair/export code belongs outside the collector.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from hexbytes import HexBytes
from web3 import Web3
from web3._utils.events import get_event_data

from market_data.config import polygon_rpc_urls
from market_data.rpc import RpcClient, watch
from market_data.storage import connect_postgres, get_state, set_state


POLYGON_CHAIN_ID = 137
CONFIRMATIONS = 20
REWIND_BLOCKS = 100
OFFICIAL_MANIFEST_COMMIT = "75d1818547862a5bd3477ed2e6b16f693d42dab6"
_CODEC = Web3().codec

OLD_ORACLE = "0xbb1a8db2d4350976a11cdfa60a1d43f97710da49"
OO_V2_OLD = "0x2c0367a9db231ddebd88a94b4f6461a6e47c58b1"
OO_V2 = "0xee3afe347d5c74317041e2618c49534daf887c24"
OLD_ADAPTER = "0xcb1822859cef82cd2eb4e6276c7916e692995130"
SPORTS_REQUESTER = "0xb21182d0494521cF45DbbeEbb5A3ACAAb6d22093".lower()
CTF = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"

ORACLE_STARTS = {
    OLD_ORACLE: 23_569_780,
    OO_V2: 35_203_539,
    OO_V2_OLD: 74_677_419,
}
ADAPTER_STARTS = {
    OLD_ADAPTER: 23_569_780,
    "0x6a9d222616c90fca5754cd1333cfd9b7fb6a4f74": 35_203_539,
    "0x157ce2d672854c848c9b79c49a8cc6cc89176a49": 46_755_254,
    "0x2f5e3684cb1f318ec51b00edba38d79ac2c0aa9d": 50_505_488,
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": 74_797_879,
    "0x69c47de9d4d3dad79590d61b9e05918e03775f24": 82_000_000,
}
NEG_RISK_OPERATORS = (
    "0x661992aebf6becf7ba5abB66f6b0bf62aa7a2e93".lower(),
    "0x71523d0f655b41e805cec45b17163f528b59b820",
)
NEG_RISK_ADAPTERS = {
    "0x69c47de9d4d3dad79590d61b9e05918e03775f24",
    "0x2f5e3684cb1f318ec51b00edba38d79ac2c0aa9d",
}
REQUESTERS_BY_ORACLE = {
    OLD_ORACLE: {OLD_ADAPTER},
    OO_V2: {
        "0x6a9d222616c90fca5754cd1333cfd9b7fb6a4f74",
        "0x157ce2d672854c848c9b79c49a8cc6cc89176a49",
        "0x2f5e3684cb1f318ec51b00edba38d79ac2c0aa9d",
        SPORTS_REQUESTER,
    },
    OO_V2_OLD: {
        "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7",
        "0x69c47de9d4d3dad79590d61b9e05918e03775f24",
    },
}
FULL_HISTORY_START = min((*ORACLE_STARTS.values(), *ADAPTER_STARTS.values()))

UMA_LIVE_KEY = "oracle_sync"
UMA_LIVENESS_KEY = "oracle_sync_live"
UPDOWN_LIVE_KEY = "oracle_updown_sync_live"
MODULES_LIVE_KEY = "oracle_modules_sync_live"
UMA_BACKFILL_KEY = "oracle_backfill_full"
UPDOWN_BACKFILL_KEY = "oracle_backfill_updown"


def _event(name: str, inputs: Sequence[tuple[str, str, bool]]) -> dict[str, Any]:
    return {
        "anonymous": False,
        "name": name,
        "type": "event",
        "inputs": [
            {"name": arg_name, "type": arg_type, "indexed": indexed}
            for arg_name, arg_type, indexed in inputs
        ],
    }


REQUEST = _event("RequestPrice", (
    ("requester", "address", True), ("identifier", "bytes32", False),
    ("timestamp", "uint256", False), ("ancillaryData", "bytes", False),
    ("currency", "address", False), ("reward", "uint256", False),
    ("finalFee", "uint256", False),
))
PROPOSE = _event("ProposePrice", (
    ("requester", "address", True), ("proposer", "address", True),
    ("identifier", "bytes32", False), ("timestamp", "uint256", False),
    ("ancillaryData", "bytes", False), ("proposedPrice", "int256", False),
    ("expirationTimestamp", "uint256", False), ("currency", "address", False),
))
DISPUTE = _event("DisputePrice", (
    ("requester", "address", True), ("proposer", "address", True),
    ("disputer", "address", True), ("identifier", "bytes32", False),
    ("timestamp", "uint256", False), ("ancillaryData", "bytes", False),
    ("proposedPrice", "int256", False),
))
SETTLE = _event("Settle", (
    ("requester", "address", True), ("proposer", "address", True),
    ("disputer", "address", True), ("identifier", "bytes32", False),
    ("timestamp", "uint256", False), ("ancillaryData", "bytes", False),
    ("price", "int256", False), ("payout", "uint256", False),
))
QUESTION_INITIALIZED = _event("QuestionInitialized", (
    ("questionID", "bytes32", True), ("requestTimestamp", "uint256", True),
    ("creator", "address", True), ("ancillaryData", "bytes", False),
    ("rewardToken", "address", False), ("reward", "uint256", False),
    ("proposalBond", "uint256", False),
))
QUESTION_INITIALIZED_OLD = _event("QuestionInitialized", (
    ("questionID", "bytes32", True), ("ancillaryData", "bytes", False),
    ("resolutionTime", "uint256", False), ("rewardToken", "address", False),
    ("reward", "uint256", False), ("proposalBond", "uint256", False),
    ("earlyResolutionEnabled", "bool", False),
))
QUESTION_PREPARED = _event("QuestionPrepared", (
    ("marketId", "bytes32", True), ("questionId", "bytes32", True),
    ("requestId", "bytes32", True), ("index", "uint256", False),
    ("data", "bytes", False),
))
CONDITION_PREPARATION = _event("ConditionPreparation", (
    ("conditionId", "bytes32", True), ("oracle", "address", True),
    ("questionId", "bytes32", True), ("outcomeSlotCount", "uint256", False),
))
CONDITION_RESOLUTION = _event("ConditionResolution", (
    ("conditionId", "bytes32", True), ("oracle", "address", True),
    ("questionId", "bytes32", True), ("outcomeSlotCount", "uint256", False),
    ("payoutNumerators", "uint256[]", False),
))


def _signature(abi: Mapping[str, Any]) -> str:
    types = ",".join(str(item["type"]) for item in abi["inputs"])
    return f'{abi["name"]}({types})'


def _topic(abi: Mapping[str, Any]) -> str:
    return "0x" + Web3.keccak(text=_signature(abi)).hex()


UMA_BY_TOPIC = {
    _topic(REQUEST): ("request", REQUEST),
    _topic(PROPOSE): ("propose", PROPOSE),
    _topic(DISPUTE): ("dispute", DISPUTE),
    _topic(SETTLE): ("settle", SETTLE),
}
CURRENT_INIT_TOPIC = _topic(QUESTION_INITIALIZED)
OLD_INIT_TOPIC = _topic(QUESTION_INITIALIZED_OLD)
RESET_TOPIC = "0x" + Web3.keccak(text="QuestionReset(bytes32)").hex()
CURRENT_RESOLVED_TOPIC = "0x" + Web3.keccak(text="QuestionResolved(bytes32,int256,uint256[])").hex()
OLD_RESOLVED_TOPIC = "0x" + Web3.keccak(text="QuestionResolved(bytes32,bool)").hex()
OLD_SETTLED_TOPIC = "0x" + Web3.keccak(text="QuestionSettled(bytes32,int256,bool)").hex()
PREPARED_TOPIC = _topic(QUESTION_PREPARED)
CTF_BY_TOPIC = {
    _topic(CONDITION_PREPARATION): ("request", CONDITION_PREPARATION),
    _topic(CONDITION_RESOLUTION): ("settle", CONDITION_RESOLUTION),
}


def _int(value: Any) -> int:
    if isinstance(value, int):
        return value
    return int(str(value), 16) if str(value).startswith("0x") else int(value)


def _hex(value: Any) -> str:
    if value is None or value == "":
        return ""
    raw = value.hex() if hasattr(value, "hex") else str(value)
    return (raw if raw.startswith("0x") else "0x" + raw).lower()


def _address(value: Any) -> str:
    raw = _hex(value)
    return "0x" + raw[-40:] if len(raw) >= 42 else ""


def _signed_topic(value: Any) -> int:
    number = _int(_hex(value))
    return number - 2**256 if number >= 2**255 else number


def _normalized_log(raw: Mapping[str, Any]) -> dict[str, Any]:
    log = dict(raw)
    for field in ("blockNumber", "transactionIndex", "logIndex"):
        log[field] = _int(log.get(field, 0))
    for field in ("transactionHash", "blockHash", "data"):
        log[field] = HexBytes(log.get(field) or "0x")
    log["topics"] = [HexBytes(topic) for topic in (log.get("topics") or [])]
    log["removed"] = bool(log.get("removed", False))
    return log


def _decode(abi: Mapping[str, Any], raw: Mapping[str, Any]) -> dict[str, Any]:
    return dict(get_event_data(_CODEC, abi, _normalized_log(raw))["args"])


def _topic0(log: Mapping[str, Any]) -> str:
    topics = log.get("topics") or []
    return _hex(topics[0]) if topics else ""


def _ancillary(data: Any) -> tuple[str, str]:
    raw = bytes(data or b"")
    # PostgreSQL text/jsonb cannot contain U+0000.  Preserve the authoritative
    # bytes in ancillary_hex and make the readable projection reversible
    # instead of dropping embedded NULs.
    readable = raw.decode("utf-8", errors="replace").replace("\x00", "\\x00")
    return "0x" + raw.hex(), readable


def _parse_raw(text: str) -> tuple[str, str, str, str]:
    market = re.search(r"market_id:\s*([0-9]+)", text, re.I)
    title = re.search(r"title:\s*(.*?)(?:,\s*description:|$)", text, re.I | re.S)
    p1 = re.search(r"p1:\s*([0-9]+)", text)
    p2 = re.search(r"p2:\s*([0-9]+)", text)
    return (
        market.group(1) if market else "",
        title.group(1).strip() if title else "",
        p1.group(1) if p1 else "",
        p2.group(1) if p2 else "",
    )


def _price(value: Any) -> str:
    if value is None:
        return ""
    rendered = format(Decimal(int(value)) / Decimal(10**18), "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def _ensure_schema(conn: Any) -> None:
    ready = conn.execute(
        """
        SELECT (
            to_regclass('oracle.oracle_events') IS NOT NULL
            AND to_regclass('oracle.adapter_events') IS NOT NULL
            AND to_regclass('oracle.uma_adapter_mapping') IS NOT NULL
            AND to_regclass('oracle.neg_risk_request_mapping') IS NOT NULL
        ) AS ready
        """
    ).fetchone()
    if ready and bool(ready["ready"] if isinstance(ready, Mapping) else ready[0]):
        return
    conn.execute("CREATE SCHEMA IF NOT EXISTS oracle")
    conn.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS oracle.uma_adapter_mapping (
            id BIGSERIAL PRIMARY KEY,
            ancillary_data TEXT NOT NULL,
            ancillary_data_hash TEXT GENERATED ALWAYS AS
                (encode(digest(ancillary_data, 'sha256'), 'hex')) STORED,
            question_id TEXT NOT NULL,
            source_adapter TEXT,
            UNIQUE (ancillary_data_hash)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS oracle.neg_risk_request_mapping (
            request_id TEXT PRIMARY KEY,
            question_id TEXT NOT NULL,
            market_id TEXT,
            source_operator TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS oracle.adapter_events (
            id BIGSERIAL PRIMARY KEY,
            tx_hash TEXT NOT NULL,
            log_index BIGINT NOT NULL,
            block_number BIGINT NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            event_status TEXT NOT NULL,
            source_adapter TEXT NOT NULL,
            question_id TEXT NOT NULL,
            market_id BIGINT,
            condition_id TEXT,
            settled_price_raw TEXT,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (tx_hash, log_index)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS oracle.oracle_events (
            id BIGSERIAL PRIMARY KEY,
            tx_hash TEXT NOT NULL,
            log_index BIGINT NOT NULL,
            block_number BIGINT NOT NULL,
            event_time TIMESTAMPTZ,
            raw_event_time TEXT,
            event_status TEXT NOT NULL CHECK
                (event_status IN ('request','propose','dispute','settle')),
            external_market_id TEXT,
            market_id BIGINT,
            market_title TEXT,
            source_adapter TEXT,
            source_oracle TEXT,
            adapter_question_id TEXT,
            matched_by TEXT,
            question_id TEXT,
            condition_id TEXT,
            string_raw TEXT,
            p1 TEXT,
            p2 TEXT,
            proposed_price TEXT,
            settled_price TEXT,
            settlement_recipient TEXT,
            payout TEXT,
            requester TEXT,
            proposer TEXT,
            disputer TEXT,
            request_transaction TEXT,
            proposal_transaction TEXT,
            settlement_transaction TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (tx_hash, log_index)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS oracle_events_block_idx ON oracle.oracle_events(block_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS adapter_events_block_idx ON oracle.adapter_events(block_number)")


def _fetch(
    rpc: RpcClient,
    start: int,
    end: int,
    addresses: Iterable[str],
    topics: Iterable[str],
    starts: Mapping[str, int] | None = None,
    *,
    split_limits: bool = True,
) -> list[dict[str, Any]]:
    active = [a.lower() for a in addresses if not starts or end >= starts.get(a.lower(), 0)]
    if not active or start > end:
        return []
    effective_start = max(start, min((starts or {}).get(a, start) for a in active))
    topic_list = list(topics)
    try:
        logs = rpc.logs(effective_start, end, addresses=active, topics=[topic_list])
    except (ConnectionError, RuntimeError) as exc:
        message = str(exc).lower()
        if not split_limits or effective_start == end or not any(part in message for part in (
            "query returned more than", "too many results", "response size exceeded",
            "block range is too", "block range limit", "maximum block range",
        )):
            raise
        middle = (effective_start + end) // 2
        return (
            _fetch(rpc, effective_start, middle, active, topic_list, starts)
            + _fetch(rpc, middle + 1, end, active, topic_list, starts)
        )
    return [dict(log) for log in logs if not log.get("removed")]


def _timestamps(rpc: RpcClient, logs: Iterable[Mapping[str, Any]]) -> dict[int, datetime]:
    logs = list(logs)
    timestamps: dict[int, int] = {}
    hashes: dict[int, str] = {}
    for log in logs:
        number = _int(log["blockNumber"])
        block_hash = _hex(log.get("blockHash"))
        if block_hash:
            if number in hashes and hashes[number] != block_hash:
                raise RuntimeError(f"conflicting Polygon block hashes for {number}")
            hashes[number] = block_hash
        if log.get("blockTimestamp") is not None:
            timestamp = _int(log["blockTimestamp"])
            if timestamp <= 0 or (number in timestamps and timestamps[number] != timestamp):
                raise RuntimeError(f"conflicting Polygon block timestamp for {number}")
            timestamps[number] = timestamp
    missing = sorted({_int(log["blockNumber"]) for log in logs} - timestamps.keys())
    blocks = rpc.blocks(missing) if hasattr(rpc, "blocks") else [rpc.block(n) for n in missing]
    if len(blocks) != len(missing):
        raise RuntimeError("Polygon block timestamp response omitted blocks")
    for number, block in zip(missing, blocks):
        if not block or block.get("timestamp") is None:
            raise RuntimeError(f"missing Polygon block timestamp for {number}")
        if block.get("number") is not None and _int(block["number"]) != number:
            raise RuntimeError(f"unexpected Polygon block header for {number}")
        if block.get("hash") and number in hashes and _hex(block["hash"]) != hashes[number]:
            raise RuntimeError(f"Polygon block hash differs from event for {number}")
        timestamps[number] = _int(block["timestamp"])
    return {number: datetime.fromtimestamp(value, timezone.utc) for number, value in timestamps.items()}


def _decode_adapter(log: Mapping[str, Any], event_time: datetime) -> dict[str, Any] | None:
    source = str(log.get("address") or "").lower()
    topic = _topic0(log)
    topics = log.get("topics") or []
    label = ""
    question_id = _hex(topics[1])[:66] if len(topics) > 1 else ""
    ancillary_hex = ""
    settled = ""
    payload: dict[str, Any] = {}
    if topic in {CURRENT_INIT_TOPIC, OLD_INIT_TOPIC}:
        abi = QUESTION_INITIALIZED_OLD if source == OLD_ADAPTER else QUESTION_INITIALIZED
        args = _decode(abi, log)
        question_id = _hex(args["questionID"])[:66]
        ancillary_hex, _ = _ancillary(args["ancillaryData"])
        label = "adapter_question_initialized"
        payload["ancillary_data"] = ancillary_hex.lower()
    elif topic == RESET_TOPIC:
        label = "adapter_question_reset"
    elif source == OLD_ADAPTER and topic == OLD_RESOLVED_TOPIC:
        label = "adapter_question_resolved"
        payload["emergency_report"] = bool(_int(_hex(topics[2]))) if len(topics) > 2 else False
    elif source == OLD_ADAPTER and topic == OLD_SETTLED_TOPIC:
        label = "adapter_question_settled"
        settled = str(_signed_topic(topics[2])) if len(topics) > 2 else ""
        payload["early_resolution"] = bool(_int(_hex(topics[3]))) if len(topics) > 3 else False
    elif source != OLD_ADAPTER and topic == CURRENT_RESOLVED_TOPIC:
        label = "adapter_question_resolved"
        settled = str(_signed_topic(topics[2])) if len(topics) > 2 else ""
        payload["payouts_abi"] = _hex(log.get("data"))
    else:
        return None
    payload.update({"event": label, "question_id": question_id})
    if settled:
        payload["settled_price_raw"] = settled
    return {
        "tx_hash": _hex(log.get("transactionHash")),
        "log_index": _int(log.get("logIndex", 0)),
        "block_number": _int(log.get("blockNumber", 0)),
        "event_time": event_time,
        "event_status": label,
        "source_adapter": source,
        "question_id": question_id.lower(),
        "market_id": None,
        "condition_id": "",
        "settled_price_raw": settled,
        "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        "ancillary_hex": ancillary_hex.lower(),
    }


def _decode_prepared(log: Mapping[str, Any]) -> dict[str, str]:
    args = _decode(QUESTION_PREPARED, log)
    return {
        "request_id": _hex(args["requestId"])[:66],
        "question_id": _hex(args["questionId"])[:66],
        "market_id": _hex(args["marketId"])[:66],
        "source_operator": str(log.get("address") or "").lower(),
    }


def _load_adapter_map(conn: Any, ancillary_keys: Iterable[str]) -> dict[str, tuple[str, str]]:
    keys = sorted({key.lower() for key in ancillary_keys if key})
    if not keys:
        return {}
    hashes = [hashlib.sha256(key.encode()).hexdigest() for key in keys]
    rows = conn.execute(
        """
        SELECT ancillary_data, question_id, source_adapter
        FROM oracle.uma_adapter_mapping
        WHERE ancillary_data_hash = ANY(%s::text[])
        """,
        (hashes,),
    ).fetchall()
    return {
        str(row["ancillary_data"]).lower(): (
            str(row["question_id"] or "").lower(),
            str(row["source_adapter"] or "").lower(),
        )
        for row in rows
    }


def _load_neg_map(conn: Any, request_ids: Iterable[str]) -> dict[str, tuple[str, str, str]]:
    keys = sorted({key.lower() for key in request_ids if key})
    if not keys:
        return {}
    rows = conn.execute(
        """
        SELECT request_id, question_id, market_id, source_operator
        FROM oracle.neg_risk_request_mapping
        WHERE request_id = ANY(%s::text[])
        """,
        (keys,),
    ).fetchall()
    return {
        str(row["request_id"]).lower(): (
            str(row["question_id"] or "").lower(),
            str(row["market_id"] or "").lower(),
            str(row["source_operator"] or "").lower(),
        )
        for row in rows
    }


def _market_index(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Mapping[str, Any]]]:
    buckets: dict[str, dict[str, list[Mapping[str, Any]]]] = {
        "gamma": {}, "condition": {}, "question": {},
    }
    for row in rows:
        for kind, field in (("gamma", "gamma_market_id"), ("condition", "condition_id"), ("question", "question_id")):
            if row.get(field):
                buckets[kind].setdefault(str(row[field]).lower(), []).append(row)
    return {
        kind: {key: values[0] for key, values in entries.items() if len(values) == 1}
        for kind, entries in buckets.items()
    }


def _load_markets(
    conn: Any,
    *,
    gamma_ids: Iterable[str] = (),
    question_ids: Iterable[str] = (),
    condition_ids: Iterable[str] = (),
    updown_only: bool = False,
) -> dict[str, dict[str, Mapping[str, Any]]]:
    gamma = sorted({str(value) for value in gamma_ids if value})
    question = sorted({str(value).lower() for value in question_ids if value})
    condition = sorted({str(value).lower() for value in condition_ids if value})
    if not gamma and not question and not condition:
        return _market_index(())
    rows = conn.execute(
        """
        SELECT id, gamma_market_id, slug, title, description, question_id, condition_id
        FROM core.markets
        WHERE (%s = FALSE OR slug LIKE '%%-updown-%%')
          AND (
            gamma_market_id = ANY(%s::text[])
            OR question_id = ANY(%s::text[])
            OR condition_id = ANY(%s::text[])
          )
        """,
        (updown_only, gamma, question, condition),
    ).fetchall()
    return _market_index(rows)


def _bridge(index: Mapping[str, Mapping[str, Mapping[str, Any]]], gamma: str, qid: str) -> tuple[Mapping[str, Any] | None, str]:
    matches = []
    if gamma and gamma in index["gamma"]:
        matches.append((index["gamma"][gamma], "by_gamma_market_id"))
    key = qid.lower()
    if key and key in index["condition"]:
        matches.append((index["condition"][key], "by_condition_id"))
    if key and key in index["question"]:
        matches.append((index["question"][key], "question_id"))
    if matches and len({row["id"] for row, _ in matches}) == 1:
        return matches[0]
    return None, ""


def _write_adapter_events(conn: Any, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO oracle.adapter_events (
                tx_hash, log_index, block_number, event_time, event_status,
                source_adapter, question_id, market_id, condition_id,
                settled_price_raw, payload_json
            ) VALUES (
                %(tx_hash)s, %(log_index)s, %(block_number)s, %(event_time)s,
                %(event_status)s, %(source_adapter)s, %(question_id)s,
                %(market_id)s, %(condition_id)s, %(settled_price_raw)s,
                %(payload_json)s::jsonb
            )
            ON CONFLICT (tx_hash, log_index) DO UPDATE SET
                block_number=EXCLUDED.block_number,
                event_time=EXCLUDED.event_time,
                event_status=EXCLUDED.event_status,
                source_adapter=EXCLUDED.source_adapter,
                question_id=EXCLUDED.question_id,
                market_id=COALESCE(EXCLUDED.market_id, oracle.adapter_events.market_id),
                condition_id=COALESCE(NULLIF(EXCLUDED.condition_id, ''), oracle.adapter_events.condition_id),
                settled_price_raw=EXCLUDED.settled_price_raw,
                payload_json=EXCLUDED.payload_json
            WHERE ROW(
                oracle.adapter_events.block_number, oracle.adapter_events.event_time,
                oracle.adapter_events.event_status, oracle.adapter_events.source_adapter,
                oracle.adapter_events.question_id, oracle.adapter_events.market_id,
                oracle.adapter_events.condition_id, oracle.adapter_events.settled_price_raw,
                oracle.adapter_events.payload_json
            ) IS DISTINCT FROM ROW(
                EXCLUDED.block_number, EXCLUDED.event_time, EXCLUDED.event_status,
                EXCLUDED.source_adapter, EXCLUDED.question_id,
                COALESCE(EXCLUDED.market_id, oracle.adapter_events.market_id),
                COALESCE(NULLIF(EXCLUDED.condition_id, ''), oracle.adapter_events.condition_id),
                EXCLUDED.settled_price_raw, EXCLUDED.payload_json
            )
            """,
            rows,
        )


def _refresh_adapter_projection(conn: Any, ancillary_keys: Iterable[str]) -> None:
    keys = sorted({key.lower() for key in ancillary_keys if key})
    if not keys:
        return
    hashes = [hashlib.sha256(key.encode()).hexdigest() for key in keys]
    conn.execute(
        """
        INSERT INTO oracle.uma_adapter_mapping
            (ancillary_data, question_id, source_adapter)
        SELECT ancillary_data, question_id, source_adapter
        FROM (
            SELECT DISTINCT ON (payload_json->>'ancillary_data')
                payload_json->>'ancillary_data' AS ancillary_data,
                question_id,
                source_adapter
            FROM oracle.adapter_events
            WHERE event_status = 'adapter_question_initialized'
              AND payload_json->>'ancillary_data' = ANY(%s::text[])
              AND encode(digest(payload_json->>'ancillary_data', 'sha256'), 'hex') = ANY(%s::text[])
            ORDER BY payload_json->>'ancillary_data', block_number DESC,
                     log_index DESC, tx_hash DESC
        ) latest
        ON CONFLICT (ancillary_data_hash) DO UPDATE SET
            question_id=EXCLUDED.question_id,
            source_adapter=EXCLUDED.source_adapter
        WHERE (oracle.uma_adapter_mapping.question_id, oracle.uma_adapter_mapping.source_adapter)
              IS DISTINCT FROM (EXCLUDED.question_id, EXCLUDED.source_adapter)
        """,
        (keys, hashes),
    )


def _write_neg_map(conn: Any, rows: Sequence[Mapping[str, str]]) -> None:
    if not rows:
        return
    current = _load_neg_map(conn, (row["request_id"] for row in rows))
    for row in rows:
        old = current.get(row["request_id"])
        if old and old[0] != row["question_id"]:
            raise RuntimeError(f'conflicting QuestionPrepared request id {row["request_id"]}')
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO oracle.neg_risk_request_mapping
                (request_id, question_id, market_id, source_operator)
            VALUES (%(request_id)s, %(question_id)s, %(market_id)s, %(source_operator)s)
            ON CONFLICT (request_id) DO UPDATE SET
                question_id=EXCLUDED.question_id,
                market_id=COALESCE(NULLIF(EXCLUDED.market_id, ''), oracle.neg_risk_request_mapping.market_id),
                source_operator=COALESCE(NULLIF(EXCLUDED.source_operator, ''), oracle.neg_risk_request_mapping.source_operator)
            """,
            rows,
        )


def _write_oracle_events(conn: Any, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    columns = (
        "tx_hash", "log_index", "block_number", "event_time", "event_status",
        "external_market_id", "market_id", "market_title", "source_adapter",
        "source_oracle", "adapter_question_id", "matched_by", "question_id",
        "condition_id", "string_raw", "p1", "p2", "proposed_price",
        "settled_price", "settlement_recipient", "payout", "requester",
        "proposer", "disputer", "request_transaction", "proposal_transaction",
        "settlement_transaction",
    )
    names = ", ".join(columns)
    values = ", ".join(f"%({column})s" for column in columns)
    enrichment = {"external_market_id", "market_title", "matched_by",
                  "adapter_question_id", "question_id", "condition_id"}
    expressions = [
        f"COALESCE(NULLIF(EXCLUDED.{column}, ''), oracle.oracle_events.{column})"
        if column in enrichment else
        f"COALESCE(EXCLUDED.{column}, oracle.oracle_events.{column})"
        if column == "market_id" else f"EXCLUDED.{column}"
        for column in columns[2:]
    ]
    updates = ", ".join(f"{column}={value}" for column, value in zip(columns[2:], expressions))
    unchanged = ", ".join(f"oracle.oracle_events.{column}" for column in columns[2:])
    with conn.cursor() as cursor:
        cursor.executemany(
            f"""
            INSERT INTO oracle.oracle_events ({names}) VALUES ({values})
            ON CONFLICT (tx_hash, log_index) DO UPDATE SET {updates}
            WHERE ROW({unchanged}) IS DISTINCT FROM ROW({', '.join(expressions)})
            """,
            rows,
        )


def _authorized(log: Mapping[str, Any]) -> bool:
    source = str(log.get("address") or "").lower()
    topics = log.get("topics") or []
    requester = _address(topics[1]) if len(topics) > 1 else ""
    return requester in REQUESTERS_BY_ORACLE.get(source, set())


def reconcile_market_links(conn: Any, *, limit: int = 10_000) -> dict[str, int]:
    """Retry a bounded, rotating page of orphans without moving chain cursors."""
    if limit < 1:
        raise ValueError("reconcile limit must be positive")
    state_key = "oracle_market_reconciliation_v1"
    if not conn.execute(
        "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS locked", (state_key,),
    ).fetchone()["locked"]:
        return {"oracle_linked": 0, "adapter_linked": 0, "pass_complete": 1}
    state = get_state(conn, state_key)
    offsets = json.loads(state["value"]) if state and state.get("value") else {}
    counts: dict[str, int] = {}
    for table in ("oracle_events", "adapter_events"):
        rows = conn.execute(
            f"SELECT * FROM oracle.{table} WHERE market_id IS NULL AND id > %s "
            "ORDER BY id LIMIT %s",
            (int(offsets.get(table, 0)), limit),
        ).fetchall()
        # A complete pass wraps, so permanently unknown old markets cannot
        # starve markets that arrive later. These IDs are not block cursors.
        offsets[table] = int(rows[-1]["id"]) if len(rows) == limit else 0
        keys = {}
        if table == "oracle_events":
            keys = {
                int(row["id"]): "0x" + row["string_raw"].encode("utf-8").hex()
                for row in rows if row.get("string_raw")
                and row.get("source_oracle") != CTF
                and not row.get("adapter_question_id")
                and "\\x00" not in row["string_raw"] and "\ufffd" not in row["string_raw"]
            }
        adapter_map = _load_adapter_map(conn, keys.values())
        questions = {}
        for row in rows:
            fallback = adapter_map.get(keys.get(int(row["id"]), ""), ("", ""))[0]
            questions[int(row["id"])] = str(
                row.get("adapter_question_id") or row.get("question_id") or fallback or ""
            )
        neg_map = _load_neg_map(conn, questions.values())
        lookups = {
            int(row["id"]): neg_map.get(questions[int(row["id"])], (questions[int(row["id"])], "", ""))[0]
            if row.get("source_adapter") in NEG_RISK_ADAPTERS else questions[int(row["id"])]
            for row in rows
        }
        markets = _load_markets(
            conn, gamma_ids=(row.get("external_market_id") for row in rows),
            condition_ids=(row.get("condition_id") for row in rows),
            question_ids=lookups.values(),
        )
        updates = []
        for row in rows:
            condition = str(row.get("condition_id") or "").lower()
            market = markets["condition"].get(condition)
            if row.get("source_oracle") != CTF:
                candidate, _ = _bridge(markets, str(row.get("external_market_id") or ""), lookups[int(row["id"])])
                if market and candidate and market["id"] != candidate["id"]:
                    continue
                market = market or candidate
            if market:
                updates.append((market["id"], market.get("condition_id") or "", int(row["id"])))
        with conn.cursor() as cursor:
            provenance = ", matched_by='by_reconciled_market_identity'" if table == "oracle_events" else ""
            cursor.executemany(
                f"UPDATE oracle.{table} SET market_id=%s, "
                f"condition_id=COALESCE(NULLIF(condition_id, ''), %s){provenance} "
                "WHERE id=%s AND market_id IS NULL", updates,
            )
        counts["oracle_linked" if table == "oracle_events" else "adapter_linked"] = len(updates)
    set_state(conn, state_key, value=offsets)
    counts["pass_complete"] = int(not any(offsets.values()))
    return counts


def _decode_uma(log: Mapping[str, Any], event_time: datetime) -> dict[str, Any]:
    status, abi = UMA_BY_TOPIC[_topic0(log)]
    args = _decode(abi, log)
    ancillary_hex, raw = _ancillary(args.get("ancillaryData"))
    gamma, title, p1, p2 = _parse_raw(raw)
    tx_hash = _hex(log.get("transactionHash"))
    return {
        "status": status,
        "args": args,
        "ancillary_hex": ancillary_hex.lower(),
        "raw": raw,
        "gamma": gamma,
        "title": title,
        "p1": p1,
        "p2": p2,
        "tx_hash": tx_hash,
        "log_index": _int(log.get("logIndex", 0)),
        "block_number": _int(log.get("blockNumber", 0)),
        "event_time": event_time,
        "source_oracle": str(log.get("address") or "").lower(),
    }


def _uma_records(
    conn: Any,
    decoded: Sequence[dict[str, Any]],
    adapter_map: Mapping[str, tuple[str, str]],
    neg_map: Mapping[str, tuple[str, str, str]],
) -> list[dict[str, Any]]:
    gamma_ids = [row["gamma"] for row in decoded]
    question_ids: list[str] = []
    enriched: list[tuple[dict[str, Any], str, str, str]] = []
    for row in decoded:
        adapter_question, mapped_adapter = adapter_map.get(row["ancillary_hex"], ("", ""))
        translated = neg_map.get(adapter_question, ("", "", ""))[0] if mapped_adapter in NEG_RISK_ADAPTERS else ""
        lookup_question = translated or adapter_question
        question_ids.append(lookup_question)
        enriched.append((row, adapter_question, mapped_adapter, lookup_question))
    markets = _load_markets(
        conn,
        gamma_ids=gamma_ids,
        question_ids=question_ids,
        condition_ids=question_ids,
    )
    records: list[dict[str, Any]] = []
    for row, adapter_question, mapped_adapter, lookup_question in enriched:
        args = row["args"]
        market, matched_by = _bridge(markets, row["gamma"], lookup_question)
        requester = _address(args.get("requester"))
        proposer = _address(args.get("proposer"))
        disputer = _address(args.get("disputer"))
        status = row["status"]
        records.append({
            "tx_hash": row["tx_hash"],
            "log_index": row["log_index"],
            "block_number": row["block_number"],
            "event_time": row["event_time"],
            "event_status": status,
            "external_market_id": row["gamma"],
            "market_id": market.get("id") if market else None,
            "market_title": str(market.get("title") or "") if market else row["title"],
            "source_adapter": mapped_adapter or requester,
            "source_oracle": row["source_oracle"],
            "adapter_question_id": adapter_question,
            "matched_by": matched_by,
            "question_id": str(market.get("question_id") or lookup_question or "") if market else lookup_question,
            "condition_id": str(market.get("condition_id") or "") if market else "",
            "string_raw": row["raw"],
            "p1": row["p1"],
            "p2": row["p2"],
            "proposed_price": _price(args.get("proposedPrice")) if status in {"propose", "dispute"} else "",
            "settled_price": _price(args.get("price")) if status == "settle" else "",
            "settlement_recipient": proposer if status == "settle" else "",
            "payout": str(args.get("payout")) if status == "settle" and args.get("payout") is not None else "",
            "requester": requester,
            "proposer": proposer,
            "disputer": disputer,
            "request_transaction": row["tx_hash"] if status == "request" else "",
            "proposal_transaction": row["tx_hash"] if status == "propose" else "",
            "settlement_transaction": row["tx_hash"] if status == "settle" else "",
        })
    return records


def _updown_records(
    conn: Any,
    logs: Sequence[Mapping[str, Any]],
    times: Mapping[int, datetime],
) -> list[dict[str, Any]]:
    decoded: list[tuple[Mapping[str, Any], str, dict[str, Any]]] = []
    conditions: list[str] = []
    for log in logs:
        status, abi = CTF_BY_TOPIC[_topic0(log)]
        args = _decode(abi, log)
        question = _hex(args["questionId"])[:66]
        condition = _hex(args["conditionId"])[:66]
        conditions.append(condition)
        decoded.append((log, status, args))
    markets = _load_markets(conn, condition_ids=conditions)
    records: list[dict[str, Any]] = []
    for log, status, args in decoded:
        question = _hex(args["questionId"])[:66]
        condition = _hex(args["conditionId"])[:66]
        market, matched_by = _bridge(markets, "", condition)
        # The condition is the payout identity. Question IDs can be reused by
        # different oracles; metadata availability must never filter chain logs.
        tx_hash = _hex(log.get("transactionHash"))
        oracle = _address(args.get("oracle"))
        payouts = args.get("payoutNumerators") or []
        records.append({
            "tx_hash": tx_hash,
            "log_index": _int(log.get("logIndex", 0)),
            "block_number": _int(log.get("blockNumber", 0)),
            "event_time": times[_int(log.get("blockNumber", 0))],
            "event_status": status,
            "external_market_id": str(market.get("gamma_market_id") or "") if market else "",
            "market_id": market.get("id") if market else None,
            "market_title": str(market.get("title") or "") if market else "",
            "source_adapter": oracle,
            "source_oracle": CTF,
            "adapter_question_id": question,
            "matched_by": "by_ctf_condition_id" if market else "",
            "question_id": question,
            "condition_id": condition,
            "string_raw": json.dumps({
                "oracle": oracle,
                "outcome_slot_count": int(args.get("outcomeSlotCount", 0)),
                "payouts": [int(value) for value in payouts],
            }, sort_keys=True, separators=(",", ":")),
            "p1": "", "p2": "", "proposed_price": "", "settled_price": "",
            "settlement_recipient": "",
            "payout": json.dumps([int(value) for value in payouts]) if payouts else "",
            "requester": oracle if status == "request" else "",
            "proposer": "", "disputer": "",
            "request_transaction": tx_hash if status == "request" else "",
            "proposal_transaction": "",
            "settlement_transaction": tx_hash if status == "settle" else "",
        })
    return records


def collect_window(
    rpc: RpcClient,
    conn: Any,
    start: int,
    end: int,
    *,
    dry_run: bool = False,
    uma_only: bool = False,
) -> dict[str, int]:
    current_adapters = [address for address in ADAPTER_STARTS if address != OLD_ADAPTER]
    current_topics = (CURRENT_INIT_TOPIC, RESET_TOPIC, CURRENT_RESOLVED_TOPIC)
    old_topics = (OLD_INIT_TOPIC, RESET_TOPIC, OLD_RESOLVED_TOPIC, OLD_SETTLED_TOPIC)
    adapter_logs = _fetch(rpc, start, end, current_adapters, current_topics, ADAPTER_STARTS)
    adapter_logs += _fetch(rpc, start, end, (OLD_ADAPTER,), old_topics, ADAPTER_STARTS)
    prepared_logs = _fetch(rpc, start, end, NEG_RISK_OPERATORS, (PREPARED_TOPIC,))
    oracle_logs = _fetch(rpc, start, end, ORACLE_STARTS, UMA_BY_TOPIC, ORACLE_STARTS)
    ctf_logs = [] if uma_only else _fetch(rpc, start, end, (CTF,), CTF_BY_TOPIC)
    all_logs = [*adapter_logs, *prepared_logs, *oracle_logs, *ctf_logs]
    times = _timestamps(rpc, all_logs)

    adapter_rows = [
        row for log in adapter_logs
        if (row := _decode_adapter(log, times[_int(log["blockNumber"])])) is not None
    ]
    prepared_rows = [_decode_prepared(log) for log in prepared_logs]
    rejected = sum(1 for log in oracle_logs if not _authorized(log))
    authorized = [log for log in oracle_logs if _authorized(log)]
    uma_decoded = [
        _decode_uma(log, times[_int(log["blockNumber"])])
        for log in authorized
    ]
    ancillary_keys = [row["ancillary_hex"] for row in adapter_rows if row["ancillary_hex"]]
    ancillary_keys.extend(row["ancillary_hex"] for row in uma_decoded)

    if not dry_run:
        _write_adapter_events(conn, adapter_rows)
        _refresh_adapter_projection(conn, ancillary_keys)
        _write_neg_map(conn, prepared_rows)

    existing_adapter = _load_adapter_map(conn, ancillary_keys)
    recent_adapter = {
        row["ancillary_hex"]: (row["question_id"], row["source_adapter"])
        for row in adapter_rows if row["ancillary_hex"]
    }
    adapter_map = {**recent_adapter, **existing_adapter}
    request_ids = [value[0] for value in adapter_map.values() if value[0]]
    existing_neg = _load_neg_map(conn, request_ids)
    recent_neg = {
        row["request_id"]: (row["question_id"], row["market_id"], row["source_operator"])
        for row in prepared_rows
    }
    for request_id, value in recent_neg.items():
        if request_id in existing_neg and existing_neg[request_id][0] != value[0]:
            raise RuntimeError(f"conflicting QuestionPrepared request id {request_id}")
    neg_map = {**recent_neg, **existing_neg}

    oracle_rows = _uma_records(conn, uma_decoded, adapter_map, neg_map)
    updown_rows = _updown_records(conn, ctf_logs, times)

    lifecycle_qids = [
        neg_map.get(row["question_id"], (row["question_id"], "", ""))[0]
        if row["source_adapter"] in NEG_RISK_ADAPTERS else row["question_id"]
        for row in adapter_rows
    ]
    lifecycle_markets = _load_markets(
        conn, question_ids=lifecycle_qids, condition_ids=lifecycle_qids,
    )
    for row, lookup in zip(adapter_rows, lifecycle_qids):
        market, _ = _bridge(lifecycle_markets, "", lookup)
        if market:
            row["market_id"] = market.get("id")
            row["condition_id"] = str(market.get("condition_id") or "")

    if not dry_run:
        # Re-upsert lifecycle rows after deterministic market enrichment.
        _write_adapter_events(conn, adapter_rows)
        _write_oracle_events(conn, [*oracle_rows, *updown_rows])

    from market_data import oracle_modules

    module_stats = {"module_events": 0} if uma_only else oracle_modules.collect_window(
        rpc, conn, start, end, dry_run=dry_run,
    )

    return {
        "from_block": start,
        "to_block": end,
        "logs_scanned": len(all_logs),
        "adapter_events": len(adapter_rows),
        "neg_risk_mappings": len(prepared_rows),
        "uma_events": len(oracle_rows),
        "rejected_non_polymarket_uma": rejected,
        "updown_events": len(updown_rows),
        "ctf_events": len(updown_rows),
        "module_events": module_stats["module_events"],
    }


def _state_keys(mode: str, *, uma_only: bool = False) -> tuple[str, ...] | None:
    if mode == "live":
        return UMA_LIVE_KEY, UPDOWN_LIVE_KEY
    if mode == "backfill":
        return (UMA_BACKFILL_KEY,) if uma_only else (UMA_BACKFILL_KEY, UPDOWN_BACKFILL_KEY)
    return None


def _state_block(conn: Any, key: str) -> int | None:
    state = get_state(conn, key)
    return int(state["last_block"]) if state and state.get("last_block") is not None else None


def _start_block(
    conn: Any,
    *,
    mode: str,
    target: int,
    explicit: int | None,
    rewind: int,
    uma_only: bool = False,
) -> int:
    if explicit is not None:
        return explicit
    keys = _state_keys(mode, uma_only=uma_only)
    if not keys:
        raise ValueError("bounded mode requires --from-block")
    cursors = [value for key in keys if (value := _state_block(conn, key)) is not None]
    if cursors:
        next_block = min(cursors) + 1
        return max(FULL_HISTORY_START, next_block - (rewind if mode == "live" else 0))
    if mode == "backfill":
        return FULL_HISTORY_START
    return max(FULL_HISTORY_START, target - 500_000 + 1)


def _advance(conn: Any, key: str, block: int, stats: Mapping[str, int]) -> None:
    current = _state_block(conn, key)
    if current is not None and current > block:
        return
    set_state(conn, key, value=dict(stats), last_block=block, monotonic_block=True)


def run(
    *,
    mode: str = "live",
    from_block: int | None = None,
    to_block: int | None = None,
    confirmations: int = CONFIRMATIONS,
    rewind: int = REWIND_BLOCKS,
    window_blocks: int = 1_000,
    max_windows: int = 10,
    dry_run: bool = False,
    uma_only: bool = False,
    rpc: RpcClient | None = None,
    conn: Any = None,
) -> dict[str, int]:
    if mode not in {"live", "bounded", "backfill", "reconcile"}:
        raise ValueError(f"unsupported oracle mode: {mode}")
    if uma_only and mode not in {"bounded", "backfill"}:
        raise ValueError("uma-only is restricted to bounded audits and historical backfill")
    if mode == "bounded" and (from_block is None or to_block is None):
        raise ValueError("bounded mode requires --from-block and --to-block")
    if window_blocks < 1 or max_windows < 0:
        raise ValueError("window-blocks must be positive and max-windows cannot be negative")
    if mode == "reconcile":
        database = conn or connect_postgres()
        totals = {"oracle_linked": 0, "adapter_linked": 0, "batches": 0}
        try:
            while max_windows == 0 or totals["batches"] < max_windows:
                stats = reconcile_market_links(database)
                if dry_run:
                    database.rollback()
                else:
                    database.commit()
                totals["batches"] += 1
                for key in ("oracle_linked", "adapter_linked"):
                    totals[key] += stats[key]
                if dry_run or stats["pass_complete"]:
                    break
            return totals
        except Exception:
            database.rollback()
            raise
        finally:
            if conn is None:
                database.close()
    own_rpc = rpc or RpcClient(polygon_rpc_urls("oracle"))
    chain_id = _int(own_rpc.call("eth_chainId"))
    if chain_id != POLYGON_CHAIN_ID:
        raise RuntimeError(f"Oracle collector requires Polygon chain_id=137, observed={chain_id}")
    target = to_block if to_block is not None else max(0, own_rpc.head() - max(0, confirmations))
    own_conn = conn is None
    database = conn or connect_postgres()
    try:
        if not dry_run:
            _ensure_schema(database)
            from market_data.oracle_modules import ensure_schema as ensure_module_schema, observe_implementations

            if not uma_only:
                ensure_module_schema(database)
                if mode == "live":
                    observe_implementations(own_rpc, database, target)
            database.commit()
        start = _start_block(
            database, mode=mode, target=target, explicit=from_block, rewind=max(0, rewind),
            uma_only=uma_only,
        )
        totals = {
            "from_block": start, "to_block": min(start - 1, target), "windows": 0,
            "logs_scanned": 0, "adapter_events": 0, "neg_risk_mappings": 0,
            "uma_events": 0, "rejected_non_polymarket_uma": 0, "updown_events": 0,
            "ctf_events": 0,
            "module_events": 0,
        }
        current = start
        while current <= target and (max_windows == 0 or totals["windows"] < max_windows):
            window_end = min(target, current + window_blocks - 1)
            try:
                stats = collect_window(
                    own_rpc, database, current, window_end, dry_run=dry_run, uma_only=uma_only,
                )
                if not dry_run:
                    keys = _state_keys(mode, uma_only=uma_only)
                    if keys:
                        for key in keys:
                            _advance(database, key, window_end, stats)
                        if mode == "live":
                            # Existing Polymonitor health readers still prefer
                            # this key.  It is a compatibility projection only;
                            # _state_keys deliberately excludes it from resume
                            # decisions so one stale legacy row cannot move the
                            # acquisition start point.
                            _advance(database, UMA_LIVENESS_KEY, window_end, stats)
                            _advance(database, MODULES_LIVE_KEY, window_end, stats)
                    database.commit()
            except Exception:
                database.rollback()
                raise
            totals["windows"] += 1
            totals["to_block"] = window_end
            for key in (
                "logs_scanned", "adapter_events", "neg_risk_mappings", "uma_events",
                "rejected_non_polymarket_uma", "updown_events",
                "ctf_events",
                "module_events",
            ):
                totals[key] += stats[key]
            current = window_end + 1
        if not dry_run:
            totals.update(reconcile_market_links(database))
            database.commit()
        return totals
    finally:
        if own_conn:
            database.close()


def add_cli(parser: Any) -> None:
    parser.add_argument("--mode", choices=("live", "bounded", "backfill", "reconcile"), default="live")
    parser.add_argument("--from-block", type=int)
    parser.add_argument("--to-block", type=int)
    parser.add_argument("--confirmations", type=int, default=CONFIRMATIONS)
    parser.add_argument("--rewind", type=int, default=REWIND_BLOCKS)
    parser.add_argument("--window-blocks", type=int, default=1_000)
    parser.add_argument("--max-windows", type=int, default=10, help="0 processes through the target")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rpc-url", help="optional endpoint for this collector job only")
    parser.add_argument("--uma-only", action="store_true", help="backfill/audit only UMA, adapters and NegRisk bridges")
    parser.set_defaults(handler=_cli)


def _cli(args: Any) -> int:
    if args.watch and args.mode != "live":
        raise SystemExit("--watch is only valid with --mode live")
    if args.watch and args.dry_run:
        raise SystemExit("--watch cannot be combined with --dry-run")

    def once() -> None:
        print(json.dumps(run(
            mode=args.mode,
            from_block=args.from_block,
            to_block=args.to_block,
            confirmations=args.confirmations,
            rewind=args.rewind,
            window_blocks=args.window_blocks,
            max_windows=args.max_windows,
            dry_run=args.dry_run,
            uma_only=args.uma_only,
            rpc=RpcClient((args.rpc_url,)) if args.rpc_url else None,
        ), sort_keys=True))

    if args.watch:
        watch(once, interval=args.interval, error_interval=300.0)
    else:
        once()
    return 0
