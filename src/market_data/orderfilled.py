"""Acquire Polymarket OrderFilled logs and project only BUY/SELL facts.

The durable boundary is deliberately small: finalized Polygon logs are written
to PostgreSQL before any market lookup or ClickHouse write.  An unknown token
is a registry gap, never a reason to invent a market or hold back the raw
cursor.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Iterable, Mapping, Sequence

import requests
from psycopg.types.json import Jsonb

from market_data.config import ClickHouseConfig, polygon_rpc_urls
from market_data.rpc import RpcClient, watch
from market_data.storage import connect_postgres, get_state, set_state


LEGACY_EXCHANGES = (
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
)
V2026_EXCHANGES = (
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
    "0xe3333700ca9d93003f00f0f71f8515005f6c00aa",
)
EXCHANGES = LEGACY_EXCHANGES + V2026_EXCHANGES

LEGACY_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
V2026_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"

RAW_CURSOR = "orderfilled_raw_cursor"
FACT_CURSOR = "orderfilled_fact_cursor"
CONFIRMATIONS = 20
USDC_SCALE = Decimal(1_000_000)
SIDE_CODE = {"BUY": 1, "SELL": 2}
OUTCOME_CODE = {0: 1, 1: 2}


class DecodeError(ValueError):
    """A log claims to be OrderFilled but does not match its canonical ABI."""


def _hex(value: Any, *, width: int | None = None, prefix: bool = True) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        body = bytes(value).hex()
    else:
        text = str(value or "").strip().lower()
        body = text[2:] if text.startswith("0x") else text
    if width:
        body = body.zfill(width)
    return ("0x" if prefix else "") + body


def _quantity(value: Any) -> int:
    if isinstance(value, int):
        return value
    text = str(value or "0")
    return int(text, 16) if text.startswith(("0x", "0X")) else int(text)


def _token_hex(value: Any) -> str:
    """ClickHouse stores uint256 token ids as 64 lower-case hex characters."""

    text = str(value or "").strip()
    if text.startswith(("0x", "0X")):
        numeric = int(text, 16)
    elif text.isdigit():
        numeric = int(text, 10)
    else:
        raise ValueError(f"token id is neither decimal nor hexadecimal: {text!r}")
    if numeric < 0 or numeric >= 2**256:
        raise ValueError("token id does not fit uint256")
    return f"{numeric:064x}"


def _words(data: Any) -> list[bytes]:
    body = _hex(data, prefix=False)
    if len(body) % 64:
        raise DecodeError("OrderFilled data is not a sequence of 32-byte words")
    try:
        return [bytes.fromhex(body[offset : offset + 64]) for offset in range(0, len(body), 64)]
    except ValueError as exc:
        raise DecodeError("OrderFilled data is not hexadecimal") from exc


def _topic_address(topic: Any) -> str:
    body = _hex(topic, width=64, prefix=False)
    return "0x" + body[-40:]


def _price_and_size(usdc_amount: int, token_amount: int) -> tuple[str, str]:
    if token_amount <= 0:
        raise DecodeError("OrderFilled token amount must be positive")
    price = (Decimal(usdc_amount) / Decimal(token_amount)).quantize(
        Decimal("0.0000000001"), rounding=ROUND_DOWN
    )
    size = Decimal(token_amount) / USDC_SCALE
    return format(price, "f"), format(size, "f")


def decode_log(log: Mapping[str, Any]) -> dict[str, Any]:
    """Fast-decode either canonical static OrderFilled ABI.

    No generic ABI machinery is needed: all indexed fields are topics and all
    remaining fields are fixed 32-byte words.
    """

    topics = list(log.get("topics") or ())
    if len(topics) != 4:
        raise DecodeError("OrderFilled requires exactly four topics")
    address = _hex(log.get("address"), width=40).lower()
    topic0 = _hex(topics[0], width=64).lower()
    if topic0 == LEGACY_TOPIC and address in LEGACY_EXCHANGES:
        version = "legacy"
        expected_words = 5
    elif topic0 == V2026_TOPIC and address in V2026_EXCHANGES:
        version = "v2026"
        expected_words = 7
    else:
        raise DecodeError("unsupported OrderFilled topic/address pair")

    words = _words(log.get("data"))
    if len(words) != expected_words:
        raise DecodeError(f"{version} OrderFilled expects {expected_words} data words")
    values = [int.from_bytes(word, "big") for word in words]

    if version == "legacy":
        maker_asset, taker_asset, maker_amount, taker_amount, fee = values
        if maker_asset == 0 and taker_asset != 0:
            side, token_id = "BUY", taker_asset
            usdc_amount, token_amount = maker_amount, taker_amount
        elif taker_asset == 0 and maker_asset != 0:
            side, token_id = "SELL", maker_asset
            usdc_amount, token_amount = taker_amount, maker_amount
        else:
            raise DecodeError("legacy OrderFilled must pair one token with collateral asset 0")
        maker_order_hash = taker_order_hash = None
    else:
        side_value, token_id, amount0, amount1, fee = values[:5]
        if token_id == 0 or side_value not in (0, 1):
            raise DecodeError("v2026 OrderFilled has an invalid side or assetId")
        if side_value == 0:
            side = "BUY"
            maker_asset, taker_asset = 0, token_id
            maker_amount, taker_amount = amount0, amount1
            usdc_amount, token_amount = amount0, amount1
        else:
            side = "SELL"
            maker_asset, taker_asset = token_id, 0
            maker_amount, taker_amount = amount0, amount1
            token_amount, usdc_amount = amount0, amount1
        maker_order_hash = "0x" + words[5].hex()
        taker_order_hash = "0x" + words[6].hex()

    price, size = _price_and_size(usdc_amount, token_amount)
    decoded = {
        "contract": address,
        "event_version": version,
        "event_topic": topic0,
        "tx_hash": _hex(log.get("transactionHash"), width=64).lower(),
        "log_index": _quantity(log.get("logIndex")),
        "block_number": _quantity(log.get("blockNumber")),
        "order_hash": _hex(topics[1], width=64).lower(),
        "maker": _topic_address(topics[2]).lower(),
        "taker": _topic_address(topics[3]).lower(),
        "maker_asset_id": str(maker_asset),
        "taker_asset_id": str(taker_asset),
        "token_id": str(token_id),
        "side": side,
        "price": price,
        "size": size,
        "maker_amount": str(maker_amount),
        "taker_amount": str(taker_amount),
        "fee": str(fee),
    }
    if maker_order_hash is not None:
        decoded["maker_order_hash"] = maker_order_hash
        decoded["taker_order_hash"] = taker_order_hash
    return decoded


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS core.orderfilled_raw (
    id BIGSERIAL PRIMARY KEY,
    contract TEXT NOT NULL,
    event_version TEXT NOT NULL,
    event_topic TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_time TIMESTAMPTZ,
    order_hash TEXT,
    maker TEXT NOT NULL,
    taker TEXT NOT NULL,
    maker_asset_id TEXT NOT NULL,
    taker_asset_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price TEXT NOT NULL,
    size TEXT NOT NULL,
    maker_amount TEXT NOT NULL,
    taker_amount TEXT NOT NULL,
    fee TEXT NOT NULL,
    raw_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (contract, tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_orderfilled_raw_block_log
    ON core.orderfilled_raw (block_number, log_index);
CREATE INDEX IF NOT EXISTS idx_orderfilled_raw_token_block
    ON core.orderfilled_raw (token_id, block_number);

CREATE TABLE IF NOT EXISTS core.orderfilled_sync_windows (
    id BIGSERIAL PRIMARY KEY,
    from_block BIGINT NOT NULL,
    to_block BIGINT NOT NULL,
    exchange_set TEXT NOT NULL DEFAULT 'known_orderfilled',
    chain_log_count BIGINT NOT NULL DEFAULT 0,
    db_log_count BIGINT NOT NULL DEFAULT 0,
    missing_count BIGINT NOT NULL DEFAULT 0,
    repaired_count BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    audited_at TIMESTAMPTZ,
    repaired_at TIMESTAMPTZ,
    last_error TEXT,
    UNIQUE (from_block, to_block, exchange_set)
);

CREATE TABLE IF NOT EXISTS ops.orderfilled_registry_gaps (
    token_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'open',
    first_seen_block BIGINT,
    last_seen_block BIGINT,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    first_tx_hash TEXT,
    last_tx_hash TEXT,
    first_log_index BIGINT,
    last_log_index BIGINT,
    seen_count BIGINT NOT NULL DEFAULT 0,
    sample_market_id BIGINT,
    resolved_market_id BIGINT,
    resolved_condition_id TEXT,
    resolution_source TEXT,
    note TEXT,
    resolved_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_schema(conn: Any) -> None:
    for statement in SCHEMA_SQL.split(";"):
        if statement.strip():
            conn.execute(statement)
    conn.commit()


def _raw_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    return {**payload, "raw_json": Jsonb(payload, dumps=lambda value: json.dumps(value, default=str))}


RAW_INSERT_SQL = """
INSERT INTO core.orderfilled_raw (
    contract, event_version, event_topic, tx_hash, log_index, block_number,
    block_time, order_hash, maker, taker, maker_asset_id, taker_asset_id, token_id, side,
    price, size, maker_amount, taker_amount, fee, raw_json
) VALUES (
    %(contract)s, %(event_version)s, %(event_topic)s, %(tx_hash)s, %(log_index)s,
    %(block_number)s, %(block_time)s, %(order_hash)s, %(maker)s, %(taker)s, %(maker_asset_id)s,
    %(taker_asset_id)s, %(token_id)s, %(side)s, %(price)s, %(size)s,
    %(maker_amount)s, %(taker_amount)s, %(fee)s, %(raw_json)s
) ON CONFLICT (contract, tx_hash, log_index) DO NOTHING
"""


def event_key(row: Mapping[str, Any]) -> tuple[str, str, int]:
    return (
        _hex(row.get("contract") or row.get("address"), width=40).lower(),
        _hex(row.get("tx_hash") or row.get("transactionHash"), width=64).lower(),
        _quantity(row.get("log_index") if "log_index" in row else row.get("logIndex")),
    )


def compare_raw_keys(
    chain_rows: Iterable[Mapping[str, Any]], sink_rows: Iterable[Mapping[str, Any]]
) -> dict[str, int | bool]:
    chain_list = [event_key(row) for row in chain_rows]
    sink_list = [event_key(row) for row in sink_rows]
    chain, sink = set(chain_list), set(sink_list)
    return {
        "chain_unique_keys": len(chain),
        "sink_unique_keys": len(sink),
        "chain_duplicate_rows": len(chain_list) - len(chain),
        "sink_duplicate_rows": len(sink_list) - len(sink),
        "missing_keys": len(chain - sink),
        "extra_keys": len(sink - chain),
        "exact": len(chain_list) == len(chain) and len(sink_list) == len(sink) and chain == sink,
    }


def persist_and_audit_raw(
    conn: Any, rows: Sequence[Mapping[str, Any]], from_block: int, to_block: int
) -> dict[str, int | bool]:
    payloads = [_raw_payload(row) for row in rows]
    if payloads:
        with conn.cursor() as cursor:
            cursor.executemany(RAW_INSERT_SQL, payloads)
    sink_rows = conn.execute(
        """
        SELECT contract, tx_hash, log_index
        FROM core.orderfilled_raw
        WHERE block_number BETWEEN %s AND %s AND contract = ANY(%s)
        """,
        (from_block, to_block, list(EXCHANGES)),
    ).fetchall()
    result = compare_raw_keys(rows, sink_rows)
    error = None if result["exact"] else json.dumps(result, sort_keys=True)
    conn.execute(
        """
        INSERT INTO core.orderfilled_sync_windows (
            from_block, to_block, exchange_set, chain_log_count, db_log_count,
            missing_count, repaired_count, status, audited_at, repaired_at, last_error
        ) VALUES (%s, %s, 'known_orderfilled', %s, %s, %s, 0, %s, now(), now(), %s)
        ON CONFLICT (from_block, to_block, exchange_set) DO UPDATE SET
            chain_log_count=EXCLUDED.chain_log_count,
            db_log_count=EXCLUDED.db_log_count,
            missing_count=EXCLUDED.missing_count,
            status=EXCLUDED.status,
            audited_at=EXCLUDED.audited_at,
            repaired_at=EXCLUDED.repaired_at,
            last_error=EXCLUDED.last_error
        """,
        (
            from_block,
            to_block,
            result["chain_unique_keys"],
            result["sink_unique_keys"],
            result["missing_keys"],
            "complete" if result["exact"] else "incomplete",
            error,
        ),
    )
    conn.commit()
    return result


def load_raw_window(conn: Any, from_block: int, to_block: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT contract, event_version, event_topic, tx_hash, log_index,
               block_number, block_time, order_hash, maker, taker, maker_asset_id,
               taker_asset_id, token_id, side, price, size, maker_amount,
               taker_amount, fee
        FROM core.orderfilled_raw
        WHERE block_number BETWEEN %s AND %s AND contract = ANY(%s)
        ORDER BY block_number, log_index, tx_hash
        """,
        (from_block, to_block, list(EXCHANGES)),
    ).fetchall()
    return [dict(row) for row in rows]


def load_token_owners(conn: Any, token_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
    tokens = sorted(set(token_ids))
    if not tokens:
        return {}
    rows = conn.execute(
        """
        SELECT mt.token_id, mt.market_id, mt.condition_id, mt.outcome,
               mt.outcome_index, m.identity_kind
        FROM core.market_tokens AS mt
        JOIN core.markets AS m ON m.id = mt.market_id
        WHERE mt.token_id = ANY(%s)
          AND mt.active IS TRUE
          AND mt.market_id > 0
          AND COALESCE(mt.condition_id, '') <> ''
          AND lower(mt.condition_id) = lower(COALESCE(m.condition_id, ''))
          AND m.identity_kind NOT IN
              ('legacy_placeholder', 'legacy_ghost', 'invalid_shell')
          AND lower(COALESCE(m.category, '')) NOT IN
              ('orderfilled-placeholder', 'ghost', '[ghost]', 'invalid-identity-shell')
          AND lower(COALESCE(m.slug, '')) NOT LIKE 'trade-indexer-placeholder-%%'
          AND lower(COALESCE(m.slug, '')) NOT LIKE 'ghost-market-%%'
          AND lower(mt.condition_id) <> %s
        """,
        (tokens, "0x" + "0" * 64),
    ).fetchall()
    return {str(row["token_id"]): dict(row) for row in rows}


def build_projection(
    raw_rows: Sequence[Mapping[str, Any]], owners: Mapping[str, Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    facts: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for raw in raw_rows:
        token_id = str(raw["token_id"])
        owner = owners.get(token_id)
        outcome_index = int(owner["outcome_index"]) if owner else -1
        if not owner or outcome_index not in OUTCOME_CODE:
            unresolved.append(dict(raw))
            continue
        side = str(raw["side"]).upper()
        if side not in SIDE_CODE:
            raise RuntimeError(f"raw OrderFilled side is not BUY/SELL: {side!r}")
        facts.append(
            {
                "tx_hash": raw["tx_hash"],
                "log_index": raw["log_index"],
                "market_id": owner["market_id"],
                "condition_id": owner["condition_id"],
                "token_id": token_id,
                "outcome_code": (
                    0
                    if (
                        str(raw.get("contract") or "").lower() == V2026_EXCHANGES[-1]
                        or owner.get("identity_kind") == "official_combo"
                    )
                    else OUTCOME_CODE[outcome_index]
                ),
                "maker": raw["maker"],
                "taker": raw["taker"],
                "side_code": SIDE_CODE[side],
                "price": raw["price"],
                "size": raw["size"],
                "block_number": raw["block_number"],
                "order_hash": raw["order_hash"],
                "contract": raw["contract"],
                "maker_amount": raw["maker_amount"],
                "taker_amount": raw["taker_amount"],
                "fee": raw["fee"],
            }
        )
    return facts, unresolved


def queue_unresolved(conn: Any, rows: Sequence[Mapping[str, Any]]) -> None:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["token_id"]), []).append(row)
    for token_id, observations in grouped.items():
        first = min(observations, key=lambda row: (int(row["block_number"]), int(row["log_index"])))
        last = max(observations, key=lambda row: (int(row["block_number"]), int(row["log_index"])))
        conn.execute(
            """
            INSERT INTO ops.orderfilled_registry_gaps (
                token_id, status, first_seen_block, last_seen_block,
                first_tx_hash, last_tx_hash, first_log_index, last_log_index,
                seen_count, note, updated_at
            ) VALUES (%s, 'open', %s, %s, %s, %s, %s, %s, %s,
                      'raw preserved; waiting for core.market_tokens owner', now())
            ON CONFLICT (token_id) DO UPDATE SET
                status='open',
                first_seen_block=LEAST(ops.orderfilled_registry_gaps.first_seen_block,
                                       EXCLUDED.first_seen_block),
                last_seen_block=GREATEST(ops.orderfilled_registry_gaps.last_seen_block,
                                         EXCLUDED.last_seen_block),
                first_tx_hash=COALESCE(ops.orderfilled_registry_gaps.first_tx_hash,
                                       EXCLUDED.first_tx_hash),
                last_tx_hash=EXCLUDED.last_tx_hash,
                first_log_index=COALESCE(ops.orderfilled_registry_gaps.first_log_index,
                                         EXCLUDED.first_log_index),
                last_log_index=EXCLUDED.last_log_index,
                seen_count=GREATEST(ops.orderfilled_registry_gaps.seen_count,
                                    EXCLUDED.seen_count),
                note=EXCLUDED.note,
                updated_at=now()
            """,
            (
                token_id,
                first["block_number"],
                last["block_number"],
                first["tx_hash"],
                last["tx_hash"],
                first["log_index"],
                last["log_index"],
                len(observations),
            ),
        )


def mark_resolved(conn: Any, owners: Mapping[str, Mapping[str, Any]]) -> None:
    for token_id, owner in owners.items():
        conn.execute(
            """
            UPDATE ops.orderfilled_registry_gaps
            SET status='resolved', resolved_market_id=%s, resolved_condition_id=%s,
                resolution_source='core.market_tokens', resolved_at=now(), updated_at=now()
            WHERE token_id=%s AND status <> 'resolved'
            """,
            (owner["market_id"], owner["condition_id"], token_id),
        )


def _tsv(value: Any) -> str:
    if value is None:
        return r"\N"
    return str(value).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


@dataclass
class ClickHouseWriter:
    config: ClickHouseConfig

    def _table(self) -> str:
        # The MergeTree has no unique constraint.  Reading and writing the
        # single main table keeps exact-key retries idempotent even after a
        # legacy Buffer table flushes.
        return "orderfilled_fact"

    def _docker_command(self, query: str, *, stdin: bool) -> list[str]:
        command = ["docker", "exec"]
        if stdin:
            command.append("-i")
        command.extend(
            [
                self.config.container,
                "clickhouse-client",
                "--user",
                self.config.user,
                "--password",
                self.config.password,
                "--database",
                self.config.database,
                "--query",
                query,
            ]
        )
        return command

    def query(self, query: str) -> str:
        if self.config.url:
            response = requests.post(
                self.config.url,
                params={"database": self.config.database, "query": query},
                auth=(self.config.user, self.config.password),
                timeout=60,
            )
            response.raise_for_status()
            return response.text.strip()
        return subprocess.check_output(self._docker_command(query, stdin=False), text=True).strip()

    def insert(self, query: str, payload: str) -> None:
        if self.config.url:
            response = requests.post(
                self.config.url,
                params={"database": self.config.database},
                data=query + "\n" + payload,
                auth=(self.config.user, self.config.password),
                timeout=120,
            )
            response.raise_for_status()
            return
        subprocess.run(
            self._docker_command(query, stdin=True), input=payload, text=True, check=True
        )

    def existing_keys(self, from_block: int, to_block: int) -> set[tuple[str, int]]:
        output = self.query(
            f"SELECT lower(tx_hash), log_index FROM {self._table()} "
            f"WHERE block_number BETWEEN {int(from_block)} AND {int(to_block)} "
            "FORMAT TabSeparated"
        )
        return {
            (line.split("\t", 1)[0], int(line.split("\t", 1)[1]))
            for line in output.splitlines()
            if line.strip()
        }

    def write_facts(self, rows: Sequence[Mapping[str, Any]]) -> int:
        if not rows:
            return 0
        keys = [(_hex(row["tx_hash"], width=64, prefix=False), int(row["log_index"])) for row in rows]
        if len(keys) != len(set(keys)):
            raise RuntimeError("duplicate OrderFilled keys in one projection batch")
        existing: set[tuple[str, int]] = set()
        block_buckets: dict[int, list[int]] = {}
        for index, row in enumerate(rows):
            block_buckets.setdefault(int(row["block_number"]) // 2_000, []).append(index)
        for indexes in block_buckets.values():
            blocks = [int(rows[index]["block_number"]) for index in indexes]
            existing.update(self.existing_keys(min(blocks), max(blocks)))
        pending = [row for row, key in zip(rows, keys) if key not in existing]
        if not pending:
            return 0
        columns = (
            "tx_hash, log_index, market_id, condition_id, token_id, outcome_code, "
            "maker, taker, side_code, price, size, block_number, order_hash, contract, "
            "maker_amount, taker_amount, fee"
        )
        lines = []
        for row in pending:
            values = (
                _hex(row["tx_hash"], width=64, prefix=False),
                row["log_index"],
                row["market_id"],
                row["condition_id"],
                _token_hex(row["token_id"]),
                row["outcome_code"],
                _hex(row["maker"], width=40, prefix=False),
                _hex(row["taker"], width=40, prefix=False),
                row["side_code"],
                row["price"],
                row["size"],
                row["block_number"],
                _hex(row["order_hash"], width=64, prefix=False),
                str(row["contract"]).lower(),
                row["maker_amount"],
                row["taker_amount"],
                row["fee"],
            )
            lines.append("\t".join(_tsv(value) for value in values))
        self.insert(
            f"INSERT INTO {self._table()} ({columns}) FORMAT TabSeparated",
            "\n".join(lines) + "\n",
        )
        return len(pending)


def project_rows(
    conn: Any, writer: ClickHouseWriter, raw_rows: Sequence[Mapping[str, Any]]
) -> dict[str, int]:
    owners = load_token_owners(conn, (str(row["token_id"]) for row in raw_rows))
    facts, unresolved = build_projection(raw_rows, owners)
    queue_unresolved(conn, unresolved)
    inserted = writer.write_facts(facts)
    mark_resolved(conn, {str(row["token_id"]): owners[str(row["token_id"])] for row in facts})
    conn.commit()
    return {"raw": len(raw_rows), "facts": len(facts), "inserted": inserted, "unresolved": len(unresolved)}


def retry_resolved_gaps(
    conn: Any, writer: ClickHouseWriter, *, limit_tokens: int
) -> dict[str, int]:
    if limit_tokens <= 0:
        return {"tokens": 0, "raw": 0, "facts": 0, "inserted": 0, "unresolved": 0}
    candidates = conn.execute(
        """
        SELECT gap.token_id
        FROM ops.orderfilled_registry_gaps AS gap
        JOIN core.market_tokens AS mt ON mt.token_id = gap.token_id AND mt.active IS TRUE
        JOIN core.markets AS m ON m.id = mt.market_id
        WHERE gap.status='open'
          AND mt.market_id > 0
          AND mt.outcome_index IN (0, 1)
          AND COALESCE(mt.condition_id, '') <> ''
          AND lower(mt.condition_id) = lower(COALESCE(m.condition_id, ''))
          AND m.identity_kind NOT IN
              ('legacy_placeholder', 'legacy_ghost', 'invalid_shell')
          AND lower(COALESCE(m.category, '')) NOT IN
              ('orderfilled-placeholder', 'ghost', '[ghost]', 'invalid-identity-shell')
          AND lower(COALESCE(m.slug, '')) NOT LIKE 'trade-indexer-placeholder-%%'
          AND lower(COALESCE(m.slug, '')) NOT LIKE 'ghost-market-%%'
          AND lower(mt.condition_id) <> %s
        ORDER BY gap.first_seen_block, gap.token_id
        LIMIT %s
        """,
        ("0x" + "0" * 64, limit_tokens),
    ).fetchall()
    tokens = [str(row["token_id"]) for row in candidates]
    if not tokens:
        return {"tokens": 0, "raw": 0, "facts": 0, "inserted": 0, "unresolved": 0}
    rows = conn.execute(
        """
        SELECT contract, event_version, event_topic, tx_hash, log_index,
               block_number, block_time, order_hash, maker, taker, maker_asset_id,
               taker_asset_id, token_id, side, price, size, maker_amount,
               taker_amount, fee
        FROM core.orderfilled_raw
        WHERE token_id = ANY(%s)
        ORDER BY block_number, log_index, tx_hash
        """,
        (tokens,),
    ).fetchall()
    result = project_rows(conn, writer, [dict(row) for row in rows])
    return {"tokens": len(tokens), **result}


def resolve_open_combo_gaps(conn: Any, *, limit_tokens: int) -> dict[str, int]:
    """Install exact official owners for queued e333 combo assets."""

    empty = {"attempted_tokens": 0, "resolved_conditions": 0, "failed_conditions": 0}
    if limit_tokens <= 0:
        return empty
    rows = conn.execute(
        """
        SELECT DISTINCT ON (gap.token_id)
               raw.contract, raw.tx_hash, raw.token_id, raw.maker, raw.taker,
               raw.block_number, raw.block_time
        FROM ops.orderfilled_registry_gaps AS gap
        JOIN core.orderfilled_raw AS raw ON raw.token_id=gap.token_id
        WHERE gap.status='open' AND lower(raw.contract)=%s
        ORDER BY gap.token_id, raw.block_number, raw.log_index, raw.tx_hash
        LIMIT %s
        """,
        (V2026_EXCHANGES[-1], limit_tokens),
    ).fetchall()
    if not rows:
        return empty

    from market_data.combo import (
        ComboResolutionError,
        decode_combo_position,
        resolve_and_install,
    )

    grouped: dict[str, list[dict[str, Any]]] = {}
    failed = 0
    for raw in rows:
        item = dict(raw)
        try:
            condition = decode_combo_position(item["token_id"]).condition_id
        except ComboResolutionError:
            failed += 1
            continue
        grouped.setdefault(condition, []).append(item)

    resolved = 0
    for condition, observations in sorted(grouped.items()):
        try:
            receipt = resolve_and_install(conn, observations)
            conn.commit()
            resolved += int(receipt["conditions"])
        except Exception as exc:
            conn.rollback()
            failed += 1
            print(
                f"combo resolver deferred {condition}: {type(exc).__name__}: {str(exc)[:180]}",
                flush=True,
            )
    return {
        "attempted_tokens": len(rows),
        "resolved_conditions": resolved,
        "failed_conditions": failed,
    }


def advance_legacy_trade_cursor_if_complete(conn: Any) -> int | None:
    """Keep existing health consumers accurate during the repository cutover.

    ``trade_sync`` advances only when every queued token has a canonical owner.
    The new raw/fact cursors remain the acquisition authority.
    """

    row = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM ops.orderfilled_registry_gaps WHERE status='open') "
        "AS has_open"
    ).fetchone()
    if row and bool(row["has_open"]):
        return None
    fact_state = get_state(conn, FACT_CURSOR)
    if not fact_state or fact_state.get("last_block") is None:
        return None
    last_block = int(fact_state["last_block"])
    set_state(
        conn,
        "trade_sync",
        last_block=last_block,
        value={"source": FACT_CURSOR, "registry_gaps_open": 0},
    )
    conn.commit()
    return last_block


def catch_up_projection(
    conn: Any,
    writer: ClickHouseWriter,
    *,
    through_block: int,
    window_size: int,
    max_windows: int,
    fallback_start: int,
) -> list[dict[str, int]]:
    """Advance the fact cursor over already-durable raw rows.

    The cursor advances even when some rows are waiting on token metadata; the
    durable unresolved queue is the retry contract for those rows.
    """

    state = get_state(conn, FACT_CURSOR)
    cursor = int(state["last_block"]) + 1 if state and state.get("last_block") is not None else fallback_start
    results: list[dict[str, int]] = []
    while cursor <= through_block and len(results) < max_windows:
        to_block = min(through_block, cursor + window_size - 1)
        result = project_rows(conn, writer, load_raw_window(conn, cursor, to_block))
        set_state(conn, FACT_CURSOR, last_block=to_block, value=result)
        conn.commit()
        results.append({"from_block": cursor, "to_block": to_block, **result})
        cursor = to_block + 1
    return results


def fetch_decode_window(rpc: RpcClient, from_block: int, to_block: int) -> list[dict[str, Any]]:
    logs = rpc.logs(
        from_block,
        to_block,
        addresses=EXCHANGES,
        topics=[[LEGACY_TOPIC, V2026_TOPIC]],
    )
    decoded = [decode_log(log) for log in logs]
    block_times: dict[int, datetime] = {}
    for block_number in sorted({row["block_number"] for row in decoded}):
        block = rpc.block(block_number)
        timestamp = _quantity(block.get("timestamp"))
        block_times[block_number] = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    for row in decoded:
        row["block_time"] = block_times[row["block_number"]]
    decoded.sort(key=lambda row: (row["block_number"], row["log_index"], row["tx_hash"]))
    keys = [event_key(row) for row in decoded]
    if len(keys) != len(set(keys)):
        raise RuntimeError("RPC returned duplicate OrderFilled event keys")
    return decoded


def adopt_legacy_cursor(conn: Any) -> int:
    """Explicitly seed both new cursors from the proven legacy trade cursor."""

    if get_state(conn, RAW_CURSOR) is not None or get_state(conn, FACT_CURSOR) is not None:
        raise RuntimeError("cannot adopt legacy cursor after either new cursor exists")
    legacy = get_state(conn, "trade_sync")
    if not legacy or legacy.get("last_block") is None:
        raise RuntimeError("legacy ops.sync_state.trade_sync cursor is missing")
    last_block = int(legacy["last_block"])
    receipt = {"adopted_from": "trade_sync", "last_block": last_block}
    try:
        set_state(conn, RAW_CURSOR, last_block=last_block, value=receipt)
        set_state(conn, FACT_CURSOR, last_block=last_block, value=receipt)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return last_block


def run(args: argparse.Namespace) -> int:
    if args.confirmations < CONFIRMATIONS:
        raise SystemExit(f"--confirmations must be at least {CONFIRMATIONS}")

    # Cursor adoption is a one-time database migration, not a collection run.
    # Keeping it RPC-independent makes the cutover atomic and retryable: once
    # this returns, the service can start normally from the adopted boundary.
    if args.adopt_legacy_cursor:
        if args.from_block is not None or args.to_block is not None:
            raise SystemExit("--adopt-legacy-cursor cannot be combined with a block range")
        conn = connect_postgres()
        try:
            ensure_schema(conn)
            adopted = adopt_legacy_cursor(conn)
            print(json.dumps({"status": "adopted_legacy_cursor", "last_block": adopted}))
            return 0
        finally:
            conn.close()

    rpc = RpcClient(polygon_rpc_urls("orderfilled"))
    safe_head = rpc.head() - args.confirmations
    final_block = min(int(args.to_block), safe_head) if args.to_block is not None else safe_head

    # A bounded dry run needs no database connection and performs no DDL.
    if args.dry_run:
        if args.from_block is None:
            raise SystemExit("--dry-run requires --from-block")
        cursor = int(args.from_block)
        for _ in range(args.max_windows):
            if cursor > final_block:
                break
            window_end = min(final_block, cursor + args.window_size - 1)
            decoded = fetch_decode_window(rpc, cursor, window_end)
            print(
                json.dumps(
                    {
                        "status": "dry_run",
                        "from_block": cursor,
                        "to_block": window_end,
                        "logs": len(decoded),
                        "sample": decoded[: args.sample],
                    },
                    sort_keys=True,
                    default=str,
                )
            )
            cursor = window_end + 1
        return 0

    conn = connect_postgres()
    try:
        ensure_schema(conn)
        raw_state = get_state(conn, RAW_CURSOR)
        has_raw_cursor = bool(raw_state and raw_state.get("last_block") is not None)
        bounded_backfill = args.from_block is not None and not args.advance_cursor
        if has_raw_cursor and (args.from_block is None or args.advance_cursor):
            cursor = int(raw_state["last_block"]) + 1
        elif args.from_block is not None:
            cursor = int(args.from_block)
        else:
            raise SystemExit("first run requires --from-block; use --advance-cursor to seed live state")
        collection_start = cursor

        windows: list[dict[str, Any]] = []
        for _ in range(args.max_windows):
            if cursor > final_block:
                break
            window_end = min(final_block, cursor + args.window_size - 1)
            decoded = fetch_decode_window(rpc, cursor, window_end)
            audit = persist_and_audit_raw(conn, decoded, cursor, window_end)
            if not audit["exact"]:
                raise RuntimeError(f"raw window audit failed: {audit}")
            if not bounded_backfill:
                set_state(conn, RAW_CURSOR, last_block=window_end, value=audit)
                conn.commit()
            windows.append(
                {
                    "from_block": cursor,
                    "to_block": window_end,
                    "raw_logs": len(decoded),
                    "raw_audit": audit,
                }
            )
            cursor = window_end + 1

        writer = ClickHouseWriter(ClickHouseConfig.from_env())
        if bounded_backfill:
            for window in windows:
                window["projection"] = project_rows(
                    conn,
                    writer,
                    load_raw_window(conn, window["from_block"], window["to_block"]),
                )
        else:
            raw_last = int(get_state(conn, RAW_CURSOR)["last_block"])
            fact_state = get_state(conn, FACT_CURSOR)
            if fact_state and fact_state.get("last_block") is not None:
                fallback_start = int(fact_state["last_block"]) + 1
            else:
                first_window = conn.execute(
                    """
                    SELECT MIN(from_block) AS first_block
                    FROM core.orderfilled_sync_windows
                    WHERE status='complete' AND to_block <= %s
                    """,
                    (raw_last,),
                ).fetchone()
                fallback_start = int(first_window["first_block"] or collection_start)
            projected = catch_up_projection(
                conn,
                writer,
                through_block=raw_last,
                window_size=args.window_size,
                max_windows=args.max_windows,
                fallback_start=fallback_start,
            )
            combo_resolution = resolve_open_combo_gaps(
                conn, limit_tokens=args.retry_unresolved_tokens
            )
            retried = retry_resolved_gaps(
                conn, writer, limit_tokens=args.retry_unresolved_tokens
            )
            compatibility_cursor = advance_legacy_trade_cursor_if_complete(conn)
        for window in windows:
            print(json.dumps({"status": "complete", **window}, sort_keys=True))
        for result in projected if not bounded_backfill else ():
            print(json.dumps({"status": "projected", **result}, sort_keys=True))
        if not bounded_backfill and combo_resolution["attempted_tokens"]:
            print(
                json.dumps(
                    {"status": "resolved_official_combos", **combo_resolution},
                    sort_keys=True,
                )
            )
        if not bounded_backfill and retried["tokens"]:
            print(json.dumps({"status": "retried_unresolved", **retried}, sort_keys=True))
        if not bounded_backfill and compatibility_cursor is not None:
            print(
                json.dumps(
                    {"status": "compatibility_cursor", "trade_sync": compatibility_cursor},
                    sort_keys=True,
                )
            )
        if not windows and (bounded_backfill or not projected):
            print(json.dumps({"status": "caught_up", "cursor": cursor, "safe_head": safe_head}))
        return 0
    finally:
        conn.close()


def add_cli(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--from-block", type=int, help="inclusive bounded start; required to seed state")
    parser.add_argument("--to-block", type=int, help="inclusive bounded end, capped at finalized head")
    parser.add_argument("--max-windows", type=int, default=1)
    parser.add_argument("--window-size", type=int, default=2_000)
    parser.add_argument("--confirmations", type=int, default=CONFIRMATIONS)
    parser.add_argument("--sample", type=int, default=5)
    parser.add_argument("--retry-unresolved-tokens", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true", help="fetch/decode only; mutate no storage")
    parser.add_argument("--advance-cursor", action="store_true", help="advance live cursors for explicit ranges")
    parser.add_argument(
        "--adopt-legacy-cursor",
        action="store_true",
        help="one-time explicit cutover from ops.sync_state.trade_sync",
    )
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=5.0)

    def handler(args: argparse.Namespace) -> int:
        if (
            args.max_windows < 1
            or args.window_size < 1
            or args.sample < 0
            or args.retry_unresolved_tokens < 0
        ):
            raise SystemExit("window bounds must be positive and sample/retry limits cannot be negative")
        if args.watch and args.adopt_legacy_cursor:
            raise SystemExit("--adopt-legacy-cursor is a one-time command and cannot watch")
        if args.watch:
            watch(lambda: run(args), interval=args.interval)
            return 0
        return run(args)

    parser.set_defaults(handler=handler)


__all__ = [
    "EXCHANGES",
    "LEGACY_EXCHANGES",
    "V2026_EXCHANGES",
    "LEGACY_TOPIC",
    "V2026_TOPIC",
    "DecodeError",
    "decode_log",
    "adopt_legacy_cursor",
    "compare_raw_keys",
    "build_projection",
    "project_rows",
    "resolve_open_combo_gaps",
    "advance_legacy_trade_cursor_if_complete",
    "add_cli",
]
