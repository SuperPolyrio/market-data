"""The small shared storage boundary used by all three collectors."""

from __future__ import annotations

import json
from typing import Any, Iterable, Mapping, Optional

import psycopg
from psycopg.rows import dict_row

from market_data.config import PostgresConfig


def connect_postgres(config: Optional[PostgresConfig] = None) -> psycopg.Connection:
    cfg = config or PostgresConfig.from_env()
    if not cfg.password:
        raise RuntimeError("POLYDATA_POSTGRES_PASSWORD is required")
    conn = psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        dbname=cfg.database,
        row_factory=dict_row,
    )
    conn.execute("SET search_path TO core, oracle, ops, public")
    return conn


def ensure_sync_state(conn: psycopg.Connection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.sync_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            last_block BIGINT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def get_state(conn: psycopg.Connection, key: str) -> Optional[dict[str, Any]]:
    ensure_sync_state(conn)
    row = conn.execute(
        "SELECT key, value, last_block, updated_at FROM ops.sync_state WHERE key=%s",
        (key,),
    ).fetchone()
    return dict(row) if row else None


def set_state(
    conn: psycopg.Connection,
    key: str,
    *,
    value: Any = None,
    last_block: Optional[int] = None,
    monotonic_block: bool = True,
) -> None:
    ensure_sync_state(conn)
    encoded = value if isinstance(value, str) or value is None else json.dumps(value, sort_keys=True)
    if monotonic_block and last_block is not None:
        current = get_state(conn, key)
        if current and current.get("last_block") is not None and int(current["last_block"]) > last_block:
            raise RuntimeError(
                f"refusing to regress {key} from {current['last_block']} to {last_block}"
            )
    conn.execute(
        """
        INSERT INTO ops.sync_state(key, value, last_block, updated_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value=EXCLUDED.value,
            last_block=COALESCE(EXCLUDED.last_block, ops.sync_state.last_block),
            updated_at=CURRENT_TIMESTAMP
        """,
        (key, encoded, last_block),
    )


def execute_values(
    conn: psycopg.Connection,
    sql: str,
    rows: Iterable[Mapping[str, Any]],
) -> int:
    materialized = list(rows)
    if not materialized:
        return 0
    with conn.cursor() as cursor:
        cursor.executemany(sql, materialized)
    return len(materialized)
