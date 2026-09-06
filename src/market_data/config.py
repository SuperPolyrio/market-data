"""Explicit environment configuration with no sibling-repository fallbacks."""

from __future__ import annotations

import os
from dataclasses import dataclass


def env(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default)).strip().strip('"').strip("'")


def env_int(name: str, default: int) -> int:
    raw = env(name)
    return int(raw) if raw else default


def split_urls(raw: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))


@dataclass(frozen=True)
class PostgresConfig:
    host: str = "127.0.0.1"
    port: int = 45432
    user: str = "poly_user"
    password: str = ""
    database: str = "poly_data_core"

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        return cls(
            host=env("POLYDATA_POSTGRES_HOST", "127.0.0.1"),
            port=env_int("POLYDATA_POSTGRES_PORT", 45432),
            user=env("POLYDATA_POSTGRES_USER", "poly_user"),
            password=env("POLYDATA_POSTGRES_PASSWORD"),
            database=env("POLYDATA_POSTGRES_DATABASE", "poly_data_core"),
        )


@dataclass(frozen=True)
class ClickHouseConfig:
    url: str = ""
    container: str = "polydata_clickhouse_orderfilled"
    database: str = "poly_orderfilled"
    user: str = "poly_user"
    password: str = ""

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        return cls(
            url=env("POLYDATA_ORDERFILLED_CLICKHOUSE_HTTP_URL"),
            container=env(
                "POLYDATA_ORDERFILLED_CLICKHOUSE_CONTAINER",
                "polydata_clickhouse_orderfilled",
            ),
            database=env("POLYDATA_ORDERFILLED_CLICKHOUSE_DATABASE", "poly_orderfilled"),
            user=env("POLYDATA_ORDERFILLED_CLICKHOUSE_USER", "poly_user"),
            password=env("CLICKHOUSE_PASSWORD"),
        )


def polygon_rpc_urls(role: str = "") -> tuple[str, ...]:
    prefix = f"POLYDATA_{role.upper()}_RPC_URLS" if role else ""
    role_urls = split_urls(env(prefix)) if prefix else ()
    role_fallback = split_urls(env(f"POLYDATA_{role.upper()}_FALLBACK_RPC_URLS")) if role else ()
    if role and not role_fallback:
        role_fallback = split_urls(env(f"POLYDATA_{role.upper()}_FALLBACK_RPC_URL"))
    shared = split_urls(env("POLYDATA_POLYGON_RPC_URLS"))
    shared_fallback = split_urls(env("POLYDATA_POLYGON_FALLBACK_RPC_URLS"))
    legacy = split_urls(env("POLYMARKET_RPC_URL"))
    urls = tuple(
        dict.fromkeys((*role_urls, *shared, *legacy, *role_fallback, *shared_fallback))
    )
    if not urls:
        raise RuntimeError(
            "Polygon RPC is required; set POLYDATA_POLYGON_RPC_URLS or POLYMARKET_RPC_URL"
        )
    return urls


GAMMA_API_BASE = env("POLYDATA_GAMMA_API_BASE", "https://gamma-api.polymarket.com").rstrip("/")
