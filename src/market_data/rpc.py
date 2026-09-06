"""Resilient Polygon JSON-RPC access shared by OrderFilled and Oracle."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _session() -> requests.Session:
    retry = Retry(
        # Endpoint failover is the resilience boundary.  Long per-endpoint
        # retries can hide a dead local node for minutes before the healthy
        # fallback is attempted.
        total=1,
        connect=1,
        read=1,
        status=1,
        backoff_factor=0.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(("POST",)),
        raise_on_status=False,
    )
    session = requests.Session()
    session.trust_env = False
    session.mount("http://", HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32))
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32))
    return session


@dataclass
class RpcClient:
    urls: tuple[str, ...]
    timeout: float = 30.0

    def __post_init__(self) -> None:
        if not self.urls:
            raise ValueError("at least one RPC URL is required")
        self.session = _session()
        self._next_id = 1
        self._preferred = 0

    def call(self, method: str, params: Optional[list[Any]] = None) -> Any:
        errors: list[str] = []
        for offset in range(len(self.urls)):
            index = (self._preferred + offset) % len(self.urls)
            url = self.urls[index]
            request_id = self._next_id
            self._next_id += 1
            try:
                response = self.session.post(
                    url,
                    json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or []},
                    timeout=self.timeout,
                )
                response.raise_for_status()
                payload = response.json()
                if payload.get("error"):
                    raise RuntimeError(str(payload["error"]))
                self._preferred = index
                return payload["result"]
            except Exception as exc:
                errors.append(f"endpoint[{index}] {type(exc).__name__}: {str(exc)[:180]}")
        raise ConnectionError(f"all Polygon RPC endpoints failed for {method}: {'; '.join(errors)}")

    def batch_call(
        self, method: str, params_batch: Iterable[list[Any]]
    ) -> list[Any]:
        params_list = list(params_batch)
        if not params_list:
            return []
        errors: list[str] = []
        for offset in range(len(self.urls)):
            index = (self._preferred + offset) % len(self.urls)
            url = self.urls[index]
            payload = []
            request_ids: list[int] = []
            for params in params_list:
                request_id = self._next_id
                self._next_id += 1
                request_ids.append(request_id)
                payload.append(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": method,
                        "params": params,
                    }
                )
            try:
                response = self.session.post(url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, list):
                    raise RuntimeError("RPC batch response is not a list")
                by_id = {item.get("id"): item for item in body if isinstance(item, dict)}
                results: list[Any] = []
                for request_id in request_ids:
                    item = by_id.get(request_id)
                    if item is None:
                        raise RuntimeError(f"RPC batch response omitted id {request_id}")
                    if item.get("error"):
                        raise RuntimeError(str(item["error"]))
                    results.append(item.get("result"))
                self._preferred = index
                return results
            except Exception as exc:
                errors.append(
                    f"endpoint[{index}] {type(exc).__name__}: {str(exc)[:180]}"
                )
        raise ConnectionError(
            f"all Polygon RPC endpoints failed for batch {method}: {'; '.join(errors)}"
        )

    def head(self) -> int:
        return int(self.call("eth_blockNumber"), 16)

    def block(self, number: int) -> dict[str, Any]:
        return self.call("eth_getBlockByNumber", [hex(number), False])

    def blocks(self, numbers: Iterable[int], *, batch_size: int = 100) -> list[dict[str, Any]]:
        requested = list(numbers)
        result: list[dict[str, Any]] = []
        for offset in range(0, len(requested), max(1, batch_size)):
            chunk = requested[offset : offset + max(1, batch_size)]
            result.extend(
                self.batch_call(
                    "eth_getBlockByNumber",
                    ([hex(number), False] for number in chunk),
                )
            )
        return result

    def logs(
        self,
        from_block: int,
        to_block: int,
        *,
        addresses: Iterable[str],
        topics: Optional[list[Any]] = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": list(addresses),
        }
        if topics is not None:
            query["topics"] = topics
        return list(self.call("eth_getLogs", [query]))


def watch(run_once, *, interval: float, error_interval: float = 300.0) -> None:
    while True:
        try:
            run_once()
            time.sleep(interval)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            print(f"collector error: {type(exc).__name__}: {exc}", flush=True)
            time.sleep(error_interval)
