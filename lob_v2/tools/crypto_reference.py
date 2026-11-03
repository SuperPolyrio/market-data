"""Official price/contract projection on the existing market-data HTTP reader.

Only existing SQLite, canonical PostgreSQL and committed book readers are used.
No public source requests, new listener, collector, or workbench dependency.
"""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

from aiohttp import web

ROOT = Path(__file__).resolve().parents[2]
ASSETS = ("BTC", "ETH")


def bind_market(raw, asset, start_ms, observed_at):
    url = f"https://data.chain.link/streams/{asset.lower()}-usd-twap-60s-streams"
    description = raw.get("description", "")
    version = raw.get("version")
    def array(value):
        return json.loads(value) if isinstance(value, str) else value
    labels = array(raw.get("outcomes", []))
    tokens = array(raw.get("positionIds" if version == "v2" else "clobTokenIds", []))
    end = int(datetime.fromisoformat(raw["endDate"].replace("Z", "+00:00")).timestamp() * 1000)
    if (raw.get("slug") != f"{asset.lower()}-updown-5m-{start_ms // 1000}"
            or end != start_ms + 300000 or version not in ("v1", "v2")
            or raw.get("resolutionSource") != url or url not in description
            or "greater than or equal to" not in description or "time-weighted average" not in description
            or not re.fullmatch(r"0x[0-9a-fA-F]{64}", str(raw.get("conditionId")))
            or not str(raw.get("id", "")).isdigit()
            or not isinstance(labels, list) or not isinstance(tokens, list) or len(labels) != 2 or len(tokens) != 2
            or {str(label).upper() for label in labels} != {"UP", "DOWN"}
            or len(set(map(str, tokens))) != 2
            or any(not str(token).isdigit() or not 0 < int(token) < 2**256 for token in tokens)):
        raise ValueError("CONTRACT_RULES_UNVERIFIED")
    return {"asset": asset, "market_id": str(raw["id"]), "condition_id": raw["conditionId"],
            "slug": raw["slug"], "title": raw.get("question"), "version": version,
            "outcomes": {str(label).upper(): str(token) for label, token in zip(labels, tokens)},
            "starts_at_ms": start_ms, "ends_at_ms": end, "price_kind": "twap", "window_seconds": 60,
            "comparator": ">=", "rule_url": url,
            "rule_version": "market-data:twap60:" + hashlib.sha256(description.encode()).hexdigest()[:16],
            "metadata_received_at_ms": int(observed_at.timestamp() * 1000), "anchor": None}


def read_markets(now_ms):
    # Reuse the acquisition database configuration without exposing credentials.
    from market_data.storage import connect_postgres
    start = now_ms // 300000 * 300000
    output = {}
    with connect_postgres(connect_timeout=2) as db:
        db.execute("SET TRANSACTION READ ONLY")
        db.execute("SET LOCAL statement_timeout='1500ms'")
        for asset in ASSETS:
            row = db.execute("""SELECT m.condition_id,m.gamma_market_id,m.protocol_version,
                m.clob_token_ids,m.position_ids,o.source_payload,o.observed_at
                FROM core.markets m JOIN LATERAL (
                  SELECT source_payload,observed_at FROM core.market_observations
                  WHERE market_id=m.id ORDER BY observed_at DESC LIMIT 1
                ) o ON true WHERE m.slug=%s AND m.identity_kind='canonical_gamma' LIMIT 1""",
                (f"{asset.lower()}-updown-5m-{start // 1000}",)).fetchone()
            if not row:
                continue
            raw = row["source_payload"]
            raw_tokens = raw.get("positionIds" if raw.get("version") == "v2" else "clobTokenIds", [])
            if isinstance(raw_tokens, str):
                try: raw_tokens = json.loads(raw_tokens)
                except ValueError: continue
            if (row["observed_at"].timestamp()*1000 > now_ms
                    or raw.get("conditionId") != row["condition_id"] or str(raw.get("id")) != row["gamma_market_id"]
                    or raw.get("version") != row["protocol_version"]
                    or raw_tokens != row["position_ids" if raw.get("version") == "v2" else "clob_token_ids"]):
                continue
            try:
                output[asset] = bind_market(raw, asset, start, row["observed_at"])
            except (ValueError, KeyError, TypeError):
                continue
    return output


def read_prices(now_ms):
    path = ROOT / "src/prices/reference_reader.py"
    root = os.environ["MARKET_DATA_TWAP_ROOT"]
    host = os.environ.get("MARKET_DATA_TWAP_SSH_TARGET")
    if not host:
        import runpy
        return runpy.run_path(str(path))["read_archive"](root, now_ms)
    runtime = Path(os.environ.get("RUNTIME_DIRECTORY", f"/run/user/{os.getuid()}"))
    command = ["ssh", "-T", "-i", os.environ["MARKET_DATA_TWAP_SSH_KEY"],
        "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
        "-o", "ConnectTimeout=2", "-o", "ControlMaster=auto", "-o", "ControlPersist=30",
        "-o", "ControlPath=" + str(runtime / "market-data-archive-%C"), host,
        shlex.join(["python3", "-", "--root", root, "--now", str(now_ms)])]
    completed = subprocess.run(command, input=path.read_text(), text=True, capture_output=True, timeout=3, check=True)
    return json.loads(completed.stdout)


class ReferenceReader:
    def __init__(self, books):
        self.books, self.pending, self.cached = books, None, None
        self.expires = 0
        self.markets = {}
        self.market_round = None
        self.market_retry = 0
        self.market_error = None

    async def build(self):
        now = int(time.time()*1000)
        start = now // 300000 * 300000
        if self.market_round != start or time.monotonic() >= self.market_retry:
            try:
                self.markets = await asyncio.to_thread(read_markets, now)
                self.market_error = None
            except Exception:
                if self.market_round != start:
                    self.markets = {}
                self.market_error = "MARKET_READ_FAILED"
            self.market_round = start
            self.market_retry = time.monotonic() + (30 if not self.market_error and len(self.markets) == len(ASSETS) else 5)
        try:
            raw = await asyncio.to_thread(read_prices, int(time.time()*1000))
            if raw.get("source") != "market-data" or raw.get("schemaVersion") != 1:
                raise ValueError("PRICE_IDENTITY_INVALID")
            price_error = None
        except Exception:
            raw = {"assets": {}}
            price_error = "TWAP_READ_FAILED"
        result = {"source": "market-data", "schemaVersion": 1, "assets": {}}
        for asset in ASSETS:
            item = raw["assets"].get(asset, {})
            prices = item.get("prices", [])
            if price_error and self.cached:
                prices = self.cached["assets"].get(asset, {}).get("prices", [])
            market = dict(self.markets[asset]) if asset in self.markets else None
            opening = item.get("opening")
            if price_error and market and self.cached:
                previous = self.cached["assets"].get(asset, {}).get("market") or {}
                if previous.get("condition_id") == market["condition_id"]:
                    opening = previous.get("anchor")
            if market and opening and opening["event_at_ms"] == market["starts_at_ms"]:
                market["anchor"] = {**opening, "condition_id": market["condition_id"],
                    "source": "market-data official TWAP at contract start", "precision": "exact_e18"}
                market["opening_alignment"] = {"status": "EXACT_SOURCE_TIMESTAMP"}
            result["assets"][asset] = {"market": market, "prices": prices, "quotes": {},
                "read_error": price_error, "market_error": self.market_error or (None if market else "CONTRACT_NOT_FOUND")}

        async def quotes(asset, item):
            market = item["market"]
            if not market or market["ends_at_ms"] <= time.time()*1000:
                return
            try:
                envelope = await self.books.cached_read(market["outcomes"]["UP"], market["outcomes"]["DOWN"])
                if envelope.get("conditionId") != market["condition_id"]:
                    return
                for label, side in (("UP", "yes"), ("DOWN", "no")):
                    token, book = market["outcomes"][label], envelope.get(side, {})
                    if book.get("tokenId") != token or book.get("bookStatus") not in ("available", "live"):
                        continue
                    # This is a committed snapshot, not a continuity/freshness proof.
                    item["quotes"][token] = {"asset_id": token, "condition_id": market["condition_id"],
                        "best_bid": book.get("bestBid"), "best_ask": book.get("bestAsk"),
                        "book_observed_at": book.get("eventAt"), "received_at_ms": int(time.time()*1000),
                        "snapshot_at": envelope.get("generatedAt"), "quality": "snapshot",
                        "upstream_completeness": envelope.get("upstreamCompleteness", "UNKNOWN")}
            except Exception:
                item["book_error"] = "BOOK_UNAVAILABLE"
        await asyncio.gather(*(quotes(asset, item) for asset, item in result["assets"].items()))
        result["readAt"] = int(time.time()*1000)
        self.cached, self.expires = result, time.monotonic() + 1
        return result

    async def read(self):
        if self.cached and time.monotonic() < self.expires:
            return self.cached
        if self.pending is None:
            self.pending = asyncio.create_task(self.build())
            def done(task):
                self.pending = None
                if not task.cancelled(): task.exception()
            self.pending.add_done_callback(done)
        return await asyncio.shield(self.pending)


def register_reference_routes(app, books):
    reader = ReferenceReader(books)
    async def reference(request):
        if not os.environ.get("MARKET_DATA_TWAP_ROOT"):
            raise web.HTTPServiceUnavailable(text="Official price archive reader is not configured")
        return web.json_response(await reader.read(), headers={"Cache-Control": "no-store"})
    app.router.add_get("/crypto/reference", reference)
