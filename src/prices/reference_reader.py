"""Bounded read-only projection of the existing official TWAP archive.

This module is also executed over the existing archive SSH access. It uses
only the standard library and never creates, imports or modifies source data.
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
import json
import sqlite3
import time

ASSETS = ("BTC", "ETH")


def observation(row, asset, now_ms):
    """Keep original clocks and E18; reject conflicting source/clock evidence."""
    try:
        exact = int(row["full_accuracy_value"])
        price = Decimal(row["price"])
        at, published, received = row["observed_ts"], row["published_ts"], row["received_ts"]
        if (not price.is_finite() or price <= 0 or price != Decimal(exact).scaleb(-18)
                or row["feed_id"] != f"rtds:crypto_prices_twap_sixty:{asset.lower()}/usd"
                or row["source"] != "polymarket_rtds_chainlink" or row["topic"] != "crypto_prices_twap_sixty"
                or not 0 < at <= published <= received // 1_000_000 <= now_ms):
            return None
        return {"value": str(price), "full_accuracy_value": str(exact), "event_at_ms": at,
                "published_at_ms": published, "received_at_ms": received // 1_000_000,
                "original_received_ns": str(received), "feed_id": row["feed_id"],
                "frame_id": row["frame_id"], "price_kind": "twap", "window_seconds": 60,
                "quote_currency": "USD", "precision": "exact_e18"}
    except (ValueError, TypeError, KeyError, InvalidOperation):
        return None


def read_archive(root, now_ms):
    start = now_ms // 300000 * 300000
    since = now_ms - 900000
    result = {asset: {"prices": [], "opening": None} for asset in ASSETS}
    today = datetime.fromtimestamp(now_ms / 1000, timezone.utc).date()
    # Keep the last actual observation through outages and the UTC rollover.
    # Only two known daily files are inspected; never scan the archive.
    days = [today.isoformat(), (today - timedelta(days=1)).isoformat()]
    for day in days:
        crosses_midnight = datetime.fromtimestamp(since / 1000, timezone.utc).date() != today
        if day != today.isoformat() and not crosses_midnight and all(item["prices"] for item in result.values()):
            continue
        path = Path(root) / day / "twap.sqlite3"
        if not path.is_file():
            continue
        with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=.5) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            deadline = time.monotonic() + 1
            db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            for asset in ASSETS:
                rows = db.execute("""SELECT frame_id,price,full_accuracy_value,observed_ts,published_ts,
                    received_ts,feed_id,source,topic FROM records
                    WHERE asset=? AND observed_ts<=? AND window_seconds=60
                    AND error IS NULL AND duplicate=0 AND conflict=0 AND timestamp_order=0
                    ORDER BY observed_ts DESC LIMIT 901""", (asset.lower() + "/usd", now_ms))
                for row in rows:
                    point = observation(row, asset, now_ms)
                    if point:
                        result[asset]["prices"].append(point)
    for item in result.values():
        ordered = sorted(item["prices"], key=lambda p: p["event_at_ms"])
        # Conflicting values at the same observed time must not become an anchor.
        grouped = {}
        for point in ordered:
            grouped.setdefault(point["event_at_ms"], []).append(point)
        valid = [rows[0] for rows in grouped.values() if len({r["full_accuracy_value"] for r in rows}) == 1]
        item["opening"] = next((p for p in valid if p["event_at_ms"] == start), None)
        recent = [p for p in valid if p["event_at_ms"] >= since]
        item["prices"] = recent[-300:] if recent else valid[-1:]
    return {"source": "market-data", "schemaVersion": 1, "readAt": now_ms, "assets": result}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--now", required=True, type=int)
    args = parser.parse_args()
    print(json.dumps(read_archive(args.root, args.now), allow_nan=False))
