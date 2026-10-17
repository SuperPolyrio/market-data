"""Exact source books. Native snapshots replace one asset; RESET replaces scope."""
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
import json

from .wire import json_bytes


def decimal(value, *, price=False):
    if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool) or len(str(value)) > 256:
        raise ValueError("INVALID_DECIMAL")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("INVALID_DECIMAL") from exc
    if not number.is_finite() or number < 0 or abs(number.as_tuple().exponent) > 128:
        raise ValueError("INVALID_DECIMAL")
    if price and number > 1:
        raise ValueError("PRICE_OUT_OF_RANGE")
    return number


def text(number):
    value = format(number, "f")
    return value.rstrip("0").rstrip(".") if "." in value else value


def levels(rows):
    if not isinstance(rows, list):
        raise ValueError("INVALID_LEVELS")
    result = {}
    for row in rows:
        p, s = decimal(row["price"], price=True), decimal(row["size"])
        if p in result:
            raise ValueError("DUPLICATE_SNAPSHOT_PRICE")
        if s:
            result[p] = s
    return result


def empty_asset():
    return {"bids": {}, "asks": {}, "ready": False, "snapshot_epoch": 0,
            "last_source_time": None, "baseline_ref": None, "last_ref": None}


def asset_view(asset):
    return {**{k: v for k, v in asset.items() if k not in ("bids", "asks")},
            "bids": [[text(p), text(s)] for p, s in sorted(asset["bids"].items(), reverse=True)],
            "asks": [[text(p), text(s)] for p, s in sorted(asset["asks"].items())]}


def asset_load(view):
    return {**view, "bids": {decimal(p, price=True): decimal(s) for p, s in view["bids"]},
            "asks": {decimal(p, price=True): decimal(s) for p, s in view["asks"]}}


class SourceBook:
    def __init__(self, market, assets, max_levels=100000):
        self.market = market
        self.assets = {asset: empty_asset() for asset in assets}
        self.max_levels = max_levels

    @property
    def ready(self):
        return bool(self.assets) and all(asset["ready"] for asset in self.assets.values())

    def snapshot(self):
        return {asset: asset_view(book) for asset, book in self.assets.items()}

    def invalidate(self):
        for asset in self.assets.values():
            asset["ready"] = False

    def observe(self, event, ref):
        if event.get("market") != self.market:
            raise ValueError("MARKET_OWNERSHIP_MISMATCH")
        kind = event.get("event_type")
        ts = event.get("timestamp")
        if kind == "book":
            asset_id = event["asset_id"]
            if asset_id not in self.assets:
                raise ValueError("UNKNOWN_ASSET")
            previous = self.assets[asset_id]
            asset = {"bids": levels(event["bids"]), "asks": levels(event["asks"]),
                     "ready": True, "snapshot_epoch": previous["snapshot_epoch"] + 1,
                     "baseline_ref": ref, "last_ref": ref, "last_source_time": ts}
            if len(asset["bids"])+len(asset["asks"])>self.max_levels:
                raise ValueError("BOOK_MEMORY_BUDGET_EXCEEDED")
            self.assets[asset_id] = asset
            return {"kind": "SNAPSHOT", "scope": "ASSET", "assets": {asset_id: asset_view(asset)},
                    "source_time_raw": ts, "native_hash": event.get("hash"), "raw_ref": ref}
        if kind == "price_change":
            rows = event["price_changes"]
            if not isinstance(rows, list) or not rows:
                raise ValueError("INVALID_CHANGE_GROUP")
            changes, touched = [], {}
            # Validate the whole native array before exposing any changes.
            for i, row in enumerate(rows):
                asset = row["asset_id"]
                if asset not in self.assets:
                    raise ValueError("UNKNOWN_ASSET")
                if row["side"] not in ("BUY", "SELL"):
                    raise ValueError("INVALID_SIDE")
                p, s = decimal(row["price"], price=True), decimal(row["size"])
                changes.append({"asset": asset, "side": row["side"], "price": text(p),
                                "size": text(s), "original_index": i,
                                "native_hash": row.get("hash"),
                                "best_bid": row.get("best_bid"), "best_ask": row.get("best_ask")})
            if any(not self.assets[row["asset"]]["ready"] for row in changes):
                return {"kind": "BEFORE_SNAPSHOT", "raw_ref": ref}
            for row in changes:
                asset = row["asset"]
                if asset not in touched:
                    touched[asset] = {**self.assets[asset], "bids": self.assets[asset]["bids"].copy(),
                                      "asks": self.assets[asset]["asks"].copy()}
                book = touched[asset]
                side = book["bids" if row["side"] == "BUY" else "asks"]
                p, s = decimal(row["price"], price=True), decimal(row["size"])
                if s:
                    side[p] = s
                else:
                    side.pop(p, None)
                book.update(last_ref=ref, last_source_time=ts)
            if any(len(book["bids"])+len(book["asks"])>self.max_levels for book in touched.values()):
                raise ValueError("BOOK_MEMORY_BUDGET_EXCEEDED")
            self.assets.update(touched)
            return {"kind": "CHANGE_GROUP", "changes": changes, "raw_ref": ref,
                    "source_time_raw": ts}
        return {"kind": "NON_LOB", "event_type": kind or "UNKNOWN", "raw_ref": ref,
                "source_time_raw": ts}


def apply(state, body):
    """Apply an already validated complete normalized record to working/committed state."""
    kind = body["kind"]
    if kind in ("SOURCE_RESET", "CURRENT_SNAPSHOT", "BOOTSTRAP"):
        if body.get("scope") != "MARKET":
            raise ValueError("RESET_SCOPE")
        state = deepcopy(body["assets"])
    elif kind == "SNAPSHOT":
        if body.get("scope") != "ASSET" or len(body["assets"]) != 1:
            raise ValueError("ASSET_SNAPSHOT_SCOPE")
        state = {**state, **deepcopy(body["assets"])}
    elif kind == "CHANGE_GROUP":
        books = {asset: asset_load(value) for asset, value in state.items()}
        for row in body["changes"]:
            book = books[row["asset"]]
            side = book["bids" if row["side"] == "BUY" else "asks"]
            p, s = decimal(row["price"], price=True), decimal(row["size"])
            if s:
                side[p] = s
            else:
                side.pop(p, None)
            book.update(last_ref=body["raw_ref"], last_source_time=body.get("source_time_raw"))
        state = {asset: asset_view(book) for asset, book in books.items()}
    elif kind == "GAP":
        state = deepcopy(state)
        for asset in state.values():
            asset["ready"] = False
    return state


def inspect(state):
    result = {}
    for token, view in state.items():
        asset = asset_load(view)
        bid = max(asset["bids"], default=None)
        ask = min(asset["asks"], default=None)
        result[token] = {"ready": view["ready"], "bid_levels": len(asset["bids"]),
                         "ask_levels": len(asset["asks"]), "best_bid": text(bid) if bid is not None else None,
                         "best_ask": text(ask) if ask is not None else None,
                         "crossed": bid is not None and ask is not None and bid >= ask}
    return result


def digest(state):
    return hashlib.sha256(json_bytes(state)).hexdigest()
