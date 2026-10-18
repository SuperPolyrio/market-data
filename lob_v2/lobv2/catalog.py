"""Frozen identity scope and market-grouped connection planning."""
import hashlib
import json
import time

import httpx

from .wire import json_bytes

GAMMA = "https://gamma-api.polymarket.com"


def identity(row):
    version = str(row.get("version", "1")).lower().removeprefix("v")
    if version == "1":
        tokens = row.get("clobTokenIds", [])
        kind = "CLOB_TOKEN_ID"
    elif version == "2":
        tokens = row.get("positionIds", [])
        kind = "POSITION_ID"
    else:
        raise ValueError("UNSUPPORTED_MARKET_VERSION")
    tokens = json.loads(tokens) if isinstance(tokens, str) else tokens
    outcomes = row.get("outcomes", [])
    outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
    if len(tokens) != 2 or len(set(tokens)) != 2 or any(not isinstance(t, str) or not t.isdigit() for t in tokens):
        raise ValueError("UNRESOLVED_BINARY_IDENTITY")
    if len(outcomes) != len(tokens):
        raise ValueError("OUTCOME_MAPPING_MISSING")
    market = row.get("conditionId") or row.get("condition_id")
    if not market or not row.get("id"):
        raise ValueError("MARKET_ID_MISSING")
    return {"market": market, "gamma_id": str(row["id"]), "version": version,
            "asset_id_kind": kind, "assets": tokens, "outcomes": outcomes,
            "question": row.get("question"), "slug": row.get("slug"),
            "eligibility": "ELIGIBLE", "eligibility_policy": "explicit_frozen_binary_scope_v2",
            "evidence": {k: row.get(k) for k in ("active", "closed", "enableOrderBook", "acceptingOrders")}}


def validate_scope(scope):
    markets, owners = set(), {}
    for row in scope["markets"]:
        if row["market"] in markets or len(row["assets"]) != 2:
            raise ValueError("DUPLICATE_MARKET_OR_INCOMPLETE_PAIR")
        markets.add(row["market"])
        for token in row["assets"]:
            if not isinstance(token, str) or not token.isdigit() or token in owners:
                raise ValueError("DUPLICATE_TOKEN_OR_OWNERSHIP_CONFLICT")
            owners[token] = row["market"]
    if not owners:
        raise ValueError("EMPTY_SCOPE")
    return scope


def shards(scope, capacity):
    if capacity < 2:
        raise ValueError("CAPACITY_SMALLER_THAN_MARKET_PAIR")
    # Stable initial order; a frozen scope never rebalances in response to title refreshes.
    ordered = sorted(scope["markets"], key=lambda m: hashlib.sha256(m["market"].encode()).digest())
    result, current, count = [], [], 0
    for market in ordered:
        if current and count + len(market["assets"]) > capacity:
            result.append(current)
            current, count = [], 0
        current.append(market)
        count += len(market["assets"])
    if current:
        result.append(current)
    return result


async def discover(limit=None, *, slugs=(), pages=None, events=False, progress_path=None, budget=None):
    """Exhaust keyset pagination unless an explicit sample budget was requested."""
    started = time.time_ns()
    found, unresolved, excluded, cursor, seen = {}, [], [], None, set()
    page_count, exhausted = 0, False
    def consume(rows):
        for row in rows:
            if row.get("closed") or row.get("enableOrderBook") is False:
                excluded.append({"id":row.get("id"),"reason":"CLOSED_OR_NO_ORDERBOOK","closed":row.get("closed"),"enableOrderBook":row.get("enableOrderBook")})
                continue
            try:
                parsed = identity(row)
                found[parsed["market"]] = parsed
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                unresolved.append({"id":row.get("id"),"reason":str(exc)})
    async with httpx.AsyncClient(timeout=45) as client:
        if slugs:
            for slug in slugs:
                if budget:await budget.wait()
                response = await client.get(GAMMA+"/markets",params={"slug":slug})
                response.raise_for_status()
                consume(response.json())
        else:
            while pages is None or page_count < pages:
                params = {"closed":"false","limit":100}
                if cursor:
                    params["after_cursor"] = cursor
                for attempt in range(5):
                    try:
                        if budget:await budget.wait()
                        response = await client.get(GAMMA+("/events/keyset" if events else "/markets/keyset"),params=params)
                        response.raise_for_status()
                        break
                    except (httpx.HTTPError, OSError):
                        if attempt == 4:
                            raise
                        import asyncio
                        await asyncio.sleep(2**attempt)
                data = response.json()
                rows = data if isinstance(data,list) else data.get("data",data.get("events" if events else "markets",[]))
                if events:
                    rows = [market for event in rows for market in event.get("markets",[])]
                consume(rows)
                page_count += 1
                if progress_path:
                    from .journal import atomic_json
                    atomic_json(progress_path,{"utc_ns":time.time_ns(),"sweep_start_ns":started,
                        "pages":page_count,"markets":len(found),"unresolved":len(unresolved),
                        "excluded":len(excluded),"status":"SCANNING","full_universe_sweep":False})
                next_cursor = data.get("next_cursor") if isinstance(data,dict) else None
                if not next_cursor:
                    exhausted = True
                    break
                if next_cursor in seen:
                    raise ValueError("REPEATED_DISCOVERY_CURSOR")
                seen.add(next_cursor)
                cursor = next_cursor
    selected = list(found.values()) if limit is None else list(found.values())[:limit]
    full = exhausted and limit is None and not slugs and not events
    scope = {"schema":2,"catalog_revision":started,"scope_kind":"OPEN_BINARY_UNIVERSE" if full else "BOUNDED_SAMPLE",
             "sweep_start_ns":started,"sweep_end_ns":time.time_ns(),"pages":page_count,
             "full_universe_sweep":full,"markets":selected,"unresolved":unresolved,"excluded":excluded}
    scope["discovery_source"] = "GAMMA_EVENTS_KEYSET" if events else "GAMMA_MARKETS_KEYSET"
    scope["full_events_sweep"] = bool(events and exhausted and limit is None)
    scope["catalog_digest"] = hashlib.sha256(json_bytes(selected)).hexdigest()
    return validate_scope(scope)
