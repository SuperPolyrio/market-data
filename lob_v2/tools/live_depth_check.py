#!/usr/bin/env python3
"""Bracket a committed gateway snapshot with official full-depth REST books."""
import argparse
import asyncio
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import random
import sys
import time

PACKAGE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PACKAGE/(".vendor312" if sys.version_info >= (3,12) else ".vendor")),str(PACKAGE)]
import httpx
from lobv2.journal import atomic_json
from lobv2.node import receive_snapshot,confirm_snapshot
from lobv2.wire import control,send


def levels(rows):
    result = {}
    for row in rows:
        price,size = (row["price"],row["size"]) if isinstance(row,dict) else row
        price,size = Decimal(price),Decimal(size)
        if price in result:
            raise ValueError("DUPLICATE_PRICE")
        if size:
            result[price] = size
    return result


def depth(book):
    return {side:levels(book[side]) for side in ("bids","asks")}


async def check(root, destination, count, market_ids=()):
    destination.mkdir(parents=True,exist_ok=True)
    settings = json.loads((root/"settings.json").read_bytes())
    data_root = Path(settings["data_root"])
    candidates = []
    for path in data_root.glob("G-*/status.json"):
        status = json.loads(path.read_bytes())
        if time.time_ns()-status["utc_ns"] > 90*10**9:
            continue
        good = [m for m,assets in status["books"].items() if
            (m in market_ids if market_ids else len(assets)==2 and all(b["ready"] and not b["crossed"] for b in assets.values()))]
        random.Random(path.parent.name).shuffle(good)
        for market in (good if market_ids else good[:3]):
            candidates.append((path.parent.name,market))
    random.Random(20261017).shuffle(candidates)
    candidates = candidates if market_ids else candidates[:count]
    if not candidates:
        raise ValueError("NO_QUALIFIED_GATEWAY_BOOKS")
    groups = {}
    for node,market in candidates:
        groups.setdefault(node,[]).append(market)
    semaphore = asyncio.Semaphore(8)
    results = []
    async with httpx.AsyncClient(timeout=30) as client:
        async def rest(token,phase):
            async with semaphore:
                started = time.time_ns()
                try:
                    response = await client.get("https://clob.polymarket.com/book",params={"token_id":token})
                    target = destination/phase/(token+".json")
                    target.parent.mkdir(parents=True,exist_ok=True)
                    target.write_bytes(response.content)
                    return {"status":response.status_code,"body":response.json(),"start_ns":started,"end_ns":time.time_ns(),
                        "raw_sha256":hashlib.sha256(response.content).hexdigest()}
                except Exception as exc:
                    return {"status":"ERROR","error":str(exc),"start_ns":started,"end_ns":time.time_ns()}
        for node,markets in groups.items():
            instance = json.loads((root/"instances"/(node+".json")).read_bytes())
            scope = json.loads(Path(instance["scope_file"]).read_bytes())
            tokens = [t for m in scope["markets"] if m["market"] in markets for t in m["assets"]]
            before = dict(zip(tokens,await asyncio.gather(*(rest(t,"before") for t in tokens))))
            writer = None
            try:
                reader,writer = await asyncio.open_connection("127.0.0.1",instance["config"]["listen"]["port"])
                await send(writer,control("HELLO",{"schema":2,"role":"consumer","markets":markets}))
                snapshot,session = await receive_snapshot(reader)
                await confirm_snapshot(reader,writer,snapshot,session)
                atomic_json(destination/(node+"-bootstrap.json"),snapshot)
            except (OSError,ValueError,asyncio.TimeoutError) as exc:
                results.extend({"gateway":node,"token":token,"result":"GATEWAY_UNAVAILABLE","error":str(exc)} for token in tokens)
                continue
            finally:
                if writer:
                    writer.close()
                    await writer.wait_closed()
            after = dict(zip(tokens,await asyncio.gather(*(rest(t,"after") for t in tokens))))
            snapshots = {token:(item,book) for item in snapshot["items"] for token,book in item["assets"].items()}
            for token in tokens:
                result = {"gateway":node,"token":token,"tape_uuid":snapshot["stream"],
                    "before":{k:v for k,v in before[token].items() if k!="body"},
                    "after":{k:v for k,v in after[token].items() if k!="body"}}
                if before[token]["status"] != 200 or after[token]["status"] != 200:
                    result["result"] = "OFFICIAL_BOOK_UNAVAILABLE"
                elif token not in snapshots:
                    result["result"] = "NO_COMMITTED_BASELINE_AT_CUT"
                else:
                    item,book = snapshots[token]
                    old,new,actual = depth(before[token]["body"]),depth(after[token]["body"]),depth(book)
                    result.update(market=item["market"],cut=item["cut"],route=item["route"],
                        quality=item.get("quality",{}),bid_levels=len(actual["bids"]),ask_levels=len(actual["asks"]),ready=book["ready"])
                    result["result"] = ("NO_COMMITTED_BASELINE_AT_CUT" if not book["ready"] else
                        "CONCURRENT_UPDATE" if old!=new else "EXACT_STABLE_BRACKET_MATCH" if actual==old else "STABLE_DEPTH_MISMATCH")
                results.append(result)
    counts = {label:sum(r["result"]==label for r in results) for label in sorted({r["result"] for r in results})}
    report = {"utc_ns":time.time_ns(),"markets_requested":len(candidates),"explicit_markets":list(market_ids),
        "explicit_markets_unavailable":sorted(set(market_ids)-{m for _,m in candidates}),"tokens_checked":len(results),"counts":counts,
        "scope":"Sampled full-depth comparison at committed gateway cuts; upstream event completeness remains UNKNOWN",
        "results":results}
    atomic_json(destination/"report.json",report)
    return {k:v for k,v in report.items() if k!="results"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--deployment",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--markets",type=int,default=24)
    parser.add_argument("--market-id",action="append",default=[])
    args = parser.parse_args()
    print(json.dumps(asyncio.run(check(args.deployment,args.output,args.markets,args.market_id)),indent=2))
