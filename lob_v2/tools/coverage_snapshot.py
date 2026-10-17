#!/usr/bin/env python3
"""Freeze observed token coverage against the independent full market census."""
import argparse
import json
from pathlib import Path
import sys
import time

PACKAGE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(PACKAGE/(".vendor312" if sys.version_info >= (3,12) else ".vendor")),str(PACKAGE)]
import pyarrow as pa
import pyarrow.parquet as pq
from lobv2.journal import atomic_json


def load(path):
    return json.loads(path.read_bytes())


def collect(root, output):
    output.mkdir(parents=True,exist_ok=True)
    started=time.time_ns()
    catalog=load(root/"universe.json")
    expected={m["market"] for m in catalog["markets"]}
    settings=load(root/"settings.json")
    deployment=load(root/"deployment-summary.json")
    data_root=Path(settings["data_root"])
    schema=pa.schema([("plant",pa.string()),("producer",pa.string()),("stream",pa.string()),
        ("market",pa.string()),("token",pa.string()),("lane",pa.int8()),("assigned",pa.bool_()),
        ("in_full_market_census",pa.bool_()),("written",pa.bool_()),("book_seen",pa.bool_()),
        ("baseline_durable",pa.bool_()),("ready",pa.bool_()),("connection",pa.string()),
        ("connection_epoch",pa.int64()),("state",pa.string()),("status_utc_ns",pa.int64()),
        ("transport_healthy",pa.bool_()),("pong_rtt_ms",pa.float64())])
    ready={"P":set(),"Q":set(),"G":set()}
    owners,duplicates,stale={},[],[]
    transports={role:{"observed_lanes":0,"healthy_lanes":0,"unqualified_lanes":0} for role in ("P","Q")}
    lane_counts={role:{str(lane):{key:0 for key in ("assigned","written","book_seen","baseline_durable","ready")}
        for lane in (0,1)} for role in ("P","Q")}
    with pq.ParquetWriter(output/"subscription_coverage.parquet",schema,compression="zstd") as writer:
        for cohort in deployment["cohorts"]:
            scope=load(root/"scopes"/(cohort["id"]+".json"))
            for market in scope["markets"]:
                if market["market"] in owners:
                    duplicates.append(market["market"])
                owners[market["market"]]=cohort["id"]
            for role in ("P","Q","G"):
                node=role+"-"+cohort["id"]
                status=load(data_root/node/"status.json")
                is_stale=time.time_ns()-status["utc_ns"]>90*10**9
                if is_stale:stale.append(node)
                if not is_stale:
                    ready[role].update(m for m,books in status["books"].items() if len(books)==2 and all(b["ready"] and not b["crossed"] for b in books.values()))
                if role=="G":continue
                states={(s["token"],s["lane"]):s for s in status["token_states"]}
                lanes={}
                for state in status["token_states"]:
                    lanes.setdefault((state["shard"],state["lane"]),state)
                transports[role]["observed_lanes"]+=len(lanes)
                healthy=sum(bool(s.get("transport_healthy")) and not is_stale for s in lanes.values())
                transports[role]["healthy_lanes"]+=healthy
                transports[role]["unqualified_lanes"]+=len(lanes)-healthy
                rows=[]
                for market in scope["markets"]:
                    for token in market["assets"]:
                        for lane in (0,1):
                            state=states.get((token,lane),{})
                            row={"plant":role,"producer":node,"stream":status["stream"],"market":market["market"],
                                "token":token,"lane":lane,"assigned":True,"in_full_market_census":market["market"] in expected,
                                **{key:bool(state.get(key)) and not is_stale for key in ("written","book_seen","baseline_durable","ready")},
                                "connection":state.get("connection"),"connection_epoch":state.get("connection_epoch"),
                                "transport_healthy":bool(state.get("transport_healthy")) and not is_stale,"pong_rtt_ms":state.get("pong_rtt_ms"),
                                "state":"STALE_TELEMETRY" if is_stale else state.get("status","ASSIGNED_UNSENT"),"status_utc_ns":status["utc_ns"]}
                            rows.append(row)
                            for key in lane_counts[role][str(lane)]:lane_counts[role][str(lane)][key]+=int(row[key])
                writer.write_table(pa.Table.from_pylist(rows,schema=schema))
    report={"started_ns":started,"finished_ns":time.time_ns(),"catalog_digest":catalog["catalog_digest"],
        "catalog_sweep_start_ns":catalog["sweep_start_ns"],"catalog_sweep_end_ns":catalog["sweep_end_ns"],
        "full_universe_sweep":catalog["full_universe_sweep"],"census_markets":len(expected),"census_tokens":2*len(expected),
        "assigned_markets":len(owners),"assigned_tokens":2*len(owners),"duplicate_market_owners":duplicates,
        "catalog_markets_unassigned":sorted(expected-set(owners)),"stale_instances":stale,"lane_observations":lane_counts,
        "transport_lanes":transports,
        "roles":{role:{"qualified_markets_all":len(markets),"qualified_tokens_all":2*len(markets),
            "qualified_census_markets":len(markets&expected),"qualified_census_tokens":2*len(markets&expected),
            "census_coverage":len(markets&expected)/len(expected),"missing_census_markets":sorted(expected-markets)} for role,markets in ready.items()},
        "unresolved_catalog_rows":len(catalog["unresolved"]),"query_projection":"TOKEN_COVERAGE_ONLY_NOT_TAPE_QUERY_COMPLETION",
        "upstream_native_event_completeness":"UNKNOWN"}
    atomic_json(output/"report.json",report)
    print(json.dumps({k:v for k,v in report.items() if k not in ("roles","lane_observations")} | {"roles":{r:{k:v for k,v in row.items() if k!="missing_census_markets"} for r,row in report["roles"].items()}},indent=2))


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--deployment",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    collect(args.deployment,args.output)
