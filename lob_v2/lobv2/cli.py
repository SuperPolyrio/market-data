import argparse
import asyncio
import json
from pathlib import Path
import sys

from .archive import DirectoryStore, S3Store, build_archive, commit_archive, read_archive, verify_archive
from .catalog import discover
from .journal import atomic_json, read_journal
from .node import run_node


def main():
    parser = argparse.ArgumentParser(description="Polymarket SOURCE_SELECTED_L2 V2")
    commands = parser.add_subparsers(dest="command",required=True)
    p = commands.add_parser("discover")
    p.add_argument("--market-limit",type=int,default=None)
    p.add_argument("--pages",type=int,default=None)
    p.add_argument("--slug",action="append",default=[])
    p.add_argument("--events",action="store_true",help="Supplementary event catalog; does not certify the full market denominator")
    p.add_argument("--output",required=True)
    for name in ("capture","gateway"):
        p = commands.add_parser(name)
        p.add_argument("--id",default="G" if name == "gateway" else "P")
        p.add_argument("--config",required=True)
        p.add_argument("--scope",required=True)
        p.add_argument("--root",required=True)
        p.add_argument("--seconds",type=float)
    p = commands.add_parser("archive")
    p.add_argument("--root",required=True)
    p.add_argument("--store",required=True,help="Directory test backend path, or s3://bucket/prefix")
    p = commands.add_parser("replay")
    p.add_argument("--path",required=True)
    p.add_argument("--market")
    p.add_argument("--tape-uuid")
    p.add_argument("--frames",action="store_true")
    p = commands.add_parser("verify")
    p.add_argument("--path",required=True)
    p = commands.add_parser("verify-market")
    p.add_argument("--run",required=True)
    p = commands.add_parser("export-parquet")
    p.add_argument("--root",required=True)
    p.add_argument("--output",required=True)
    p = commands.add_parser("acceptance")
    p.add_argument("--scope",required=True)
    p.add_argument("--root",required=True)
    p.add_argument("--seconds",type=float,default=90)
    p.add_argument("--lanes",type=int,choices=(1,2),default=2)
    p.add_argument("--capacity",type=int,default=250)
    p.add_argument("--faults",action="store_true")
    p.add_argument("--routing-settings",help="Explicit production VPS routing settings for the live sample")
    args = parser.parse_args()
    if args.command == "discover":
        scope = asyncio.run(discover(args.market_limit,slugs=args.slug,pages=args.pages,events=args.events))
        atomic_json(args.output,scope)
        print(json.dumps({"markets":len(scope["markets"]),"tokens":2*len(scope["markets"]),"digest":scope["catalog_digest"]}))
    elif args.command in ("capture","gateway"):
        config = json.loads(Path(args.config).read_bytes())
        scope = json.loads(Path(args.scope).read_bytes())
        asyncio.run(run_node("plant" if args.command=="capture" else "gateway",args.id,config,scope,args.root,args.seconds))
    elif args.command == "archive":
        store = object_store(args.store)
        results = []
        for journal in sorted(Path(args.root).rglob("*.journal")):
            archive = journal.with_suffix(".lobseg")
            metadata = build_archive(journal,archive)
            key,manifest = commit_archive(store,archive,"capture" if journal.name.startswith("capture") else "tape")
            results.append({"manifest":key,"object_bytes":manifest["object_bytes"],"journal":str(journal)})
        print(json.dumps(results))
    elif args.command == "replay":
        reader = read_archive if args.path.endswith(".lobseg") else read_journal
        for rec in reader(args.path):
            if args.tape_uuid and rec.stream != args.tape_uuid:
                raise ValueError("RESET_REQUIRED: GENERATION_CHANGED")
            if args.market and rec.body.get("market") != args.market:
                continue
            if args.frames:
                sys.stdout.buffer.write(rec.frame)
            else:
                print(json.dumps({"id":[rec.stream,rec.offset],"kind":rec.kind,"body":rec.body,"raw_bytes":len(rec.raw)}))
    elif args.command == "verify":
        print(json.dumps(verify_archive(args.path)))
    elif args.command == "verify-market":
        from .validation import audit_run
        print(json.dumps(audit_run(Path(args.run)),indent=2))
    elif args.command == "export-parquet":
        from .validation import export_parquet
        print(json.dumps(export_parquet(Path(args.root),Path(args.output))))
    elif args.command == "acceptance":
        from .validation import live_acceptance
        print(json.dumps(asyncio.run(live_acceptance(args)),indent=2))


def object_store(uri):
    if uri.startswith("s3://"):
        from urllib.parse import urlsplit
        parsed = urlsplit(uri)
        return S3Store(parsed.netloc,parsed.path)
    return DirectoryStore(uri)
