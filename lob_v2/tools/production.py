#!/usr/bin/env python3
"""Manage immutable ownership cohorts; discover new tokens without rebalancing live books."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import fcntl

PACKAGE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PACKAGE/(".vendor312" if sys.version_info >= (3,12) else ".vendor")),str(PACKAGE)]
from lobv2.catalog import discover
from lobv2.journal import atomic_json
from lobv2.node import run_node
from lobv2.routing import plant_network
from lobv2.wire import json_bytes


def load(path):
    return json.loads(path.read_bytes())


def unit(instance):
    return "market-data-lob-v2@"+instance+".service"


def enroll(root, scope):
    with (root/"catalog-owner.lock").open("a") as lock:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX)
        return enroll_locked(root,scope)


def enroll_locked(root, scope):
    settings = load(root/"settings.json")
    manifest_path = root/"deployment.json"
    manifest = load(manifest_path) if manifest_path.exists() else {"cohorts":[],"ownership":{},"conflicts":[]}
    # A crash after publishing the tail scope but before the deployment index is
    # repaired from that durable scope; its members must never get a second owner.
    tail_scope=None
    if manifest['cohorts']:
        tail=manifest['cohorts'][-1]
        tail_scope=load(root/'scopes'/(tail['id']+'.json'))
        for row in tail_scope['markets']:
            manifest['ownership'].setdefault(row['market'],{'cohort':tail['id'],'assets':row['assets'],'version':row['version']})
        tail.update(markets=len(tail_scope['markets']),tokens=sum(len(m['assets']) for m in tail_scope['markets']),
            scope_digest=tail_scope['catalog_digest'])
    from lobv2.catalog_owner import CatalogOwner
    owner=CatalogOwner(root)
    # Migrate known, still-unresolved ownership without erasing members merely
    # because a newer partial/full scan did not return them.
    if not owner.metadata('ownership_seeded',False):
        with owner.db:
            owner.db.execute('BEGIN IMMEDIATE')
            for path in sorted((root/'scopes').glob('*.json')):
                for row in load(path)['markets']:owner.upsert(row,{'source':'EXISTING_OWNERSHIP_MIGRATION','path':str(path)})
            owner.set_metadata('ownership_seeded',True)
            if (root/'universe.json').exists():
                old_scope=load(root/'universe.json')
                if old_scope.get('full_universe_sweep'):
                    owner.set_metadata('census_members',[m['market'] for m in old_scope['markets']])
                    owner.set_metadata('last_complete_scan',{'source':old_scope.get('discovery_source'),
                        'scan_id':old_scope['catalog_revision'],'observed_at':old_scope['sweep_end_ns'],'complete':True})
    input_scope=scope
    scope=owner.apply_scan(scope)
    atomic_json(root/'catalog-snapshot.json',scope)
    blocked={row['market'] for row in scope['unadmitted_groups']}
    terminal={row['market'] for row in scope['terminal_evidence']+scope['scope_exclusions']}
    atomic_json(root/'route-exclusions.json',{'catalog_revision':scope['catalog_revision'],'scope_policy_version':scope['scope_policy_version'],
        'terminal':scope['terminal_evidence']+scope['scope_exclusions'],'quarantined':scope['unadmitted_groups']})
    owner.close()
    fresh = []
    for market in scope["markets"]:
        if market["market"] in blocked or market["market"] in terminal:continue
        old = manifest["ownership"].get(market["market"])
        if old:
            if old["assets"] != market["assets"] or old["version"] != market["version"]:
                conflict = {"market":market["market"],"old":old,"new":market,"status":"MIGRATION_REQUIRED"}
                if conflict not in manifest["conflicts"]:
                    manifest["conflicts"].append(conflict)
            continue
        fresh.append(market)
    fresh.sort(key=lambda m:hashlib.sha256(m["market"].encode()).digest())
    def write_scope(name,group):
        group_scope = {**scope,"markets":group,"scope_kind":"UNIVERSE_OWNERSHIP_COHORT",
            "parent_catalog_digest":scope["catalog_digest"],"catalog_digest":hashlib.sha256(json_bytes(group)).hexdigest()}
        for field in ('excluded','unresolved','terminal_evidence','scope_exclusions','unadmitted_groups','pending_validation'):
            group_scope.pop(field,None)
        atomic_json(root/'scopes'/(name+'.json'),group_scope)
        return group_scope['catalog_digest']
    if fresh and tail_scope is not None and settings.get('fill_existing_cohorts',False):
        tail=manifest['cohorts'][-1]
        room=max(0,settings['markets_per_cohort']-len(tail_scope['markets']))
        added=fresh[:room]
        if added:
            group=tail_scope['markets']+added
            tail['scope_digest']=write_scope(tail['id'],group)
            tail.update(markets=len(group),tokens=sum(len(m['assets']) for m in group),updated_ns=time.time_ns())
            for market in added:
                manifest['ownership'][market['market']]={'cohort':tail['id'],'assets':market['assets'],'version':market['version']}
            fresh=fresh[len(added):]
    for start in range(0,len(fresh),settings["markets_per_cohort"]):
        group = fresh[start:start+settings["markets_per_cohort"]]
        index = len(manifest["cohorts"])
        if 22000+index*3+2 >= 65000:
            scope['unadmitted_groups'].extend({'market':m['market'],'reason':'PORT_SPACE_EXHAUSTED'} for m in fresh[start:])
            break
        name = f"{index:04d}"
        scope_digest=write_scope(name,group)
        for offset, role in enumerate(("P","Q","G")):
            instance = role+"-"+name
            config = load(PACKAGE/"config/base.json")
            config.update(settings["node_defaults"])
            config["listen"] = {"host":"127.0.0.1","port":22000+index*3+offset,"queue_bytes":config["queue_bytes"]}
            config["outbound_proxy"] = settings["proxies"].get(role,"env")
            if role in settings.get("lane_proxies",{}):
                config["lane_proxies"] = settings["lane_proxies"][role]
            if role != "G":
                config.update(plant_network(settings,index,role))
                config["connection_rate_file"] = str(Path(settings["data_root"])/("connection-credit-"+role))
            if role == "G":
                config["plants"] = {plant+"-"+name:{"host":"127.0.0.1","port":22000+index*3+off}
                    for off,plant in enumerate(("P","Q"))}
            config["archive"] = settings["archive"]
            atomic_json(root/"instances"/(instance+".json"),{"role":"gateway" if role=="G" else "plant",
                "node_id":instance,"config":config,"scope_file":str(root/"scopes"/(name+".json")),
                "root":str(Path(settings["data_root"])/instance)})
        manifest["cohorts"].append({"id":name,"markets":len(group),"tokens":2*len(group),"created_ns":time.time_ns(),
            'scope_digest':scope_digest})
        for market in group:
            manifest["ownership"][market["market"]] = {"cohort":name,"assets":market["assets"],"version":market["version"]}
    if input_scope.get("full_universe_sweep"):
        manifest["last_full_sweep"] = {"sweep_end_ns":input_scope["sweep_end_ns"],"markets":len(input_scope["markets"]),"catalog_digest":input_scope["catalog_digest"]}
    manifest.update(utc_ns=time.time_ns(),latest_catalog_digest=scope["catalog_digest"],
        latest_catalog_markets=manifest.get("last_full_sweep",{}).get("markets",len(input_scope["markets"])),desired_markets=len(scope["markets"]),catalog_revision=scope["catalog_revision"],scope_policy_version=scope["scope_policy_version"],membership_digest=scope["membership_digest"],unadmitted_groups=scope["unadmitted_groups"],latest_full_sweep=bool(manifest.get("last_full_sweep")),
        missing_from_current_catalog=sorted(set(manifest["ownership"])-{m["market"] for m in scope["markets"]}))
    atomic_json(manifest_path,manifest)
    atomic_json(root/"deployment-summary.json",{k:v for k,v in manifest.items() if k != "ownership"} | {"assigned_markets":len(manifest["ownership"])})
    controls={c['id']:[] for c in manifest['cohorts']}
    for row in scope['terminal_evidence']+scope['scope_exclusions']+scope['unadmitted_groups']:
        if row['market'] in manifest['ownership']:
            controls[manifest['ownership'][row['market']]['cohort']].append({**row,'reason':row.get('reason','CATALOG_TERMINAL_OR_EXCLUDED')})
    for cohort,blocked_rows in controls.items():
        cohort_meta=next(c for c in manifest['cohorts'] if c['id']==cohort)
        digest=cohort_meta.get('scope_digest')
        if digest is None:digest=load(root/'scopes'/(cohort+'.json'))['catalog_digest']
        atomic_json(root/'controls'/(cohort+'.json'),{'catalog_revision':scope['catalog_revision'],
            'scope_policy_version':scope['scope_policy_version'],'membership_digest':scope['membership_digest'],
            'cohort_scope_digest':digest,'blocked':blocked_rows})
    # Commit after all cohort files; readers defer future revisions until this head.
    atomic_json(root/'catalog-head.json',{'catalog_revision':scope['catalog_revision'],
        'scope_policy_version':scope['scope_policy_version'],'membership_digest':scope['membership_digest'],
        'desired_markets':len(scope['markets']),'last_complete_scan':scope['last_complete_scan']})
    # Save membership before launching so a crash cannot create a second owner.
    # Retry every existing cohort after controller restart or partial systemctl failure.
    active_units, stopped_units = [], []
    quarantined = {c["old"]["cohort"] for c in manifest["conflicts"]}
    for cohort in manifest["cohorts"]:
        names = [unit(role+"-"+cohort["id"]) for role in ("P","Q","G")]
        active_units.extend(names)
    if stopped_units:
        subprocess.run(["systemctl","--user","stop",*stopped_units],check=True)
    if active_units:
        subprocess.run(["systemctl","--user","enable",*active_units],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        subprocess.run(["systemctl","--user","start",*active_units],check=True)
    return manifest


def health(root):
    settings = load(root/"settings.json")
    deployment = load(root/"deployment-summary.json") if (root/"deployment-summary.json").exists() else load(root/"deployment.json")
    assigned = deployment.get("assigned_markets",len(deployment.get("ownership",{})))
    result = {"utc_ns":time.time_ns(),"catalog_markets":deployment["latest_catalog_markets"],
        "assigned_markets":assigned,"assigned_tokens":2*assigned,
        "full_universe_sweep":deployment["latest_full_sweep"],"cohorts":len(deployment["cohorts"]),
        "identity_conflicts":len(deployment["conflicts"]),"roles":{},"stale_instances":[],"archive":{}}
    for role in ("P","Q","G"):
        aggregate = {"instances":0,"raw_received":0,"raw_bytes":0,"pending_bytes":0,"emits":0,"parse_errors":0,
            "written_observations":0,"book_seen_observations":0,"durable_baseline_observations":0,"ready_observations":0,
            "ready_tokens":0,"published_tokens":0,"published_markets":0,"crossed_tokens":0,"max_status_age_seconds":0,
            "local_spool_bytes":0}
        archive = {"sealed_pending_bytes":0,"oldest_sealed_seconds":0,"journal_bytes_committed":0,
            "object_bytes_committed":0,"evicted_bytes":0,"committed_segments":0,"holes":0,"missing_progress":0,
            "open_journal_bytes":0}
        for cohort in deployment["cohorts"]:
            name = role+"-"+cohort["id"]
            directory = Path(settings["data_root"])/name
            path = directory/"status.json"
            compact = directory/"summary.json"
            if compact.exists() and (not path.exists() or compact.stat().st_mtime_ns >= path.stat().st_mtime_ns):
                path = compact
            if not path.exists():
                result["stale_instances"].append(name)
                continue
            status = load(path)
            age = (time.time_ns()-status["utc_ns"])/1e9
            if age > settings["node_defaults"]["metrics_seconds"]*3:
                result["stale_instances"].append(name)
            aggregate["max_status_age_seconds"] = max(aggregate["max_status_age_seconds"],age)
            aggregate["instances"] += 1
            for key in ("raw_received","raw_bytes","pending_bytes","emits","parse_errors"):
                aggregate[key] += status.get(key,0)
            aggregate["local_spool_bytes"] += status.get("local_spool_bytes",0)
            if "published_markets" in status:
                for key in ("written_observations","book_seen_observations","durable_baseline_observations","ready_observations",
                        "ready_tokens","published_tokens","published_markets","crossed_tokens"):
                    aggregate[key] += status[key]
                states = []
            else:
                states = status.get("token_states",[])
            for output,key in (("written_observations","written"),("book_seen_observations","book_seen"),
                    ("durable_baseline_observations","baseline_durable"),("ready_observations","ready")):
                aggregate[output] += sum(bool(s.get(key)) for s in states)
            aggregate["ready_tokens"] += len({s["token"] for s in states if s["ready"]})
            for market,books in status.get("books",{}).items():
                good = all(b["ready"] and not b["crossed"] for b in books.values()) and len(books)==2
                aggregate["published_markets"] += good
                aggregate["published_tokens"] += 2*good
                aggregate["crossed_tokens"] += sum(b["crossed"] for b in books.values())
            if (directory/"archive-progress.json").exists():
                progress = load(directory/"archive-progress.json")
                for key in ("journal_bytes_committed","object_bytes_committed","evicted_bytes","committed_segments"):
                    archive[key] += progress[key]
                archive["holes"] += sum(len(s.get("holes_before_last_commit",s["holes"])) for s in progress["streams"].values())
            else:
                archive["missing_progress"] += 1
            # A long archive batch can delay its progress receipt. Count actual
            # uncommitted sealed files rather than showing its stale zero.
            for path in (directory/"journal").glob("*.journal"):
                if not path.with_suffix(".archive.json").exists():
                    try:
                        stat = path.stat()
                    except FileNotFoundError:
                        continue
                    archive["sealed_pending_bytes"] += stat.st_size
                    archive["oldest_sealed_seconds"] = max(archive["oldest_sealed_seconds"],time.time()-stat.st_mtime)
            for path in (directory/"journal").glob("*.open"):
                try:
                    archive["open_journal_bytes"] += path.stat().st_size
                except FileNotFoundError:
                    pass
        result["roles"][role],result["archive"][role] = aggregate,archive
    atomic_json(root/"health.json",result)
    with (root/"health-history.jsonl").open("a") as out:
        out.write(json.dumps(result,separators=(",",":"))+"\n")
    return result


async def supervise(root):
    from lobv2.catalog_feed import HTTPBudget,consume_outboxes,validate_pending,incremental_new_markets,revalidate_known_ids
    from lobv2.catalog_owner import CatalogOwner
    budget=HTTPBudget(load(root/'settings.json').get('catalog_requests_per_second',4))
    # Restore durable membership before waiting for a potentially long full scan.
    if (root/'universe.json').exists():await asyncio.to_thread(enroll,root,load(root/'universe.json'))
    async def lifecycle():
        while True:
            try:
                await asyncio.to_thread(consume_outboxes,root,load(root/'settings.json'))
                await validate_pending(root,budget)
                delta,watermark=await incremental_new_markets(root,budget)
                owner=CatalogOwner(root)
                revision=owner.metadata('revision',0);owner.close()
                head=load(root/'catalog-head.json') if (root/'catalog-head.json').exists() else {}
                if delta['markets'] or delta['excluded'] or revision!=head.get('catalog_revision'):
                    await asyncio.to_thread(enroll,root,delta)
                owner=CatalogOwner(root)
                with owner.db:owner.set_metadata('incremental_gamma_id',watermark)
                owner.close()
                atomic_json(root/'incremental-discovery-status.json',{'utc_ns':time.time_ns(),'status':'COMPLETE','success_watermark':watermark})
            except Exception as exc:
                atomic_json(root/'incremental-discovery-status.json',{'utc_ns':time.time_ns(),'status':'FAILED_RETRY_PENDING','error':repr(exc)})
            await asyncio.sleep(10)
    async def sweep(events):
        label = "events" if events else "markets"
        while True:
            try:
                from lobv2.catalog import acknowledge_discovery_cache
                cache=root/(label+'-sweep-pages.sqlite')
                scope = await discover(events=events,progress_path=root/(label+"-sweep-progress.json"),budget=budget,resume_path=cache)
                atomic_json(root/"catalogs"/(label+"-"+str(scope["sweep_end_ns"])+".json"),scope)
                atomic_json(root/("events-universe.json" if events else "universe.json"),scope)
                await asyncio.to_thread(enroll,root,scope)
                await asyncio.to_thread(acknowledge_discovery_cache,cache)
                atomic_json(root/(label+"-discovery-status.json"),{"utc_ns":time.time_ns(),"status":"COMPLETE",
                    "markets":len(scope["markets"]),"unresolved":len(scope["unresolved"]),"full_universe_sweep":scope["full_universe_sweep"]})
                delay=load(root/"settings.json").get("discovery_seconds",120)
            except Exception as exc:
                atomic_json(root/(label+"-discovery-status.json"),{"utc_ns":time.time_ns(),"status":"FAILED_RETRY_PENDING","error":repr(exc)})
                delay=30
            await asyncio.sleep(delay)
    async def revalidate():
        while True:
            try:
                delta,receipt=await revalidate_known_ids(root,budget)
                if delta['markets'] or delta['excluded']:await asyncio.to_thread(enroll,root,delta)
                owner=CatalogOwner(root)
                with owner.db:owner.set_metadata('revalidation_after_id',receipt['after_id'])
                owner.close()
                atomic_json(root/'identifier-revalidation-status.json',receipt)
            except Exception as exc:
                atomic_json(root/'identifier-revalidation-status.json',{'utc_ns':time.time_ns(),'status':'FAILED_RETRY_PENDING','error':repr(exc)})
            await asyncio.sleep(10)
    await asyncio.gather(sweep(False),sweep(True),lifecycle(),revalidate())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command",choices=("enroll","instance","supervise","health"))
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--instance")
    args = parser.parse_args()
    if args.command == "enroll":
        result = enroll(args.root,load(args.root/"universe.json"))
        print(json.dumps({"cohorts":len(result["cohorts"]),"markets":len(result["ownership"])}))
    elif args.command == "instance":
        instance = load(args.root/"instances"/(args.instance+".json"))
        instance['config'].update(catalog_control_file=str(args.root/'controls'/(args.instance.split('-',1)[1]+'.json')),
            catalog_head_file=str(args.root/'catalog-head.json'),scope_file=instance['scope_file'])
        asyncio.run(run_node(instance["role"],instance["node_id"],instance["config"],load(Path(instance["scope_file"])),instance["root"]))
    elif args.command == "health":
        print(json.dumps(health(args.root),indent=2))
    else:
        asyncio.run(supervise(args.root))


if __name__ == "__main__":
    main()
