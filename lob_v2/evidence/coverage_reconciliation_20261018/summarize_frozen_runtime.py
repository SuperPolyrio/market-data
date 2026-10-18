"""Explain failed coverage from retained node cuts, without polling production."""
import collections
import hashlib
import json
from pathlib import Path
import sys
import zstandard as zstd

run=Path(sys.argv[1]);manifest=json.loads((run/'coverage_run.json').read_bytes());rows=[]
for name,entry in manifest['inputs'].items():
    if not name.startswith('nodes/') or '/archive-' in name:continue
    raw=zstd.ZstdDecompressor().decompress((run/entry['path']).read_bytes())
    assert hashlib.sha256(raw).hexdigest()==entry['sha256']
    s=json.loads(raw)
    if 'producer' not in s:continue
    states=s.get('token_states',[])
    rows.append({'node':s['producer'],'stream':s['stream'],'utc_ns':s['utc_ns'],
        'source_sha256':s.get('provenance',{}).get('source_sha256'),
        'token_session_observations':len(states),'active_market_routes':len(s.get('active_source_routes',[])),
        'recovery_budget_wait_cumulative':s.get('stage_metrics',{}).get('counters',{}).get('recovery_budget_wait',0),
        'unmatched_or_ambiguous_text_pong_cumulative':s.get('stage_metrics',{}).get('counters',{}).get('unmatched_or_ambiguous_text_pong',0),
        'journal_pending_bytes':s['pending_bytes'],'journal_pending_records':s['pending_records'],
        'journal_oldest_age_ms':s['journal_oldest_age_ms'],'local_spool_bytes':s.get('local_spool_bytes'),
        'publish_latency_ms':s['publish_latency_ms'],'fsync_latency_ms':s['fsync_latency_ms'],
        'ingress_queues':s.get('ingress_queues',{}),'downstream_subscription_queues':s.get('subscription_queues',[]),
        'event_loop_lag_ms':s.get('event_loop_lag_ms')})
summary={}
for role in ('P','Q','G'):
    subset=[r for r in rows if r['node'].startswith(role+'-')]
    latency=[r['publish_latency_ms']['p99'] for r in subset if r['publish_latency_ms'].get('n')]
    summary[role]={'nodes':len(subset),'nodes_without_token_sessions':sum(not r['token_session_observations'] for r in subset) if role!='G' else None,
        'nodes_with_budget_wait_cumulative':sum(r['recovery_budget_wait_cumulative']>0 for r in subset),
        'nodes_with_publish_latency_samples':len(latency),
        'nodes_with_publish_p99_over_50ms':sum(x>50 for x in latency),
        'node_publish_p99_range_ms':[min(latency),max(latency)] if latency else None,
        'journal_pending_bytes_at_local_cuts':sum(r['journal_pending_bytes'] for r in subset),
        'source_builds':dict(collections.Counter(r['source_sha256'] for r in subset))}
result={'coverage_run_id':manifest['coverage_run_id'],'catalog_revision':manifest['catalog_revision'],
    'summary':summary,'rows':rows,
    'limits':['Process cumulative budget waits do not prove a particular missing token is currently blocked on a lease.',
        'Latency samples are each process bounded sample, not one shared time window or a pooled fleet p99.',
        'A queue cut does not establish drain rate or persistent backlog.',
        'No raw reconstruction or remote completeness gate is upgraded by these diagnostics.']}
(Path(__file__).parent/'failure-diagnostics.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(summary,indent=2))
