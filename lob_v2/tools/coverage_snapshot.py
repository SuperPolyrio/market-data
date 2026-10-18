#!/usr/bin/env python3
"""Reproducible full-universe reconciliation at one catalog revision and a vector of local cuts.

No rolling count is an acceptance input. Capture is bounded by the existing catalog
publication lock; WS ingestion and journal writers continue. Different process cuts
are explicit, never represented as a globally atomic or interval-coverage proof.
"""
import argparse
from collections import Counter
import fcntl
import hashlib
import json
from pathlib import Path
import sys
import time
import uuid

PACKAGE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PACKAGE/(".vendor312" if sys.version_info >= (3,12) else ".vendor")), str(PACKAGE)]
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd
from lobv2.journal import atomic_json
from lobv2.wire import json_bytes

ROLES = ('P', 'Q', 'G')
LANES = ('P0', 'P1', 'Q0', 'Q1')
pa.set_cpu_count(2)
pa.set_io_thread_count(2)


def load(path):
    data = Path(path).read_bytes()
    if str(path).endswith('.zst'):
        data = zstd.ZstdDecompressor().decompress(data)
    return orjson.loads(data)


def preserve(output, name, data, inputs):
    path = output/'inputs'/(name+'.zst')
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as file:
        file.write(zstd.ZstdCompressor(level=3).compress(data))
    inputs[name] = {'path':str(path.relative_to(output)), 'sha256':hashlib.sha256(data).hexdigest(), 'bytes':len(data)}


def capture(root, output, timeout=95):
    """Hold catalog publication, not reception, until fresh local snapshots arrive."""
    output.mkdir(parents=True, exist_ok=False)
    run = {'coverage_run_id':str(uuid.uuid4()), 'requested_ns':time.time_ns(),
           'audit_source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
           'cut_contract':'ONE_COMMITTED_CATALOG_WITH_EXPLICIT_PROCESS_LOCAL_CUTS',
           'global_atomic_cut':False, 'interval_coverage':'NOT_EVALUATED',
           'upstream_native_event_completeness':'UNKNOWN', 'inputs':{}, 'nodes':{}}
    preserve(output,'audit_source.py',Path(__file__).read_bytes(),run['inputs'])
    started = time.monotonic()
    with (root/'catalog-owner.lock').open('a') as lock:
        while True:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic()-started > timeout:
                    raise TimeoutError('CATALOG_PUBLICATION_LOCK_TIMEOUT')
                time.sleep(.2)
        run['catalog_lock_acquired_ns'] = time.time_ns()
        for name in ('catalog-head.json','catalog-snapshot.json','deployment.json','settings.json','universe.json'):
            preserve(output, name, (root/name).read_bytes(), run['inputs'])
        head = load(output/run['inputs']['catalog-head.json']['path'])
        catalog = load(output/run['inputs']['catalog-snapshot.json']['path'])
        deployment = load(output/run['inputs']['deployment.json']['path'])
        settings = load(output/run['inputs']['settings.json']['path'])
        revision = head['catalog_revision']
        if catalog['catalog_revision'] != revision or deployment['catalog_revision'] != revision or catalog['membership_digest'] != head['membership_digest']:
            raise ValueError('UNCOMMITTED_CATALOG_PUBLICATION')
        run.update(catalog_revision=revision, membership_digest=head['membership_digest'],
                   scope_policy_version=head['scope_policy_version'])
        for cohort in deployment['cohorts']:
            name = cohort['id']
            for relative in ('scopes/'+name+'.json','controls/'+name+'.json'):
                preserve(output, relative, (root/relative).read_bytes(), run['inputs'])
            for role in ROLES:
                node = role+'-'+name
                preserve(output, 'instances/'+node+'.json', (root/'instances'/(node+'.json')).read_bytes(), run['inputs'])
                run['nodes'][node] = {'cohort':name,'role':role}
        del catalog, deployment
        run['capture_started_ns'] = time.time_ns()
        deadline = time.monotonic()+timeout
        pending = set(run['nodes'])
        cache = {}
        plant_cut_floor = None
        while pending and time.monotonic() < deadline:
            gateways_pending=any(node.startswith('G-') for node in pending)
            if not gateways_pending and plant_cut_floor is None:
                plant_cut_floor=max((item.get('status_utc_ns',run['capture_started_ns'])
                    for node,item in run['nodes'].items() if node.startswith('G-')),default=run['capture_started_ns'])
            for node in sorted(pending):
                if gateways_pending and not node.startswith('G-'):continue
                path = Path(settings['data_root'])/node/'status.json'
                try:
                    stamp = path.stat().st_mtime_ns
                    if cache.get(node, (None,))[0] == stamp:
                        continue
                    data = path.read_bytes()
                    status = orjson.loads(data)
                    cache[node] = (stamp, data)
                    # Legacy snapshots are preserved as explicit evidence failures.
                    if status['utc_ns'] < run['capture_started_ns']:
                        continue
                    if not node.startswith('G-') and status['utc_ns'] < plant_cut_floor:
                        continue
                    if status.get('catalog_revision') not in (None, revision):
                        continue
                    preserve(output, 'nodes/'+node+'.json', data, run['inputs'])
                    run['nodes'][node]['captured_ns'] = time.time_ns()
                    run['nodes'][node]['status_utc_ns'] = status['utc_ns']
                    pending.remove(node)
                except (OSError, ValueError, KeyError) as exc:
                    run['nodes'][node]['read_error'] = repr(exc)
            if pending:
                time.sleep(.25)
        for node in sorted(pending):
            run['nodes'][node]['capture_error'] = 'FRESH_MATCHING_REVISION_NOT_OBTAINED'
            if node in cache:
                preserve(output, 'nodes/'+node+'.json', cache[node][1], run['inputs'])
        run['capture_finished_ns'] = time.time_ns()
    run['catalog_lock_released_ns'] = time.time_ns()
    # Archive completion is observed separately; it cannot upgrade capture coverage.
    for node in run['nodes']:
        path = Path(settings['data_root'])/node/'archive-progress.json'
        if path.exists():
            preserve(output, 'archive/'+node+'.json', path.read_bytes(), run['inputs'])
    run['capture_unavailable_nodes'] = sorted(pending)
    atomic_json(output/'coverage_run.json',run)
    return run


def cut_errors(status, run, node):
    errors=[]
    if not status:return ['STATUS_MISSING']
    if status.get('producer') != node:errors.append('PRODUCER_IDENTITY_MISMATCH')
    if status.get('catalog_revision') != run['catalog_revision']:errors.append('CATALOG_REVISION_MISMATCH')
    if status.get('scope_policy_version') != run['scope_policy_version']:errors.append('SCOPE_POLICY_MISMATCH')
    if not run['capture_started_ns'] <= status.get('utc_ns',0) <= run['capture_finished_ns']:
        errors.append('OUTSIDE_CAPTURE_WINDOW')
    if status.get('catalog_apply_pending'):errors.append('CATALOG_APPLICATION_IN_PROGRESS')
    offsets=[status.get(k) for k in ('published_offset','applied_offset','durable_offset','written_offset','journal_offset')]
    if any(v is None for v in offsets):errors.append('EXACT_COMMIT_CUT_MISSING')
    elif offsets != sorted(offsets):errors.append('COMMIT_CUT_ORDER_INVALID')
    for key in ('published','applied','durable'):
        receipt=status.get('cut_receipts',{}).get(key,{})
        if (receipt.get('log_generation') != status.get('stream') or
                receipt.get('record_sequence') != status.get(key+'_offset') or
                not receipt.get('log_id','').startswith(node+':')):
            errors.append('INVALID_'+key.upper()+'_CUT_RECEIPT')
    if status.get('failure') or status.get('admission_failure'):errors.append('JOURNAL_FAILURE')
    return errors


def index_states(status):
    active={(r['market'],r['lane']):r for r in status.get('active_source_routes',[])}
    result={}; candidates=0; duplicates=set()
    for state in status.get('token_states',[]):
        route=active.get((state['market'],state['lane']))
        if not route or (route['shard'],route.get('connection')) != (state['shard'],state.get('connection')):
            candidates+=1
            continue
        key=(state['market'],state['token'],state['lane'])
        if key in result:duplicates.add(key)
        result[key]=state
    return result, candidates, duplicates


def token_row(market, token, role, lane, cohort, assigned, status, states, duplicates, common, run):
    key=(market['market'],token,lane)
    state=states.get(key,{})
    errors=list(common)
    if not assigned:errors.append('DESIRED_UNASSIGNED')
    if key in duplicates:errors.append('DUPLICATE_ACTIVE_STATE')
    if not state:errors.append('NO_CURRENT_ACTIVE_ROUTE')
    baseline=state.get('baseline_ref') or {}
    receipt=state.get('baseline_capture_durable_ref') or {}
    current=bool(state and state.get('connection') and state.get('connection_epoch') is not None)
    written=bool(current and state.get('written') and state.get('send_attempt_id') and
                 state.get('write_succeeded_ns',0) >= state.get('assigned_ns',1))
    book=bool(current and state.get('book_seen') and state.get('baseline_session') == state.get('connection') and
              baseline.get('connection') == state.get('connection') and baseline.get('capture_run') == status.get('stream') and
              baseline.get('plant') == status.get('producer'))
    offset=baseline.get('capture_offset')
    durable=bool(book and state.get('baseline_durable') and offset is not None and
                 receipt.get('log_generation') == status.get('stream') and receipt.get('record_sequence') == offset and
                 receipt.get('log_id') == status.get('producer','')+':capture' and
                 offset <= status.get('applied_offset',-1) <= status.get('durable_offset',-1))
    if current and not written:errors.append('SUBSCRIPTION_WRITE_EVIDENCE_MISSING')
    if current and not book:errors.append('CURRENT_NATIVE_BASELINE_MISSING')
    if book and not durable:errors.append('BASELINE_DURABILITY_EVIDENCE_INVALID')
    errors.extend(state.get('eligibility_reasons',[]))
    if state and not state.get('ready'):errors.append(state.get('status','NOT_READY'))
    if state.get('ready') and not (written and book and durable):errors.append('READY_EVIDENCE_CONTRADICTION')
    errors=list(dict.fromkeys(errors))
    ready=bool(state.get('ready') and written and book and durable and not errors)
    return {'market':market['market'],'token':token,'plant':role,'lane':lane,'cohort':cohort,
        'catalog_revision':run['catalog_revision'],'assigned':assigned,'session_assigned':current,
        'written':written,'book_seen':book,'baseline_durable':durable,'ready_reported':bool(state.get('ready')),
        'qualified':ready,'connection':state.get('connection'),'connection_epoch':state.get('connection_epoch'),
        'physical_shard':state.get('shard'),'logical_shard':state.get('logical_shard'),
        'stream':status.get('stream'),'status_utc_ns':status.get('utc_ns'),
        'committed_input_cut':status.get('applied_offset'),'baseline_offset':offset,
        'send_attempt_id':state.get('send_attempt_id'),'baseline_ref_json':json.dumps(baseline),
        'baseline_durable_ref_json':json.dumps(receipt),'all_reasons':errors,
        'primary_reason':errors[0] if errors else 'READY'}


def market_row(market, rows, gateway, common, run, plants=None):
    pairs={}
    for role in ('P','Q'):
        for lane in (0,1):
            pair=[r for r in rows if r['plant']==role and r['lane']==lane]
            pairs[role+str(lane)]=len(pair)==2 and all(r['qualified'] for r in pair) and len({(r['connection'],r['physical_shard'],r['connection_epoch']) for r in pair})==1
    books=gateway.get('books',{}).get(market['market'],{})
    observed=set(books)==set(market['assets']) and all(b.get('ready') and not b.get('crossed') for b in books.values())
    source=gateway.get('committed_sources',{}).get(market['market'],{})
    errors=list(common)
    if not observed:errors.append('GATEWAY_PAIR_NOT_READY')
    if observed and (not source or source.get('market_seq') != gateway.get('committed_seq',{}).get(market['market']) or
                     set(source.get('scope_assets',[])) != set(market['assets']) or
                     source.get('market_version') != market['version']):
        errors.append('COMMITTED_SOURCE_EVIDENCE_MISSING')
    if source and source.get('route',{}).get('plant_session_id') != gateway.get('sessions',{}).get(source.get('route',{}).get('plant')):
        errors.append('GATEWAY_SOURCE_SESSION_MISMATCH')
    if source:
        route=source.get('route',{})
        producer=gateway.get('producer','')
        cohort=producer.split('-',1)[1] if '-' in producer else ''
        if (route.get('plant') not in ('P-'+cohort,'Q-'+cohort) or route.get('lane') not in (0,1)
                or not route.get('connection') or not route.get('plant_stream')
                or route.get('connection_epoch') is None):
            errors.append('GATEWAY_SOURCE_IDENTITY_INVALID')
        parent=source.get('source_plant_record')
        if parent and (parent.get('stream')!=route.get('plant_stream') or not parent.get('offset')):
            errors.append('GATEWAY_PARENT_RECORD_INVALID')
        if not source.get('plant_market_seq',source.get('plant_cut')):
            errors.append('GATEWAY_PARENT_MARKET_CUT_MISSING')
        if observed and plants is not None:
            plant=plants.get(route.get('plant','')[:1],{})
            parent_seq=source.get('plant_market_seq',source.get('plant_cut',0))
            if (route.get('plant_stream') != plant.get('stream') or
                    parent_seq > plant.get('committed_seq',{}).get(market['market'],0) or
                    parent and parent.get('offset',0) > plant.get('applied_offset',-1)):
                errors.append('GATEWAY_PARENT_OUTSIDE_CAPTURED_PLANT_PREFIX')
    qualified=observed and not errors
    missing=[key for key in LANES if not pairs[key]]
    return {'market':market['market'],'tokens':market['assets'],'catalog_revision':run['catalog_revision'],
        **pairs,'any_complete_source':any(pairs.values()),'both_plants':any(pairs[k] for k in ('P0','P1')) and any(pairs[k] for k in ('Q0','Q1')),
        'all_four_lanes':all(pairs.values()),'gateway_ready_reported':observed,'gateway_qualified':qualified,
        'missing_lanes':missing,'gateway_reasons':errors,'gateway_stream':gateway.get('stream'),
        'gateway_market_seq':gateway.get('committed_seq',{}).get(market['market']),
        'gateway_committed_source_json':json.dumps(source)}


TOKEN_SCHEMA=pa.schema([(k,t) for k,t in {
    'market':pa.string(),'token':pa.string(),'plant':pa.string(),'lane':pa.int8(),'cohort':pa.string(),
    'catalog_revision':pa.uint64(),**{k:pa.bool_() for k in ('assigned','session_assigned','written','book_seen','baseline_durable','ready_reported','qualified')},
    'connection':pa.string(),'connection_epoch':pa.int64(),'physical_shard':pa.int64(),'logical_shard':pa.int64(),
    'stream':pa.string(),'status_utc_ns':pa.int64(),'committed_input_cut':pa.int64(),'baseline_offset':pa.int64(),
    'send_attempt_id':pa.string(),'baseline_ref_json':pa.string(),'baseline_durable_ref_json':pa.string(),
    'all_reasons':pa.list_(pa.string()),'primary_reason':pa.string()}.items()])
MARKET_SCHEMA=pa.schema([(k,t) for k,t in {
    'market':pa.string(),'tokens':pa.list_(pa.string()),'catalog_revision':pa.uint64(),
    **{k:pa.bool_() for k in (*LANES,'any_complete_source','both_plants','all_four_lanes','gateway_ready_reported','gateway_qualified')},
    'missing_lanes':pa.list_(pa.string()),'gateway_reasons':pa.list_(pa.string()),'gateway_stream':pa.string(),
    'gateway_market_seq':pa.int64(),'gateway_committed_source_json':pa.string()}.items()])


def reconcile(output):
    run=load(output/'coverage_run.json')
    def frozen(name, default=None):
        entry=run['inputs'].get(name)
        if entry is None:return default
        path=output/entry['path'];data=zstd.ZstdDecompressor().decompress(path.read_bytes())
        if hashlib.sha256(data).hexdigest()!=entry['sha256']:raise ValueError('INPUT_DIGEST_MISMATCH:'+name)
        return orjson.loads(data)
    catalog=frozen('catalog-snapshot.json');deployment=frozen('deployment.json');census=frozen('universe.json')
    members=catalog['markets'];desired={m['market']:m for m in members}
    membership=[(m['market'],m['version'],m['assets'],m['asset_id_kind']) for m in members]
    if hashlib.sha256(json_bytes(membership)).hexdigest()!=run['membership_digest']:raise ValueError('CATALOG_DIGEST_MISMATCH')
    owners={};duplicates=[];scope_errors=[];token_owners={};token_conflicts=[]
    groups={c['id']:[] for c in deployment['cohorts']};groups['UNASSIGNED']=[]
    for c in deployment['cohorts']:
        scope=frozen('scopes/'+c['id']+'.json');control=frozen('controls/'+c['id']+'.json')
        if control['catalog_revision'] != run['catalog_revision'] or hashlib.sha256(json_bytes(scope['markets'])).hexdigest()!=control['cohort_scope_digest']:
            scope_errors.append(c['id'])
        for m in scope['markets']:
            if m['market'] in owners:duplicates.append(m['market'])
            owners[m['market']]=(c['id'],m)
            indexed=deployment.get('ownership',{}).get(m['market'],{})
            if indexed.get('cohort')!=c['id'] or indexed.get('assets')!=m['assets'] or indexed.get('version')!=m['version']:
                scope_errors.append(c['id'])
    for m in members:
        for token in m['assets']:
            if token in token_owners:token_conflicts.append({'token':token,'markets':[token_owners[token],m['market']]})
            token_owners[token]=m['market']
        owner=owners.get(m['market'])
        groups[owner[0] if owner else 'UNASSIGNED'].append(m)
    census_ids={m['market'] for m in census['markets']}
    exclusions={r['market']:r for r in catalog['scope_exclusions']+catalog['terminal_evidence']}
    unadmitted={r['market']:r for r in catalog['unadmitted_groups']}
    catalog_rows=[]
    for m in sorted(set(desired)|set(owners)|census_ids|set(exclusions)):
        reason='DESIRED_UNASSIGNED' if m in desired and m not in owners else 'ASSIGNED_NOT_DESIRED_WITH_EVIDENCE' if m in owners and m not in desired and m in exclusions else 'ASSIGNED_NOT_DESIRED_UNEXPLAINED' if m in owners and m not in desired else 'DESIRED_NOT_IN_LATEST_CENSUS_RETAINED' if m in desired and m not in census_ids else 'IN_SCOPE' if m in desired else 'OUTSIDE_DESIRED'
        catalog_rows.append({'market':m,'desired':m in desired,'assigned':m in owners,'in_latest_census':m in census_ids,
            'cohort':owners[m][0] if m in owners else None,'reason':reason,'exclusion_evidence_json':json.dumps(exclusions.get(m)),
            'unadmitted_evidence_json':json.dumps(unadmitted.get(m))})
    pq.write_table(pa.Table.from_pylist(catalog_rows),output/'catalog_reconciliation.parquet',compression='zstd')
    catalog_counts=dict(Counter(r['reason'] for r in catalog_rows))
    atomic_json(output/'unresolved_catalog.json',{
        'pending_validation':catalog.get('pending_validation',[]),
        'full_scan_unresolved':census.get('unresolved',[]),
        'unadmitted_groups':catalog.get('unadmitted_groups',[]),
        'contract':'UNKNOWN_IDENTITIES_ARE_SEPARATE_NOT_INVENTED_TOKEN_IDS'})
    counters={k:Counter() for k in LANES};primary={k:Counter() for k in LANES};all_reasons={k:Counter() for k in LANES}
    market_counts=Counter();token_counts=Counter();node_rows=[];candidates=0;duplicate_states=0;row_count=0
    with pq.ParquetWriter(output/'token_state_at_cut.parquet',TOKEN_SCHEMA,compression='zstd') as tw, pq.ParquetWriter(output/'market_gap_attribution.parquet',MARKET_SCHEMA,compression='zstd') as mw, pq.ParquetWriter(output/'candidate_observations.parquet',TOKEN_SCHEMA,compression='zstd') as cw:
        for cohort, markets in groups.items():
            statuses={r:frozen('nodes/'+r+'-'+cohort+'.json',{}) for r in ROLES}
            errors={r:cut_errors(statuses[r],run,r+'-'+cohort) for r in ROLES}
            indexes={r:index_states(statuses[r]) for r in ('P','Q')}
            candidate_rows=[]
            for role in ('P','Q'):
                active={(r['market'],r['lane']):r for r in statuses[role].get('active_source_routes',[])}
                for state in statuses[role].get('token_states',[]):
                    route=active.get((state['market'],state['lane']),{})
                    if (route.get('shard'),route.get('connection'))==(state['shard'],state.get('connection')):continue
                    market=desired.get(state['market'])
                    if not market or state['token'] not in market['assets']:continue
                    key=(state['market'],state['token'],state['lane'])
                    candidate_rows.append(token_row(market,state['token'],role,state['lane'],cohort,True,
                        statuses[role],{key:state},set(),errors[role]+['CANDIDATE_NOT_ACTIVE_ROUTE'],run))
            if candidate_rows:cw.write_table(pa.Table.from_pylist(candidate_rows,schema=TOKEN_SCHEMA))
            for role,s in statuses.items():
                if cohort=='UNASSIGNED':continue
                node=role+'-'+cohort
                node_rows.append({'node':node,'catalog_revision':s.get('catalog_revision'),'utc_ns':s.get('utc_ns'),
                    'mono_ns':s.get('mono_ns'),'stream':s.get('stream'),'errors':errors[role],
                    'published_offset':s.get('published_offset'),'applied_offset':s.get('applied_offset'),
                    'durable_offset':s.get('durable_offset'),'written_offset':s.get('written_offset'),
                    'journal_offset':s.get('journal_offset'),'cut_receipts':s.get('cut_receipts'),
                    'provenance':s.get('provenance'),'recovery_budget_wait':s.get('stage_metrics',{}).get('counters',{}).get('recovery_budget_wait')})
                if role!='G':candidates+=indexes[role][1];duplicate_states+=len(indexes[role][2])
            token_rows=[];market_rows=[]
            for market in markets:
                rows=[]
                owner=owners.get(market['market'])
                identity_ok=owner and all(owner[1][k]==market[k] for k in ('assets','version','asset_id_kind'))
                for role in ('P','Q'):
                    common=errors[role].copy()
                    if owner and not identity_ok:common.append('ASSIGNED_IDENTITY_MISMATCH')
                    if cohort in scope_errors:common.append('ROUTING_PLAN_MISMATCH')
                    if market['market'] in unadmitted:common.append(unadmitted[market['market']].get('reason','CATALOG_UNADMITTED'))
                    for lane in (0,1):
                        for token in market['assets']:
                            row=token_row(market,token,role,lane,cohort,bool(owner),statuses[role],indexes[role][0],indexes[role][2],common,run)
                            rows.append(row);label=role+str(lane)
                            for key in ('assigned','session_assigned','written','book_seen','baseline_durable','ready_reported','qualified'):counters[label][key]+=row[key]
                            primary[label][row['primary_reason']]+=1;all_reasons[label].update(row['all_reasons'])
                gateway_errors=errors['G']+(['CATALOG_UNADMITTED'] if market['market'] in unadmitted else [])
                result=market_row(market,rows,statuses['G'],gateway_errors,run,statuses)
                for token in market['assets']:
                    observations=[r for r in rows if r['token']==token]
                    for key in ('assigned','session_assigned','written','book_seen','baseline_durable','qualified'):
                        token_counts[key]+=any(r[key] for r in observations)
                token_rows.extend(rows);market_rows.append(result)
                for k in (*LANES,'any_complete_source','both_plants','all_four_lanes','gateway_ready_reported','gateway_qualified'):market_counts[k]+=result[k]
                market_counts['gateway_parent_prefix_failures']+=('GATEWAY_PARENT_OUTSIDE_CAPTURED_PLANT_PREFIX' in result['gateway_reasons'])
            if token_rows:tw.write_table(pa.Table.from_pylist(token_rows,schema=TOKEN_SCHEMA));row_count+=len(token_rows)
            if market_rows:mw.write_table(pa.Table.from_pylist(market_rows,schema=MARKET_SCHEMA))
    atomic_json(output/'node_cuts.json',node_rows)
    bad_nodes=[r['node'] for r in node_rows if r['errors']]
    cut_times=[r['utc_ns'] for r in node_rows if r['utc_ns'] is not None]
    expected_rows=sum(len(m['assets']) for m in members)*4
    scope_errors=sorted(set(scope_errors))
    gates={'denominator_rows_complete':row_count==expected_rows and len(desired)==len(members),
        'unique_market_and_token_identity':not duplicates and not token_conflicts and not duplicate_states,
        'routing_plan_matches_catalog':not scope_errors,
        'all_desired_assigned':set(desired)<=set(owners),
        'retired_assignments_explained':catalog_counts.get('ASSIGNED_NOT_DESIRED_UNEXPLAINED',0)==0,
        'same_revision_fresh_exact_node_cuts':not bad_nodes,
        'gateway_parent_prefixes_closed':market_counts['gateway_parent_prefix_failures']==0,
        'all_desired_have_complete_source':market_counts['any_complete_source']==len(desired),
        'all_desired_both_plants':market_counts['both_plants']==len(desired),
        'all_desired_four_lanes':market_counts['all_four_lanes']==len(desired),
        'all_desired_gateway_qualified':market_counts['gateway_qualified']==len(desired)}
    report={'coverage_run_id':run['coverage_run_id'],'catalog_revision':run['catalog_revision'],'membership_digest':run['membership_digest'],
        'reconciler_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'capture_started_ns':run['capture_started_ns'],'capture_finished_ns':run['capture_finished_ns'],
        'local_cut_min_ns':min(cut_times) if cut_times else None,
        'local_cut_max_ns':max(cut_times) if cut_times else None,
        'local_cut_span_ms':(max(cut_times)-min(cut_times))/1e6 if cut_times else None,
        'cut_contract':run['cut_contract'],'global_atomic_cut':False,
        'desired_markets':len(desired),'desired_tokens':len(token_owners),'token_lane_rows':row_count,'expected_token_lane_rows':expected_rows,
        'assigned_markets':len(owners),'census_markets':len(census_ids),'catalog_reconciliation':catalog_counts,
        'pending_identity_validation':len(catalog.get('pending_validation',[])),
        'full_scan_unresolved_rows':len(census.get('unresolved',[])),
        'unadmitted_market_groups':len(unadmitted),
        'duplicate_market_owners':duplicates,'token_identity_conflicts':token_conflicts,'duplicate_active_states':duplicate_states,
        'routing_plan_mismatch_cohorts':scope_errors,
        'candidate_observations_separate':candidates,'nodes_with_evidence_failure':bad_nodes,
        'lane_counts_contract':'ACTIVE_ROUTE_ONLY; independent candidate observations preserved separately',
        'lanes':counters,'primary_reasons':primary,'all_reasons':all_reasons,'markets':market_counts,
        'active_route_unique_tokens_by_stage':token_counts,
        'gates':{k:'PASS' if v else 'FAIL' for k,v in gates.items()},'acceptance':'PASS' if all(gates.values()) else 'FAIL',
        'time_window_coverage':'NOT_EVALUATED','remote_archive_readback':'NOT_EVALUATED',
        'baseline_validation':'REFERENCE_IDENTITY_GENERATION_AND_LOCAL_DURABLE_CUT',
        'full_native_payload_reconstruction':'NOT_EVALUATED_IN_THIS_COVERAGE_AUDIT',
        'archive_progress':'PRESERVED_SEPARATELY_NOT_USED_TO_UPGRADE_COVERAGE','upstream_native_event_completeness':'UNKNOWN'}
    atomic_json(output/'report.json',report)
    lines=['# Full production coverage reconciliation','',f"Acceptance: **{report['acceptance']}**",'',
        f"Catalog revision: {run['catalog_revision']}; desired: {len(desired):,} markets / {len(token_owners):,} tokens.",
        f"Emitted {row_count:,} / {expected_rows:,} expected token-lane rows.",'',
        'One fixed catalog and explicit process-local committed cuts. This is not a globally atomic cut, time-window coverage, or complete native upstream history.','',
        '| Gate | Result |','| --- | --- |',*[f'| {k} | {v} |' for k,v in report['gates'].items()],'',
        '| Lane | Session | Written | Native baseline | Durable baseline | Qualified |','| --- | ---: | ---: | ---: | ---: | ---: |']
    for lane,c in counters.items():lines.append('| '+lane+' | '+' | '.join(str(c[k]) for k in ('session_assigned','written','book_seen','baseline_durable','qualified'))+' |')
    lines.extend(['','Market qualification: '+json.dumps(dict(market_counts)), '',
        'Inputs are compressed immutable copies with SHA-256 in coverage_run.json. Replay with coverage_snapshot.py --reconcile OUTPUT.',
        'catalog_reconciliation.parquet contains actual membership differences and exclusion evidence; token_state_at_cut.parquet contains every expected token in every lane including unassigned and absent sessions; market_gap_attribution.parquet keeps same-source pairs and committed gateway source references; node_cuts.json records individual process cuts.'])
    (output/'ACCEPTANCE.md').write_text('\n'.join(lines)+'\n')
    return report


def collect(root, output, timeout=95):
    capture(root,output,timeout)
    return reconcile(output)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--deployment',type=Path)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--reconcile',type=Path)
    parser.add_argument('--timeout',type=float,default=95)
    args=parser.parse_args()
    if args.reconcile:report=reconcile(args.reconcile)
    else:
        if not args.deployment or not args.output:parser.error('--deployment and --output are required')
        report=collect(args.deployment,args.output,args.timeout)
    print(json.dumps(report,indent=2))
