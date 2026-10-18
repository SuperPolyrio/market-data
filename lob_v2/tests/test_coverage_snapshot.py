import asyncio
from copy import deepcopy
import importlib.util
from pathlib import Path

from lobv2.node import status
from test_semantics import make_plant,initialize

spec=importlib.util.spec_from_file_location('coverage_snapshot',Path(__file__).parents[1]/'tools/coverage_snapshot.py')
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
M={'market':'m','assets':['1','2'],'version':'1','asset_id_kind':'CLOB_TOKEN_ID'}
RUN={'catalog_revision':8,'scope_policy_version':'p','capture_started_ns':10,'capture_finished_ns':100}


def snapshot():
    return {'producer':'P-0000','stream':'generation','utc_ns':50,'catalog_revision':8,'scope_policy_version':'p',
        'journal_offset':20,'written_offset':20,'durable_offset':20,'applied_offset':20,'published_offset':20,
        'cut_receipts':{k:{'log_generation':'generation','record_sequence':20,'log_id':'P-0000:capture'} for k in ('applied','durable','published')},
        'active_source_routes':[{'market':'m','lane':0,'shard':0,'connection':'new'}]}


def state(token='1',connection='new'):
    return {'market':'m','token':token,'lane':0,'shard':0,'connection':connection,'connection_epoch':1,
        'written':True,'send_attempt_id':'attempt','write_succeeded_ns':30,'assigned_ns':20,
        'book_seen':True,'baseline_session':connection,
        'baseline_ref':{'connection':connection,'capture_run':'generation','plant':'P-0000','capture_offset':15},
        'baseline_capture_durable_ref':{'log_generation':'generation','log_id':'P-0000:capture','record_sequence':15},
        'baseline_durable':True,'ready':True,'eligibility_reasons':[]}


def row(s=None):
    snap=snapshot();s=s or state()
    return audit.token_row(M,'1','P',0,'0000',True,snap,{('m','1',0):s},set(),audit.cut_errors(snap,RUN,'P-0000'),RUN)


def test_missing_assignment_stays_in_denominator_and_fails():
    r=audit.token_row(M,'1','P',0,'UNASSIGNED',False,{}, {},set(),[],RUN)
    assert not r['assigned'] and not r['qualified']
    assert r['primary_reason']=='DESIRED_UNASSIGNED'
    assert 'NO_CURRENT_ACTIVE_ROUTE' in r['all_reasons']


def test_candidate_cannot_overwrite_active_or_merge_pair():
    s=snapshot();s['token_states']=[state(),state(connection='candidate')]
    states,n,duplicates=audit.index_states(s)
    assert n==1 and states[('m','1',0)]['connection']=='new' and not duplicates
    one=row();two={**one,'token':'2','connection':'other'}
    result=audit.market_row(M,[one,two],{},[],RUN)
    assert not result['P0'] and not result['any_complete_source']


def test_current_generation_and_exact_receipt_are_required():
    assert row()['qualified']
    for key,value in [('baseline_session','old'),('baseline_capture_durable_ref',{'log_generation':'old','record_sequence':15}),('baseline_ref',{'capture_run':'generation','plant':'P-0000','connection':'new','capture_offset':21})]:
        s=state();s[key]=value
        assert not row(s)['qualified']


def test_ready_with_reason_cannot_pass_and_old_cut_is_not_current():
    s=state();s['eligibility_reasons']=['PROOF_EXPIRED']
    assert not row(s)['qualified']
    snap=snapshot();snap['utc_ns']=9;snap['catalog_revision']=7;snap['applied_offset']=21
    errors=audit.cut_errors(snap,RUN,'P-0000')
    assert {'OUTSIDE_CAPTURE_WINDOW','CATALOG_REVISION_MISMATCH','COMMIT_CUT_ORDER_INVALID'}<=set(errors)


def test_gateway_working_selection_cannot_replace_committed_source():
    async def run():
        p=make_plant(Path(__import__('tempfile').mkdtemp()))
        p.runtime_config={'metrics_digests':False};p.provenance={}
        initialize(p);await p.journal.sync()
        before=status(p)['committed_sources']['m']
        p.selected['m']=('m',0,1)
        assert status(p)['committed_sources']['m']==before
        assert before['route']['lane']==0 and before['market_seq']==p.committed_seq['m']
        await p.journal.close()
    asyncio.run(run())


def test_gateway_requires_exact_token_set_and_source_session():
    source={'market_seq':4,'market_version':'1','plant_cut':8,'scope_assets':['1','2'],'route':{'plant':'P-0000','plant_session_id':'s',
        'lane':0,'connection':'c','connection_epoch':1,'plant_stream':'plant-generation'}}
    g={'producer':'G-0000','books':{'m':{t:{'ready':True,'crossed':False} for t in ['1','2']}},'committed_seq':{'m':4},
       'committed_sources':{'m':source},'sessions':{'P-0000':'s'}}
    assert audit.market_row(M,[],g,[],RUN)['gateway_qualified']
    g['sessions']['P-0000']='new'
    assert not audit.market_row(M,[],g,[],RUN)['gateway_qualified']
    del g['books']['m']['2']
    assert not audit.market_row(M,[],g,[],RUN)['gateway_ready_reported']


def test_full_reconciliation_is_reproducible_and_keeps_unassigned(tmp_path):
    import hashlib
    import json
    import pyarrow.parquet as pq
    from lobv2.wire import json_bytes
    import pytest
    second={**M,'market':'unassigned','assets':['3','4']}
    members=[M,second]
    digest=hashlib.sha256(json_bytes([(m['market'],m['version'],m['assets'],m['asset_id_kind']) for m in members])).hexdigest()
    catalog={'markets':members,'scope_exclusions':[],'terminal_evidence':[],'unadmitted_groups':[]}
    run={**RUN,'coverage_run_id':'fixture','membership_digest':digest,'cut_contract':'ONE_COMMITTED_CATALOG_WITH_EXPLICIT_PROCESS_LOCAL_CUTS','inputs':{}}
    inputs={'catalog-snapshot.json':catalog,'deployment.json':{'cohorts':[{'id':'0000'}],
        'ownership':{'m':{'cohort':'0000','assets':['1','2'],'version':'1'}}},
        'universe.json':{'markets':members},'scopes/0000.json':{'markets':[M]},
        'controls/0000.json':{'catalog_revision':8,'cohort_scope_digest':hashlib.sha256(json_bytes([M])).hexdigest()}}
    for name,data in inputs.items():audit.preserve(tmp_path,name,json_bytes(data),run['inputs'])
    (tmp_path/'coverage_run.json').write_text(json.dumps(run))
    first=audit.reconcile(tmp_path)
    assert first['desired_tokens']==4 and first['token_lane_rows']==16
    assert first['active_route_unique_tokens_by_stage']['assigned']==2
    assert first['gates']['denominator_rows_complete']=='PASS'
    assert first['gates']['all_desired_assigned']=='FAIL' and first['acceptance']=='FAIL'
    table=pq.read_table(tmp_path/'token_state_at_cut.parquet').to_pylist()
    assert len([r for r in table if r['market']=='unassigned'])==8
    assert all(sum(v.values())==4 for v in first['primary_reasons'].values())
    assert audit.reconcile(tmp_path)==first
    entry=run['inputs']['catalog-snapshot.json'];entry['sha256']='tampered'
    (tmp_path/'coverage_run.json').write_text(json.dumps(run))
    with pytest.raises(ValueError,match='INPUT_DIGEST_MISMATCH'):audit.reconcile(tmp_path)


def test_gateway_parent_must_exist_inside_captured_plant_generation_and_cut():
    source={'market_seq':4,'market_version':'1','plant_market_seq':8,'scope_assets':['1','2'],
        'source_plant_record':{'stream':'pg','offset':12},
        'route':{'plant':'P-0000','plant_session_id':'s','lane':0,'connection':'c','connection_epoch':1,'plant_stream':'pg'}}
    g={'producer':'G-0000','books':{'m':{t:{'ready':True,'crossed':False} for t in ['1','2']}},
        'committed_seq':{'m':4},'committed_sources':{'m':source},'sessions':{'P-0000':'s'}}
    p={'stream':'pg','committed_seq':{'m':8},'applied_offset':12}
    assert audit.market_row(M,[],g,[],RUN,{'P':p})['gateway_qualified']
    for field,bad in [('stream','new'),('committed_seq',{'m':7}),('applied_offset',11)]:
        changed={**p,field:bad}
        assert not audit.market_row(M,[],g,[],RUN,{'P':changed})['gateway_qualified']
