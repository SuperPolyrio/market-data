import asyncio
from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest

from lobv2.catalog_feed import delta_scope
from lobv2.core import Gateway
from lobv2.journal import Journal,read_journal
from lobv2.reader import Session
from lobv2.wire import json_bytes
from test_semantics import make_plant,initialize,FIXTURE,SCOPE

NEW={'market':'new','assets':['3','4'],'version':'1','asset_id_kind':'CLOB_TOKEN_ID','gamma_id':'202'}


def test_dynamic_subscription_preserves_old_book_and_requires_both_current_baselines(tmp_path):
    async def run():
        p=make_plant(tmp_path);initialize(p);await p.journal.sync()
        old_book=p.books[('m',0,0)];old_seq=p.seq['m'];old_route=p.routes['m']
        p.require_source_admission=True;p.admitted_keys.add(('m',0,0))
        _,follow=p.bootstrap();follow['follow_scope']=True
        _,fixed=p.bootstrap(['m'])
        control={'catalog_revision':2,'scope_policy_version':'p','blocked':[],'market_groups':SCOPE['markets']+[NEW]}
        await p.apply_catalog_control(control)
        sent=[]
        class WS:
            async def send(self,message):sent.append(json.loads(message))
        class Lease:
            def release(self):pass
        session=Session(p,0,0,0,1,list(SCOPE['markets']),{},asyncio.Event(),Lease())
        session.connection='c0';session.ws=WS()
        await session.subscribe_more([NEW])
        assert sent[0]['operation']=='subscribe' and sent[0]['assets_ids']==['3','4']
        assert p.books[('m',0,0)] is old_book and p.seq['m']==old_seq and p.routes['m']==old_route
        assert 'new' in follow['markets'] and 'new' not in fixed['markets']
        assert not p.usable(('new',0,0))
        for i,event in enumerate(FIXTURE[:2]):
            event={**event,'market':'new','asset_id':str(i+3)}
            await p.ingest_async(0,0,'c0',10+i,json_bytes(event))
            await p.journal.sync()
            p.consider_admission(('new',0,0));await p.journal.sync()
            p.reconcile(('new',0,0));await p.journal.sync()
            if i==0:assert not p.usable(('new',0,0))
        assert p.usable(('new',0,0))
        assert p.working['new']['3']['ready'] and p.working['new']['4']['ready']
        await p.journal.close()
        records=[r for path in (tmp_path/'P').glob('*.journal') for r in read_journal(path)]
        upsert=next(r.offset for r in records if r.kind=='CATALOG_SCOPE_UPSERT')
        first_raw=next(r.offset for r in records if r.kind=='RAW' and b'"new"' in r.raw)
        assert upsert<first_raw
    asyncio.run(run())


def test_dynamic_identity_change_is_atomic_and_rejected(tmp_path):
    async def run():
        p=make_plant(tmp_path)
        with pytest.raises(ValueError,match='MIGRATION_REQUIRED'):
            await p.apply_catalog_control({'catalog_revision':2,'scope_policy_version':'p','blocked':[],
                'market_groups':[NEW,{**SCOPE['markets'][0],'assets':['5','6']}]})
        assert 'new' not in p.scope and p.catalog_revision==0
        await p.journal.close()
    asyncio.run(run())


def test_incremental_catalog_fills_tail_and_recovers_scope_index_crash(tmp_path,monkeypatch):
    path=Path(__file__).parents[1]/'tools/production.py'
    spec=importlib.util.spec_from_file_location('production_under_test',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    monkeypatch.setattr(module.subprocess,'run',lambda *a,**k:None)
    for name in ['scopes','instances','controls']:(tmp_path/name).mkdir()
    (tmp_path/'settings.json').write_text(json.dumps({'markets_per_cohort':3,'fill_existing_cohorts':True,
        'node_defaults':{},'data_root':str(tmp_path/'data'),'archive':{},'proxies':{}}))
    first={**SCOPE['markets'][0],'gamma_id':'201'}
    one=module.enroll(tmp_path,delta_scope([first]))
    two=module.enroll(tmp_path,delta_scope([NEW]))
    assert len(two['cohorts'])==1 and two['cohorts'][0]['markets']==2
    # Simulate a crash between scope replacement and deployment-index replacement.
    (tmp_path/'deployment.json').write_text(json.dumps(one))
    recovered=module.enroll(tmp_path,delta_scope([NEW]))
    assert len(recovered['cohorts'])==1 and len(recovered['ownership'])==2
    scope=json.loads((tmp_path/'scopes/0000.json').read_bytes())
    assert len(scope['markets'])==2
    control=json.loads((tmp_path/'controls/0000.json').read_bytes())
    assert control['cohort_scope_digest']==scope['catalog_digest']


def test_full_scan_resumes_committed_pages_after_failure(tmp_path,monkeypatch):
    import httpx
    import lobv2.catalog as catalog
    calls=[];failing=True
    def native(i):
        return {'id':str(i),'conditionId':'0x'+str(i)*64,'clobTokenIds':json.dumps([str(i*2),str(i*2+1)]),
            'outcomes':'["Yes","No"]','closed':False,'enableOrderBook':True,'version':'1'}
    class Client:
        def __init__(self,**kwargs):pass
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def get(self,url,params):
            cursor=params.get('after_cursor');calls.append(cursor)
            if cursor=='next' and failing:raise RuntimeError('network process interrupted')
            data={'data':[native(2 if cursor else 1)],'next_cursor':None if cursor else 'next'}
            return httpx.Response(200,json=data,request=httpx.Request('GET',url))
    monkeypatch.setattr(catalog.httpx,'AsyncClient',Client)
    async def run():
        nonlocal failing
        with pytest.raises(RuntimeError):await catalog.discover(resume_path=tmp_path/'pages.sqlite')
        assert calls==[None,'next']
        failing=False
        scope=await catalog.discover(resume_path=tmp_path/'pages.sqlite')
        assert calls==[None,'next','next'] and len(scope['markets'])==2 and scope['full_universe_sweep']
        assert (await catalog.discover(resume_path=tmp_path/'pages.sqlite'))['catalog_revision']==scope['catalog_revision']
        assert len(calls)==3 # Completed-but-not-enrolled result survives a second restart.
        catalog.acknowledge_discovery_cache(tmp_path/'pages.sqlite')
        await catalog.discover(resume_path=tmp_path/'pages.sqlite')
        assert calls[-2:]==[None,'next']
    asyncio.run(run())
