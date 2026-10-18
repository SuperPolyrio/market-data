from copy import deepcopy
import asyncio
import json

from lobv2.catalog_owner import CatalogOwner
from lobv2.catalog_feed import delta_scope,consume_outboxes


def row(m='m1',tokens=None):
    return {'market':m,'gamma_id':'101' if m=='m1' else '102','assets':tokens or ['1','2'],
            'version':'1','asset_id_kind':'CLOB_TOKEN_ID','outcomes':['Yes','No'],'question':'q'}


def test_tombstone_survives_restart_and_stale_full_sweep(tmp_path):
    owner=CatalogOwner(tmp_path)
    owner.apply_scan(delta_scope([row()]))
    with owner.db:owner.resolve('m1',{'source':'NATIVE','raw_ref':{'capture_offset':10}})
    owner.close()
    owner=CatalogOwner(tmp_path)
    snapshot=owner.apply_scan({**delta_scope([row()]),'full_universe_sweep':True})
    assert snapshot['markets']==[]
    assert snapshot['terminal_evidence'][0]['evidence']['raw_ref']['capture_offset']==10
    owner.close()


def test_scan_absence_and_failed_or_partial_sweep_never_remove_members(tmp_path):
    owner=CatalogOwner(tmp_path)
    owner.apply_scan({**delta_scope([row(),row('m2',['3','4'])]),'full_universe_sweep':True})
    before=owner.metadata('last_complete_scan')
    snapshot=owner.apply_scan(delta_scope([row()]))
    assert len(snapshot['markets'])==2 and snapshot['last_complete_scan']==before
    snapshot=owner.apply_scan({**delta_scope([row()]),'full_universe_sweep':True})
    assert len(snapshot['markets'])==2
    assert owner.metadata('census_members')==['m1']
    owner.close()


def test_membership_digest_ignores_title_but_identity_change_quarantines(tmp_path):
    owner=CatalogOwner(tmp_path)
    first=owner.apply_scan(delta_scope([row()]))
    second=owner.apply_scan(delta_scope([{**row(),'question':'fresh metadata'}]))
    assert first['membership_digest']==second['membership_digest']
    third=owner.apply_scan(delta_scope([row(tokens=['3','4'])]))
    assert third['markets'][0]['assets']==['1','2']
    assert third['unadmitted_groups'][0]['reason']=='IDENTITY_MIGRATION_REQUIRED'
    owner.close()


def test_durable_lifecycle_consumption_cursor_is_idempotent(tmp_path):
    from test_semantics import make_plant
    async def run():
        data=tmp_path/'P-0000';data.mkdir()
        p=make_plant(data)
        await p.ingest_async(0,0,'c0',1,json.dumps({'market':'m1','event_type':'market_resolved'}).encode())
        await p.journal.sync()
        control=tmp_path/'controller';control.mkdir()
        assert consume_outboxes(control,{'data_root':str(tmp_path)})
        assert not consume_outboxes(control,{'data_root':str(tmp_path)})
        owner=CatalogOwner(control)
        assert len(owner.snapshot()['terminal_evidence'])==1
        assert owner.db.execute('SELECT COUNT(*) FROM changes').fetchone()[0]==1
        owner.close();await p.journal.close()
    asyncio.run(run())


def test_quarantine_only_blocks_affected_market_and_requires_new_baseline(tmp_path):
    from test_semantics import make_plant,FIXTURE
    async def run():
        p=make_plant(tmp_path)
        for i,e in enumerate(FIXTURE[:2]):p.ingest(0,0,'c0',i+1,json.dumps(e).encode())
        await p.journal.sync()
        market=FIXTURE[0]['market']
        assert market in p.selected
        await p.apply_catalog_control({'catalog_revision':5,'scope_policy_version':'policy-v1',
            'blocked':[{'market':market,'reason':'IDENTITY_MIGRATION_REQUIRED'}]})
        await p.journal.sync()
        assert market not in p.selected and not p.usable((market,0,0))
        assert all(not v['ready'] for v in p.committed[market].values())
        assert p.eligibility_reasons((market,0,0),FIXTURE[0]['asset_id'])[0]=='IDENTITY_MIGRATION_REQUIRED'
        await p.apply_catalog_control({'catalog_revision':6,'scope_policy_version':'policy-v1','blocked':[]})
        assert market not in p.selected
        await p.journal.close()
    asyncio.run(run())


def test_catalog_notification_recovers_after_capture_sync_before_outbox_failure(tmp_path,monkeypatch):
    import os
    import lobv2.journal as journal_module
    from test_semantics import make_plant
    async def run():
        data=tmp_path/'P-0000';data.mkdir()
        p=make_plant(data)
        await p.ingest_async(0,0,'c0',1,json.dumps({'market':'m1','event_type':'market_resolved'}).encode())
        original=journal_module.append_lifecycle
        def fail(*args):raise OSError('injected crash before outbox')
        monkeypatch.setattr(journal_module,'append_lifecycle',fail)
        try:await p.journal.sync()
        except OSError:pass
        else:raise AssertionError('failure was not injected')
        assert p.journal.durable_offset==0
        os.close(p.journal.fd);p.journal.fd=None;p.journal._io.shutdown(wait=True)
        monkeypatch.setattr(journal_module,'append_lifecycle',original)
        journal_module.recover_open(p.journal.root)
        assert journal_module.repair_lifecycle_outboxes(p.journal.root)==1
        assert journal_module.repair_lifecycle_outboxes(p.journal.root)==0
        control=tmp_path/'controller';control.mkdir()
        assert consume_outboxes(control,{'data_root':str(tmp_path)})
        owner=CatalogOwner(control)
        assert owner.snapshot()['terminal_evidence'][0]['market']=='m1'
        owner.close()
    asyncio.run(run())


def test_duplicate_token_owner_is_visible_and_cannot_be_routed(tmp_path):
    owner=CatalogOwner(tmp_path)
    owner.apply_scan(delta_scope([row()]))
    snapshot=owner.apply_scan(delta_scope([row('m2',['1','3'])]))
    assert len(snapshot['markets'])==2
    assert snapshot['unadmitted_groups'][0]['reason']=='TOKEN_OWNERSHIP_CONFLICT'
    assert owner.db.execute("SELECT market FROM token_owners WHERE token='1'").fetchone()[0]=='m1'
    owner.close()


def test_stale_scan_does_not_undo_quarantine_or_explicit_exclusion(tmp_path):
    owner=CatalogOwner(tmp_path)
    owner.apply_scan(delta_scope([row()]))
    owner.apply_scan(delta_scope([row(tokens=['3','4'])]))
    assert owner.apply_scan(delta_scope([row()]))['unadmitted_groups']
    owner.apply_scan(delta_scope(excluded=[{'id':'101','closed':True}]))
    assert not owner.apply_scan(delta_scope([row()]))['markets']
    owner.close()
