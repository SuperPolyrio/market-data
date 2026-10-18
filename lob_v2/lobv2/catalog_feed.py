"""Durable lifecycle delivery, validated Gamma additions, and shared HTTP budget."""
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import time

import httpx

from .archive import DirectoryStore
from .catalog import GAMMA,identity
from .catalog_owner import CatalogOwner
from .journal import atomic_json
from .wire import FRAME,MAX_RECORD,decode,json_bytes


class HTTPBudget:
    def __init__(self,requests_per_second=4):
        self.lock=asyncio.Lock();self.next=0.;self.interval=1/requests_per_second
    async def wait(self):
        async with self.lock:
            remaining=self.next-time.monotonic()
            if remaining>0:await asyncio.sleep(remaining)
            self.next=time.monotonic()+self.interval


def delta_scope(rows=(),excluded=(),source='CATALOG_LIFECYCLE'):
    now=time.time_ns()
    return {'schema':2,'catalog_revision':now,'sweep_start_ns':now,'sweep_end_ns':now,'pages':0,
        'full_universe_sweep':False,'markets':list(rows),'excluded':list(excluded),'unresolved':[],
        'discovery_source':source,'catalog_digest':hashlib.sha256(json_bytes(list(rows))).hexdigest()}


def consume_outboxes(root,settings):
    changed=False;records=0;remaining_bytes=0
    with (root/'catalog-owner.lock').open('a') as lock:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX)
        owner=CatalogOwner(root)
        with owner.db:
            owner.db.execute('BEGIN IMMEDIATE')
            for directory in Path(settings['data_root']).glob('[PQ]-*/lifecycle'):
                for progress in directory.glob('*.progress.json'):
                    receipt=json.loads(progress.read_bytes())
                    path=Path(receipt['path'])
                    if path.resolve().parent!=directory.resolve() or path.stem!=receipt['stream']:raise ValueError('INVALID_LIFECYCLE_PATH')
                    log=receipt['producer']+':'+receipt['stream']
                    row=owner.db.execute('SELECT byte_offset FROM input_cursors WHERE log_id=?',(log,)).fetchone()
                    offset=row[0] if row else 0
                    limit=receipt['durable_bytes']
                    if offset>limit:raise ValueError('LIFECYCLE_DURABLE_CUT_REGRESSION')
                    start=offset;count=0
                    with path.open('rb') as source:
                        source.seek(offset)
                        while offset<limit and count<1024 and offset-start<4*1024*1024:
                            header=source.read(FRAME.size)
                            if len(header)!=FRAME.size:raise ValueError('SHORT_COMMITTED_LIFECYCLE')
                            size,_=FRAME.unpack(header)
                            if size+FRAME.size>MAX_RECORD or offset+size+FRAME.size>limit:raise ValueError('INVALID_COMMITTED_LIFECYCLE_CUT')
                            rec=decode(header+source.read(size))
                            if rec.kind!='CATALOG_LIFECYCLE' or rec.producer!=receipt['producer'] or rec.stream!=receipt['stream']:raise ValueError('LIFECYCLE_IDENTITY_MISMATCH')
                            capture=rec.body.get('capture_durable_receipt');ref=rec.body['raw_ref']
                            if capture:
                                if capture['log_generation']!=rec.stream or capture['record_sequence']!=ref['capture_offset']:raise ValueError('LIFECYCLE_CAPTURE_RECEIPT_MISMATCH')
                            else:
                                proof=rec.body['ordered_capture_prefix_receipt']
                                if proof['log_generation']!=rec.stream or not ref['capture_offset']<proof['record_sequence']==rec.body['canonical_notification_cut']:
                                    raise ValueError('LIFECYCLE_ORDERED_PREFIX_MISMATCH')
                            changed=owner.consume_lifecycle(rec.body['event'],{**rec.body,'outbox_stream':log,'outbox_record':rec.offset}) or changed
                            offset+=FRAME.size+size;count+=1;records+=1
                    remaining_bytes+=limit-offset
                    owner.db.execute('INSERT OR REPLACE INTO input_cursors VALUES (?,?)',(log,offset))
        pending=owner.db.execute("SELECT COUNT(*) FROM markets WHERE state='PENDING_VALIDATION'").fetchone()[0]
        owner.close()
    atomic_json(root/'lifecycle-input-status.json',{'utc_ns':time.time_ns(),'records_consumed':records,'remaining_durable_bytes':remaining_bytes,
        'pending_validation':pending,'cursor_contract':'CATALOG_SQLITE_FULL_COMMIT_NOT_FLOW_ACK'})
    return changed


async def validate_pending(root,budget):
    owner=CatalogOwner(root)
    pending=owner.db.execute("SELECT market,gamma_id,candidate,evidence FROM markets WHERE state='PENDING_VALIDATION'").fetchall()
    owner.close()
    changed=False;processed=0
    store=DirectoryStore(root/'catalog-evidence')
    async with httpx.AsyncClient(timeout=45) as client:
        for market,gamma_id,candidate,evidence in pending:
            proof=json.loads(evidence)
            if proof.get('next_retry_ns',0)>time.time_ns():continue
            if processed>=100:break  # admission batch; remaining IDs persist in the catalog
            processed+=1
            try:
                if not gamma_id.isdigit():raise ValueError('GAMMA_ID_REQUIRED_FOR_VALIDATION')
                await budget.wait()
                response=await client.get(GAMMA+'/markets/'+gamma_id)
                digest=hashlib.sha256(response.content).hexdigest()
                key='gamma/'+digest+'.json';store.put(key,response.content)
                receipt={'source':'GAMMA_MARKET_BY_ID','request_id':gamma_id,'status':response.status_code,
                    'response_sha256':digest,'evidence_key':key,'observed_ns':time.time_ns()}
                response.raise_for_status()
                raw=response.json()
                if raw.get('conditionId')!=market:raise ValueError('NATIVE_GAMMA_MARKET_MISMATCH')
                owner=CatalogOwner(root)
                try:
                    with owner.db:
                        owner.db.execute('BEGIN IMMEDIATE')
                        current=owner.db.execute('SELECT state FROM markets WHERE market=?',(market,)).fetchone()
                        if not current or current[0]!='PENDING_VALIDATION':continue
                        if raw.get('closed') or raw.get('enableOrderBook') is False:
                            owner.db.execute("UPDATE markets SET state='EXCLUDED_SCOPE',evidence=? WHERE market=?",(json.dumps(receipt),market))
                            owner.changed('SCOPE_EXCLUDED',market,receipt);changed=True
                        else:
                            parsed=identity(raw)
                            native=json.loads(candidate)
                            if native.get('assets_ids') and set(native['assets_ids'])!=set(parsed['assets']):raise ValueError('NATIVE_GAMMA_TOKEN_MISMATCH')
                            changed=owner.upsert(parsed,receipt) or changed
                finally:owner.close()
            except (ValueError,httpx.HTTPError,OSError) as exc:
                owner=CatalogOwner(root)
                with owner.db:
                    owner.db.execute("UPDATE markets SET evidence=? WHERE market=? AND state='PENDING_VALIDATION'",
                        (json.dumps({**proof,'validation_error':repr(exc),'next_retry_ns':time.time_ns()+60*10**9}),market))
                owner.close()
    return changed


async def incremental_new_markets(root,budget):
    owner=CatalogOwner(root)
    watermark=owner.metadata('incremental_gamma_id')
    if watermark is None:
        row=owner.db.execute("SELECT MAX(CAST(gamma_id AS INTEGER)) FROM markets WHERE body IS NOT NULL").fetchone()
        watermark=row[0] or 0
    owner.close()
    rows=[];excluded=[];offset=0;highest=watermark;seen=set();previous=None
    async with httpx.AsyncClient(timeout=45) as client:
        while True:
            await budget.wait()
            response=await client.get(GAMMA+'/markets',params={'limit':100,'offset':offset,'order':'id','ascending':'false'})
            response.raise_for_status();page=response.json()
            if not isinstance(page,list):raise ValueError('INVALID_INCREMENTAL_GAMMA_PAGE')
            identifiers=[int(r['id']) for r in page]
            if identifiers!=sorted(identifiers,reverse=True):raise ValueError('INCREMENTAL_GAMMA_ORDER_NOT_CONFIRMED')
            fingerprint=tuple(identifiers)
            if page and (fingerprint in seen or previous is not None and max(identifiers)>=previous):raise ValueError('REPEATED_OR_UNSTABLE_INCREMENTAL_PAGE')
            seen.add(fingerprint)
            if page:previous=min(identifiers)
            for raw in page:
                highest=max(highest,int(raw['id']))
                if int(raw['id'])<=watermark:continue
                if raw.get('closed') or raw.get('enableOrderBook') is False:
                    excluded.append({'id':raw['id'],'reason':'CLOSED_OR_NO_ORDERBOOK','closed':raw.get('closed'),'enableOrderBook':raw.get('enableOrderBook')})
                else:
                    try:rows.append(identity(raw))
                    except (ValueError,TypeError):
                        # Invalid identity is an explicit pending candidate, never a
                        # reason to stop pagination or forget later valid markets.
                        owner=CatalogOwner(root)
                        with owner.db:
                            owner.db.execute('BEGIN IMMEDIATE')
                            owner.consume_lifecycle({'event_type':'new_market','market':raw.get('conditionId','gamma:'+raw['id']),'id':raw['id']},{'source':'GAMMA_INCREMENTAL_IDENTITY_UNRESOLVED'})
                        owner.close()
            if not page or min(identifiers)<=watermark or len(page)<100:break
            offset+=100
    # The caller enrolls successfully before advancing this success watermark.
    return delta_scope(rows,excluded,'GAMMA_INCREMENTAL_ID_DESC'),highest


async def revalidate_known_ids(root,budget,batches=10):
    """Reconcile assigned-but-absent IDs using explicit closed=any responses.

    Missing rows stay desired. A full open sweep's absence never resolves a market.
    The cursor advances only after the caller commits the returned delta.
    """
    owner=CatalogOwner(root)
    census=set(owner.metadata('census_members',[]))
    cursor=owner.metadata('revalidation_after_id',0)
    targets=sorted({int(gamma) for market,gamma in owner.db.execute(
        "SELECT market,gamma_id FROM markets WHERE state='ELIGIBLE' AND gamma_id IS NOT NULL")
        if market not in census and gamma.isdigit() and int(gamma)>cursor})
    owner.close()
    rows=[];excluded=[];missing=[];processed=0
    store=DirectoryStore(root/'catalog-evidence')
    async with httpx.AsyncClient(timeout=45) as client:
        for start in range(0,min(len(targets),batches*100),100):
            ids=targets[start:start+100]
            await budget.wait()
            response=await client.get(GAMMA+'/markets',params=[('id',str(i)) for i in ids]+[('closed','any'),('limit','100')])
            response.raise_for_status();page=response.json()
            if not isinstance(page,list):raise ValueError('INVALID_IDENTIFIER_REVALIDATION_RESPONSE')
            returned={int(r['id']) for r in page}
            if not returned<=set(ids):raise ValueError('UNREQUESTED_IDENTIFIER_RESPONSE')
            digest=hashlib.sha256(response.content).hexdigest();key='gamma/'+digest+'.json';store.put(key,response.content)
            evidence={'source':'GAMMA_IDENTIFIER_CLOSED_ANY','observed_ns':time.time_ns(),'response_sha256':digest,'evidence_key':key}
            for raw in page:
                if raw.get('closed') or raw.get('enableOrderBook') is False:
                    excluded.append({'id':raw['id'],'reason':'EXPLICIT_CLOSED_OR_NO_ORDERBOOK',
                        'closed':raw.get('closed'),'enableOrderBook':raw.get('enableOrderBook'),**evidence})
                else:
                    try:rows.append(identity(raw))
                    except (ValueError,TypeError):missing.append({'id':raw['id'],'reason':'IDENTITY_UNRESOLVED'})
            missing.extend({'id':str(i),'reason':'IDENTIFIER_NOT_RETURNED_NOT_TERMINAL'} for i in sorted(set(ids)-returned))
            cursor=ids[-1];processed+=len(ids)
    done=processed==len(targets)
    return delta_scope(rows,excluded,'GAMMA_KNOWN_ID_REVALIDATION'),{
        'after_id':0 if done else cursor,'cycle_complete':done,'processed':processed,'remaining':len(targets)-processed,
        'unresolved':missing,'observed_ns':time.time_ns()}
