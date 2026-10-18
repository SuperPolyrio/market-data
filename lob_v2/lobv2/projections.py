"""Rebuildable immutable query views; original journals remain the custody source."""
from collections import defaultdict
import hashlib
import heapq
import json
from pathlib import Path
import time

from .archive import read_archive,sha_file,footer_of
from .book import apply_state,prepare_record,state_view
from .journal import atomic_json
from .wire import decode,json_bytes

VIEW_VERSION='v1'


def observation_projection(inputs,destination,budget_bytes=32*1024*1024):
    """Physical byte reuse within bounded bundles. Every occurrence remains a row.

    Prefix includes the original frame length, CRC and protobuf header. We never
    reserialize a received frame. Unsupported envelope layouts stay opaque.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=True)
    occurrence_schema=pa.schema([('producer',pa.string()),('stream',pa.string()),('offset',pa.uint64()),
        ('kind',pa.string()),('created_ns',pa.uint64()),('payload_index',pa.int32()),('frame_prefix',pa.binary())])
    payload_schema=pa.schema([('payload_index',pa.int32()),('raw_bytes',pa.binary())])
    lookup={};payloads=[];occurrences=[];bytes_used=0;part=0
    before=defaultdict(hashlib.sha256);after=defaultdict(hashlib.sha256)
    count=raw_count=raw_bytes=unique_bytes=0
    def flush():
        nonlocal lookup,payloads,occurrences,bytes_used,part
        if not occurrences:return
        pq.write_table(pa.Table.from_pylist(payloads,schema=payload_schema),destination/f'payloads-{part:06d}.parquet',compression='zstd')
        pq.write_table(pa.Table.from_pylist(occurrences,schema=occurrence_schema),destination/f'observations-{part:06d}.parquet',compression='zstd')
        part+=1;lookup={};payloads=[];occurrences=[];bytes_used=0
    # Interleave archive heads to expose cross-source byte equality inside the
    # bounded window. Each input's order is preserved; this is no event union.
    groups=defaultdict(list)
    for path in inputs:
        with Path(path).open('rb') as src:footer=footer_of(src)
        groups[(footer['producer'],footer['stream'])].append((footer['first'],footer['last'],path))
    def stream_records(parts):
        previous=0
        for first,last,path in sorted(parts):
            if first<=previous:raise ValueError('OVERLAPPING_OBSERVATION_INPUT_RANGES')
            yield from read_archive(path)
            previous=last
    streams=[iter(stream_records(parts)) for _,parts in sorted(groups.items())]
    heads=[]
    for index,source in enumerate(streams):
        rec=next(source,None)
        if rec is not None:heapq.heappush(heads,(rec.created_ns,index,rec))
    while heads:
        _,source_index,rec=heapq.heappop(heads)
        if bytes_used+len(rec.frame)>budget_bytes or len(occurrences)>=10000:flush()
        prefix=rec.frame;index=-1
        if rec.kind=='RAW' and rec.raw and rec.frame.endswith(rec.raw):
            raw_count+=1;raw_bytes+=len(rec.raw)
            if rec.raw not in lookup:
                lookup[rec.raw]=len(payloads);payloads.append({'payload_index':len(payloads),'raw_bytes':rec.raw})
                unique_bytes+=len(rec.raw);bytes_used+=len(rec.raw)
            index=lookup[rec.raw];prefix=rec.frame[:-len(rec.raw)]
        occurrences.append({'producer':rec.producer,'stream':rec.stream,'offset':rec.offset,'kind':rec.kind,
            'created_ns':rec.created_ns,'payload_index':index,'frame_prefix':prefix})
        bytes_used+=len(prefix)+256
        before[(rec.producer,rec.stream)].update(rec.frame);count+=1
        following=next(streams[source_index],None)
        if following is not None:heapq.heappush(heads,(following.created_ns,source_index,following))
    flush()
    recovered=0
    for i in range(part):
        payloads=pq.read_table(destination/f'payloads-{i:06d}.parquet').to_pylist()
        for batch in pq.ParquetFile(destination/f'observations-{i:06d}.parquet').iter_batches(batch_size=1000):
            for row in batch.to_pylist():
                frame=row['frame_prefix']+(payloads[row['payload_index']]['raw_bytes'] if row['payload_index']>=0 else b'')
                rec=decode(frame)
                if (rec.producer,rec.stream,rec.offset,rec.kind,rec.created_ns)!=(row['producer'],row['stream'],row['offset'],row['kind'],row['created_ns']):
                    raise ValueError('OBSERVATION_IDENTITY_MISMATCH')
                after[(rec.producer,rec.stream)].update(frame);recovered+=1
    if recovered!=count or {k:v.digest() for k,v in before.items()}!={k:v.digest() for k,v in after.items()}:
        raise ValueError('OBSERVATION_FRAME_READBACK_MISMATCH')
    result={'view':'observation_payload_reuse','version':VIEW_VERSION,'occurrences':count,'raw_occurrences':raw_count,
        'raw_payload_bytes':raw_bytes,'unique_payload_bytes':unique_bytes,'physical_payload_bytes_saved':raw_bytes-unique_bytes,
        'event_occurrences_removed':0,'dedup_scope':'BOUNDED_BUNDLE_EXACT_RAW_BYTES','bundles':part,
        'original_frames_readback':'IDENTICAL','input_stream_sha256':{p+':'+s:v.hexdigest() for (p,s),v in before.items()}}
    atomic_json(destination/'projection-receipt.json',result)
    return result


def tape_projection(inputs,destination):
    from .validation import export_parquet
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=True)
    def records():
        for path in inputs:yield from read_archive(path)
    receipt=export_parquet(None,destination,records())
    return {**receipt,'version':VIEW_VERSION,'input_contract':'IMMUTABLE_COMMITTED_PRODUCTION_TAPE'}


def checkpoint(inputs,destination,generated_ns=None,max_markets=10000,max_levels=500000):
    """Exact derived state at input cuts, never an invented native observation.

    Unknown segment-leading deltas remain explicitly uninitialized. Callers need
    the prior checkpoint or preceding Tape to independently replay those markets.
    """
    states={};meta={};positions={};missing=set();cuts={};levels=0
    for path in inputs:
        for rec in read_archive(path):
            cuts[(rec.producer,rec.stream)]=rec.offset
            if rec.kind!='TAPE':continue
            body=rec.body;key=(rec.producer,rec.stream,body['market'])
            if key not in states and body['kind']!='SOURCE_RESET':missing.add(key);continue
            if key in meta and body['prev_market_seq']!=meta[key]['market_seq']:raise ValueError('CHECKPOINT_MARKET_SEQUENCE_GAP')
            levels-=sum(len(a['bids'])+len(a['asks']) for a in states.get(key,{}).values())
            states[key]=apply_state(states.get(key,{}),body,prepare_record(body));meta[key]=body;positions[key]=rec.offset;missing.discard(key)
            levels+=sum(len(a['bids'])+len(a['asks']) for a in states[key].values())
            if len(states)>max_markets or levels>max_levels:raise BufferError('CHECKPOINT_PARTITION_BUDGET_EXCEEDED_INPUT_RETAINED')
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=True)
    generated=time.time_ns() if generated_ns is None else generated_ns
    # One streamed file avoids constructing another copy of every full book.
    path=destination/'checkpoint.jsonl'
    with path.open('x') as out:
        for (producer,stream,market),state in states.items():
            body=meta[(producer,stream,market)]
            row={'kind':'DERIVED_CHECKPOINT','producer':producer,'stream':stream,'market':market,
                'generated_utc_ns':generated,'native_event':False,'market_seq':body['market_seq'],
                'last_applied_cut':{'log_id':producer+':tape','log_generation':stream,'record_sequence':positions[(producer,stream,market)],'market_sequence':body['market_seq']},
                'source_epoch':body['book_epoch'],'route':body['route'],'quality':body['quality'],
                'original_baseline_refs':{t:a.get('baseline_ref') for t,a in state.items()},'assets':state_view(state)}
            out.write(json_bytes(row).decode()+'\n')
    result={'kind':'DERIVED_CHECKPOINT','generated_utc_ns':generated,'markets':len(states),
        'missing_prior_baseline':[list(k) for k in sorted(missing)],'complete_for_input_markets':not missing,
        'input_cuts':{p+':'+s:cut for (p,s),cut in cuts.items()},'native_events_created':0,'file_sha256':sha_file(path)}
    atomic_json(destination/'checkpoint-receipt.json',result)
    return result


def commit_projection(store,directory,view,input_manifests,identity_context=None):
    """Data first, verified readback, conditional commit last. No capture GC grant."""
    directory=Path(directory)
    inputs=[{'producer':m['producer'],'stream':m['stream'],'first':m['first'],'last':m['last'],
             'object_sha256':m['object_sha256']} for m in input_manifests]
    identity_body={'view':view,'version':VIEW_VERSION,'inputs':inputs}
    if identity_context is not None:identity_body['context']=identity_context
    identity=hashlib.sha256(json_bytes(identity_body)).hexdigest()
    commit_key=f'query/commits/{view}/{VIEW_VERSION}/{identity}.json'
    try:existing=json.loads(store.get(commit_key))
    except FileNotFoundError:existing=None
    except OSError as exc:
        if 'COMMITTED_OBJECT_NOT_FOUND' not in str(exc):raise
        existing=None
    if existing:
        if existing['input_ranges']!=inputs or existing['view']!=view:raise ValueError('QUERY_IDENTITY_CONFLICT')
        for row in existing['files']:
            data=store.get(row['key'])
            if len(data)!=row['bytes'] or hashlib.sha256(data).hexdigest()!=row['sha256']:raise ValueError('QUERY_RETRY_READBACK_MISMATCH')
        return {'manifest_key':commit_key,'manifest':existing}
    files=[]
    for path in sorted(directory.iterdir()):
        if not path.is_file():continue
        data=path.read_bytes();sha=hashlib.sha256(data).hexdigest()
        key=f'query/objects/{view}/{VIEW_VERSION}/{sha}/{path.name}'
        store.put(key,data)
        if store.get(key)!=data:raise ValueError('QUERY_OBJECT_READBACK_MISMATCH')
        files.append({'name':path.name,'key':key,'bytes':len(data),'sha256':sha})
    manifest={'view':view,'version':VIEW_VERSION,'status':'QUERY_EXPORT_COMMITTED','input_ranges':inputs,
        'files':files,'input_custody_unchanged':True,'authorizes_capture_deletion':False}
    if identity_context is not None:manifest['context']=identity_context
    store.put(commit_key,json_bytes(manifest))
    if json.loads(store.get(commit_key))!=manifest:raise ValueError('QUERY_COMMIT_READBACK_MISMATCH')
    return {'manifest_key':commit_key,'manifest':manifest}
