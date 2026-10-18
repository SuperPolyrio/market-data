"""Single event-loop owner: selection, publication and exact durable cuts."""
from collections import OrderedDict, deque
from copy import deepcopy
import json
import time
import uuid
import asyncio
import heapq

from .book import SourceBook, apply_state as apply, inspect, state_view, prepare_record, ChangeGroup, uncrossed, freeze_state
from .wire import ByteQueue

POLICY = "single_healthy_route_catalog_admission_v2.2.1"


async def decode_events(raw):
    """Parse once; validate the complete frame before exposing any native group.

    Initial dumps contain many independent events. Their decode boundaries are
    safe scheduling points; an individual price_change remains indivisible.
    """
    if raw==b"PONG":return []
    text=raw.decode(json.detect_encoding(raw))
    decoder=json.JSONDecoder(parse_float=str)
    size=len(text)
    def whitespace(index):
        while index<size and text[index] in ' \r\n\t':index+=1
        return index
    position=whitespace(0)
    if position==size:raise ValueError("EMPTY_NATIVE_JSON")
    if text[position]!='[':
        value,end=decoder.raw_decode(text,position)
        if whitespace(end)!=size:raise ValueError("TRAILING_NATIVE_JSON")
        return [value]
    values=[]
    position=whitespace(position+1)
    if position<size and text[position]==']':
        if whitespace(position+1)!=size:raise ValueError("TRAILING_NATIVE_JSON")
        return values
    started=time.monotonic_ns()
    while position<size:
        value,end=decoder.raw_decode(text,position)
        values.append(value)
        position=whitespace(end)
        if position<size and text[position]==']':
            if whitespace(position+1)!=size:raise ValueError("TRAILING_NATIVE_JSON")
            return values
        if position>=size or text[position]!=',':raise ValueError("INVALID_NATIVE_ARRAY")
        position=whitespace(position+1)
        if time.monotonic_ns()-started>2_000_000:
            await asyncio.sleep(0)
            started=time.monotonic_ns()
    raise ValueError("INCOMPLETE_NATIVE_ARRAY")


class Owner:
    def __init__(self, journal, scope, record_kind):
        self.journal, self.record_kind = journal, record_kind
        self.journal.policy_version = POLICY
        self.scope = {m["market"]: m for m in scope["markets"]}
        self.working, self.committed = {}, {}
        self.seq, self.committed_seq = {}, {}
        self.epochs, self.routes = {}, {}
        self.subscribers = set()
        self.emits = self.nonselected = self.resets = self.overflows = 0
        self.latencies_ms = deque(maxlen=65536)
        self.provenance = {}
        self.catalog_revision=0
        self.scope_policy_version=None
        self.blocked_markets={}
        self.journal.callbacks.append(self.on_durable)

    def emit(self, market, event, route, *, reset=False):
        if market in self.blocked_markets and event['kind']!='GAP':return None
        previous = self.seq.get(market, 0)
        if reset:
            self.epochs[market] = self.epochs.get(market, 0) + 1
            self.routes[market] = deepcopy(route)
            self.resets += 1
            event = {**event,"reset_identity":{"owner_generation":self.journal.stream,
                "reset_request_id":str(uuid.uuid4()),"source_session":route.get("connection",route.get("plant_session_id")),
                "source_epoch":route.get("connection_epoch"),"input_cut":event.get("input_cut",event.get("plant_cut")),
                "base_book_version":event.get("source_book_version")}}
        body = {**event, "market": market, "market_seq": previous+1,
                "prev_market_seq": previous, "book_epoch": self.epochs.get(market, 0),
                "route": deepcopy(route), "policy_version": POLICY,
                "scope_revision": self.catalog_revision, "scope_policy_version":self.scope_policy_version,
                "scope_assets": self.scope[market]["assets"],
                "market_version": self.scope[market]["version"],
                "asset_id_kind": self.scope[market]["asset_id_kind"],
                "decision_created_ns": time.time_ns(), "decision_mono_ns": time.monotonic_ns(),
                "provenance":self.provenance,
                "clock_id":self.provenance.get("clock_id"),
                "durability": "LOCAL_DURABLE", "stream_semantics": "SOURCE_SELECTED_L2",
                "quality": event.get("quality", {"upstream_completeness": "UNKNOWN"})}
        prepared = getattr(event, "prepared", None)
        if prepared is None:
            prepared = prepare_record(body)
        rec = self.journal.append(self.record_kind, body, prepared=prepared)
        rec.trace["working_apply_start"] = time.monotonic_ns()
        self.working[market] = apply(self.working.get(market, {}), body, prepared)
        rec.trace["working_apply_end"] = time.monotonic_ns()
        self.seq[market] = previous+1
        self.emits += 1
        return rec

    async def apply_catalog_control(self,control):
        revision=control['catalog_revision']
        if revision<self.catalog_revision:raise ValueError('CATALOG_REVISION_REGRESSION')
        added=[]
        if 'market_groups' in control:
            from .catalog import validate_scope
            validate_scope({'markets':control['market_groups']})
            for row in control['market_groups']:
                old=self.scope.get(row['market'])
                if old and any(old[k]!=row[k] for k in ('assets','version','asset_id_kind')):
                    raise ValueError('CATALOG_IDENTITY_MIGRATION_REQUIRED')
                if old is None:added.append(row)
            validate_scope({'markets':list(self.scope.values())+added})
        if added:
            # This record precedes every observation and emission for new members.
            self.journal.append('CATALOG_SCOPE_UPSERT',{'catalog_revision':revision,'markets':added})
            self.scope.update((row['market'],row) for row in added)
            for subscription in self.subscribers:
                if subscription.get('follow_scope'):
                    subscription['markets'].update(row['market'] for row in added)
        blocked={m['market']:m for m in control['blocked'] if m['market'] in self.scope}
        if revision==self.catalog_revision and blocked==self.blocked_markets and not added:return
        previous=self.blocked_markets
        self.catalog_revision=revision
        self.scope_policy_version=control['scope_policy_version']
        self.blocked_markets=blocked
        self.journal.append('CATALOG_APPLIED',{k:v for k,v in control.items() if k!='market_groups'})
        started=time.monotonic_ns()
        for market in set(blocked)|set(previous):
            if market in blocked:
                self.selected.pop(market,None)
                if market not in previous and market in self.working:
                    self.emit(market,{'kind':'GAP','reason':blocked[market]['reason'],
                        'catalog_evidence':blocked[market]},self.routes[market])
                if market not in previous and isinstance(self,Plant):
                    for key in self.market_keys.get(market,()):
                        self.books[key].invalidate();self.admitted_keys.discard(key)
                        for token in self.books[key].assets:
                            self.states[(key[1],key[2],token)].update(baseline_durable=False,ready=False)
                elif market not in previous and isinstance(self,Gateway):
                    for plant in self.market_plants.get(market,()):
                        self.shadows[(plant,market)]['ready']=False
            elif isinstance(self,Plant):self.select(market,reason='CATALOG_REVALIDATED')
            elif isinstance(self,Gateway):self.choose(market,reason='CATALOG_REVALIDATED')
            if isinstance(self,Plant):
                for key in self.market_keys.get(market,()):self.reconcile(key)
            if time.monotonic_ns()-started>2_000_000:
                await asyncio.sleep(0);started=time.monotonic_ns()

    def on_durable(self, rec):
        if rec.kind != self.record_kind:
            return
        body, market = rec.body, rec.body["market"]
        if body["prev_market_seq"] != self.committed_seq.get(market, 0):
            raise ValueError("COMMITTED_MARKET_SEQUENCE_GAP")
        rec.trace["committed_apply_start"] = time.monotonic_ns()
        self.committed[market] = apply(self.committed.get(market, {}), body, rec.prepared)
        self.committed_seq[market] = body["market_seq"]
        self._committed_meta[market] = body
        self.latencies_ms.append((time.monotonic_ns()-body["decision_mono_ns"])/1e6)
        rec.trace["committed_apply_end"] = time.monotonic_ns()
        for subscription in tuple(self.subscribers):
            if market in subscription["markets"]:
                try:
                    subscription["queue"].put(rec.frame)
                except BufferError:
                    self.overflows += 1
                    self.subscribers.discard(subscription)
        rec.trace["publish_enqueue"] = time.monotonic_ns()

    def bootstrap(self, markets=None, max_bytes=64*1024*1024, *, deferred=False):
        markets = set(self.scope) if markets is None else set(markets)
        if not markets <= self.scope.keys():
            raise ValueError("UNKNOWN_MARKET_SUBSCRIPTION")
        # No await between freezing book@cut and registering cut+1 delivery.
        items = []
        for market in sorted(markets):
            state = self.committed.get(market, {})
            if state:
                items.append({"market": market, "assets": freeze_state(state) if deferred else state_view(state),
                              "cut": self.committed_seq[market],
                              "book_epoch": self._committed_epoch(market),
                              "route": self._committed_route(market),
                              "quality": deepcopy(self._committed_meta.get(market,{}).get("quality",{}))})
        subscription = Subscription(markets, ByteQueue(max_bytes))
        self.subscribers.add(subscription)
        return {"snapshot_id": str(uuid.uuid4()), "kind": "CURRENT_SNAPSHOT",
                "stream": self.journal.stream, "items": items,
                'generated_utc_ns':time.time_ns(),'catalog_revision':self.catalog_revision,
                'expected_markets':len(markets),'owner_record_cut':self.journal.applied_offset}, subscription

    def _committed_epoch(self, market):
        return self._committed_meta.get(market, {}).get("book_epoch", 0)

    def _committed_route(self, market):
        return deepcopy(self._committed_meta.get(market, {}).get("route", {}))

    @property
    def _committed_meta(self):
        if not hasattr(self, "committed_meta"):
            self.committed_meta = {}
        return self.committed_meta


class Subscription(dict):
    __hash__ = object.__hash__
    __eq__ = object.__eq__

    def __init__(self, markets, queue):
        super().__init__(markets=markets, queue=queue)


class Plant(Owner):
    def __init__(self, journal, scope, lanes=2):
        super().__init__(journal, scope, "PLANT_EMIT")
        self.lanes = lanes
        self.books, self.connections, self.selected = {}, {}, {}
        self.market_keys = {}
        self.connection_epochs = {}
        self.subscription_revisions={}
        self.raw_received = self.raw_bytes = self.parse_errors = 0
        self.states = {}
        self.history = deque(maxlen=65536)
        self.start_mono_ns = time.monotonic_ns()
        self.ingress_queues = {}
        self.inflight_inputs = {}
        self.lifecycle_seen=OrderedDict()
        self.loop_lag_ms = 0.
        self.defer_qualification = False
        self.dirty_transports = set()
        self.transport_keys = {}
        self.active_routes = {}
        self.admitted_keys = set()
        self.require_source_admission = False
        self.require_local_progress = False
        self.admission_pending = set()
        self.dispositions = {}
        self.baseline_pending = []
        self.require_transport_proof = False
        self.prefer_primary_lane = False
        self.maximum_transport_age_ns = 30*10**9
        self.transports = {}

    def healthy_transport(self, shard, lane):
        if not self.require_transport_proof:
            return True
        proof = self.transports.get((shard,lane),{})
        return bool(proof.get("healthy") and proof.get("connection")==self.connections.get((shard,lane))
            and time.monotonic_ns()-proof["proof_mono_ns"] <= self.maximum_transport_age_ns)

    def transport_quality(self, shard, lane, connection, healthy, *, rtt_ms=None, raw_ref=None, reason="PONG", proof_mono_ns=None):
        if self.connections.get((shard,lane)) != connection:
            return
        proof={"connection":connection,"healthy":healthy,"rtt_ms":rtt_ms,
            "proof_mono_ns":proof_mono_ns or time.monotonic_ns(),"raw_ref":raw_ref,"reason":reason,
            "policy_version":"fresh_pong_rtt_v1"}
        self.transports[(shard,lane)] = proof
        self.journal.append("TRANSPORT_QUALITY",{"shard":shard,"lane":lane,**proof})
        if not self.require_transport_proof:
            return
        if self.defer_qualification:
            self.dirty_transports.add((shard,lane))
            return
        for key in tuple(self.transport_keys.get((shard,lane),())):
            self.reconcile(key)

    def reconcile(self,key):
        market,shard,lane=key
        book=self.books[key]
        if self.require_source_admission and key not in self.admitted_keys:
            self.consider_admission(key)
        usable=self.usable(key)
        for token,asset in book.assets.items():
            state=self.states[(shard,lane,token)]
            ready=usable and state["baseline_durable"]
            proof=self.transports.get((shard,lane),{})
            reasons=self.eligibility_reasons(key,token)
            self.transition((shard,lane,token),ready=ready,transport_healthy=bool(proof.get("healthy")),
                pong_rtt_ms=proof.get("rtt_ms"),pong_raw_ref=proof.get('raw_ref'),
                desired_revision=self.catalog_revision,eligibility_reasons=reasons,
                status="READY" if ready else reasons[0] if reasons else "WAIT_MARKET_ADMISSION")
        selected=self.selected.get(key[0])
        if selected==key and not usable:
            self.select(key[0],exclude=key,reason="TRANSPORT_UNQUALIFIED")
        elif usable and (selected is None or self.prefer_primary_lane and lane==0 and selected[2]!=0):
            self.select(key[0],reason="HEALTHY_PRIMARY_READY" if lane==0 else "HEALTHY_SOURCE_READY")

    def emit(self, market, event, route, *, reset=False):
        proof=self.transports.get((route.get("shard"),route.get("lane")),{})
        quality={**event.get("quality",{"upstream_completeness":"UNKNOWN"}),
            "transport_policy":"fresh_pong_rtt_v1" if self.require_transport_proof else "OBSERVATION_ONLY",
            "transport_healthy":self.healthy_transport(route.get("shard"),route.get("lane")),
            "pong_rtt_ms":proof.get("rtt_ms"),"pong_raw_ref":proof.get("raw_ref")}
        normalized = ChangeGroup({**event,"quality":quality})
        normalized.prepared = getattr(event,"prepared",None)
        return super().emit(market,normalized,route,reset=reset)

    def transition(self, key, **values):
        state = self.states[key]
        before = state.get("ready", False)
        state.update(values)
        if before != state.get("ready", False):
            change = {"key": list(key), "ready": state["ready"],
                      "mono_ns": time.monotonic_ns(), "utc_ns": time.time_ns()}
            self.history.append(change)
            self.journal.append("TOKEN_TRANSITION",change)

    def connect(self, shard, lane, connection, epoch, markets):
        self.connections[(shard, lane)] = connection
        self.connection_epochs[(shard,lane)] = epoch
        self.subscription_revisions[(shard,lane)]=0
        self.journal.append("CONNECT", {"shard": shard, "lane": lane, "connection": connection,
                                        "connection_epoch": epoch})
        self.assign(shard,lane,connection,epoch,markets)

    def assign(self,shard,lane,connection,epoch,markets):
        if self.connections.get((shard,lane))!=connection:raise ValueError('SESSION_MISMATCH')
        for market in markets:
            key = (market["market"], shard, lane)
            self.books[key] = SourceBook(market["market"], market["assets"])
            self.market_keys.setdefault(market["market"],set()).add(key)
            self.transport_keys.setdefault((shard,lane),set()).add(key)
            self.active_routes.setdefault((market["market"],lane),key)
            for token in market["assets"]:
                state_key = (shard, lane, token)
                if state_key in self.states:
                    self.transition(state_key, ready=False)
                self.states[state_key] = {"market": market["market"], "token": token,
                    "shard": shard, "lane": lane, "assigned": True, "written": False,
                    "book_seen": False, "baseline_durable": False, "ready": False,
                    "connection": connection, "connection_epoch": epoch,
                    "assigned_ns": time.time_ns(), "status": "ASSIGNED_UNSENT"}

    def written(self, shard, lane, connection, tokens, attempt):
        if self.connections.get((shard, lane)) != connection:
            return False
        self.subscription_revisions[(shard,lane)]+=1
        rec = self.journal.append("SUBSCRIPTION_WRITE_OK", {"shard": shard, "lane": lane,
            "connection": connection, "assets": tokens, "send_attempt_id": attempt,
            'catalog_revision':self.catalog_revision,'subscription_revision':self.subscription_revisions[(shard,lane)]})
        for token in tokens:
            self.states[(shard, lane, token)].update(written=True, write_succeeded_ns=rec.created_ns,
                subscription_written_revision=self.subscription_revisions[(shard,lane)],
                send_attempt_id=attempt, status="READY" if self.states[(shard,lane,token)]["ready"] else "SENT_WAIT_BOOK")
        return True

    def route(self, key):
        market, shard, lane = key
        conn = self.connections[(shard, lane)]
        token = self.scope[market]["assets"][0]
        return {"plant": self.journal.producer, "plant_stream": self.journal.stream,
                "shard": shard, "lane": lane, "connection": conn,
                "connection_epoch": self.states[(shard,lane,token)]["connection_epoch"]}

    def usable(self, key):
        book = self.books[key]
        return (not self.require_source_admission or key in self.admitted_keys and self.active_routes.get((key[0],key[2]))==key) and self.base_usable(key)

    def local_issue(self,shard,lane):
        if not self.require_local_progress:return None
        config=getattr(self,"runtime_config",{})
        inflight=self.inflight_inputs.get((shard,lane))
        if inflight and (time.monotonic_ns()-inflight)/1e6>config.get("maximum_local_queue_age_ms",1000):return "LOCAL_QUEUE_BACKLOG"
        queue=self.ingress_queues.get((shard,lane))
        if queue and (queue.failed or queue.arrivals and (time.monotonic_ns()-queue.arrivals[0])/1e6>config.get("maximum_local_queue_age_ms",1000)):
            return "LOCAL_QUEUE_BACKLOG"
        if self.loop_lag_ms>config.get("maximum_event_loop_lag_ms",200):
            return "EVENT_LOOP_STALL"
        if self.journal.pending and (time.monotonic_ns()-self.journal.pending[0].trace["journal_enqueue"])/1e6>config.get("maximum_durability_age_ms",1000):
            return "DURABILITY_STALL"

    def base_usable(self,key):
        book=self.books[key]
        return key[0] not in self.blocked_markets and self.healthy_transport(key[1],key[2]) and book.ready and book.uncrossed and not self.local_issue(key[1],key[2])

    def eligibility_reasons(self,key,token):
        market,shard,lane=key
        state=self.states[(shard,lane,token)]
        book=self.books[key]
        reasons=[]
        if market in self.blocked_markets:reasons.append(self.blocked_markets[market]['reason'])
        if self.connections.get((shard,lane))!=state["connection"]:reasons.append("SESSION_MISMATCH")
        if not state["written"]:reasons.append("SUBSCRIPTION_NOT_WRITTEN")
        if not state["book_seen"] or not book.assets[token]["ready"]:reasons.append("WAIT_NATIVE_BASELINE")
        if state.get("baseline_session")!=state["connection"]:reasons.append("BASELINE_EPOCH_MISMATCH")
        if not state["baseline_durable"]:reasons.append("CAPTURE_PENDING_DURABLE")
        if not uncrossed(book.assets[token]):reasons.append("CROSSED_BOOK")
        if not all(self.states[(shard,lane,t)]["baseline_durable"] and book.assets[t]["ready"] for t in book.assets):reasons.append("MARKET_PAIR_NOT_READY")
        proof=self.transports.get((shard,lane),{})
        if self.require_transport_proof and not self.healthy_transport(shard,lane):
            reasons.append("RTT_EXCEEDED" if (proof.get("rtt_ms") or 0)>2000 else "PROOF_EXPIRED" if proof.get("healthy") else "WAIT_HEALTHY_PONG")
        issue=self.local_issue(shard,lane)
        if issue:reasons.append(issue)
        if self.require_source_admission and key not in self.admitted_keys:reasons.append("SOURCE_ADMISSION_PENDING")
        if self.require_source_admission and key in self.admitted_keys and self.active_routes.get((market,lane))!=key:reasons.append("ROUTE_SUPERSEDED")
        return reasons

    def consider_admission(self,key):
        if key in self.admission_pending or key in self.admitted_keys or not self.base_usable(key):return
        connection=self.connections[key[1:]]
        if not all((state:=self.states[(key[1],key[2],t)])["written"] and state['connection']==connection
                   and state.get('baseline_session')==connection and state["baseline_durable"]
                   for t in self.books[key].assets):return
        self.journal.append("SOURCE_ADMISSION",{"key":list(key),"connection":self.connections[key[1:]],
            "input_cut":self.journal.offset,"source_book_version":self.books[key].version})
        self.admission_pending.add(key)

    def select(self, market, exclude=None, reason="FIRST_READY"):
        candidates = [k for k in sorted(self.market_keys.get(market,()),key=lambda k:(k[2],-k[1])) if k != exclude and self.usable(k)]
        if candidates:
            key = candidates[0]
            self.selected[market] = key
            return self.emit(market, {"kind": "SOURCE_RESET", "scope": "MARKET",
                "assets": self.books[key].snapshot(), "reason": reason,
                "input_timing":getattr(self.books[key],"input_timing",{}),
                "source_book_version":self.books[key].version,
                "input_cut":{"log_id":self.journal.producer+":"+self.journal.kind,
                    "log_generation":self.journal.stream,"record_sequence":self.journal.offset,
                    "native_observation_ref":getattr(self.books[key],"last_input_ref",None)},
                "quality": {"continuity_unknown": True, "upstream_completeness": "UNKNOWN"}},
                self.route(key), reset=True)
        self.selected.pop(market, None)
        if market in self.working:
            return self.emit(market, {"kind": "GAP", "reason": reason}, self.routes[market])

    def disconnect(self, shard, lane, connection, reason):
        self.journal.append("DISCONNECT", {"shard": shard, "lane": lane,
            "connection": connection, "reason": reason})
        if self.connections.get((shard, lane)) != connection:
            return
        if self.defer_qualification:
            self.transports[(shard,lane)]={"connection":connection,"healthy":False,"reason":"TRANSPORT_DEAD","proof_mono_ns":time.monotonic_ns()}
            for key in self.transport_keys.get((shard,lane),()):self.books[key].invalidate()
            self.dirty_transports.add((shard,lane))
            return
        for key, book in self.books.items():
            if key[1:] == (shard, lane):
                book.invalidate()
                for token in book.assets:
                    self.transition((shard,lane,token), ready=False, status="RECOVERING")
                if self.selected.get(key[0]) == key:
                    self.select(key[0], exclude=key, reason=reason)

    async def forget_source(self,shard,lane):
        started=time.monotonic_ns()
        for key in tuple(self.transport_keys.pop((shard,lane),())):
            if self.selected.get(key[0])==key:self.select(key[0],exclude=key,reason="SOURCE_SESSION_RETIRED")
            self.journal.append('SOURCE_STATE_RELEASED',{'market':key[0],'shard':shard,'lane':lane,
                'connection':self.connections.get((shard,lane))})
            for token in self.books[key].assets:
                self.transition((shard,lane,token),ready=False,status='SESSION_RETIRED')
                self.states.pop((shard,lane,token),None)
            del self.books[key]
            self.market_keys[key[0]].discard(key)
            self.admitted_keys.discard(key)
            self.admission_pending.discard(key)
            if self.active_routes.get((key[0],lane))==key:self.active_routes.pop((key[0],lane),None)
            if time.monotonic_ns()-started>2_000_000:
                await asyncio.sleep(0)
                started=time.monotonic_ns()
        self.connections.pop((shard,lane),None)
        self.connection_epochs.pop((shard,lane),None)
        self.subscription_revisions.pop((shard,lane),None)
        self.transports.pop((shard,lane),None)
        self.ingress_queues.pop((shard,lane),None)

    def ingest(self,*args,**kwargs):
        steps=self._ingest_steps(*args,**kwargs)
        rec=next(steps)
        raw=rec.raw
        try:
            events=[] if raw==b"PONG" else json.loads(raw,parse_float=str)
            events=events if isinstance(events,list) else [events]
        except (ValueError,KeyError,TypeError) as exc:
            try:steps.throw(exc)
            except StopIteration:return rec
        try:
            steps.send(events)
            while True:next(steps)
        except StopIteration:return rec

    async def ingest_async(self,*args,**kwargs):
        steps=self._ingest_steps(*args,**kwargs)
        rec=next(steps)
        try:
            events=await decode_events(rec.raw)
        except (ValueError,KeyError,TypeError) as exc:
            try:steps.throw(exc)
            except StopIteration:return rec
        started=time.monotonic_ns()
        try:
            steps.send(events)
            while True:
                self.release_baselines(self.journal.durable_offset)
                if time.monotonic_ns()-started>2_000_000:
                    await asyncio.sleep(0)
                    started=time.monotonic_ns()
                next(steps)
        except StopIteration:return rec

    def _ingest_steps(self, shard, lane, connection, recv_seq, raw, recv_utc_ns=None, recv_mono_ns=None, ingress=None):
        # Identity assigned before parsing, routing or readiness filters.
        meta = {"shard": shard, "lane": lane, "connection": connection, "recv_seq": recv_seq,
                "connection_epoch": self.connection_epochs.get((shard,lane)) if self.connections.get((shard,lane))==connection else "STALE_UNKNOWN",
                "recv_utc_ns": recv_utc_ns or time.time_ns(),
                "recv_mono_ns": recv_mono_ns or time.monotonic_ns(),
                "clock_quality": "LOCAL_WALL_UNVERIFIED", "subscription_revision":self.subscription_revisions.get((shard,lane)),
                'catalog_revision':self.catalog_revision}
        raw_rec = self.journal.append("RAW", meta, raw)
        timing = {"recv_return":meta["recv_mono_ns"],**(ingress or {}),"clock_id":self.provenance.get("clock_id")}
        raw_rec.trace.update({k:v for k,v in timing.items() if k!="clock_id"})
        self.raw_received += 1
        self.raw_bytes += len(raw)
        base_ref = {"plant": self.journal.producer, "capture_run": self.journal.stream,
                    "connection": connection, "recv_seq": recv_seq, "capture_offset": raw_rec.offset}
        results = []
        try:
            timing["parse_start"] = time.monotonic_ns()
            events = yield raw_rec
            timing["parse_end"] = time.monotonic_ns()
            if raw == b"PONG":results.append({"status":"HEARTBEAT"})
            for index, event in enumerate(events):
                ref = {**base_ref, "top_level_message_index": index}
                if self.connections.get((shard,lane)) != connection:
                    results.append({"status": "STALE_CONNECTION", "raw_ref": ref})
                    yield None
                    continue
                if not isinstance(event, dict):
                    raise ValueError("INVALID_EVENT")
                market = event.get("market")
                if event.get("event_type") in ('new_market','market_resolved'):
                    action=(market,event['event_type'])
                    if action not in self.lifecycle_seen:
                        self.journal.append('LIFECYCLE_CAPTURED',{'event':event,'raw_ref':ref},prepared=raw_rec)
                        self.lifecycle_seen[action]=True
                        if len(self.lifecycle_seen)>4096:self.lifecycle_seen.popitem(last=False)
                key = (market, shard, lane)
                if key not in self.books:
                    results.append({"status": "UNKNOWN_MARKET_OR_CONTROL", "raw_ref": ref})
                    yield None
                    continue
                try:
                    timing["source_apply_start"] = time.monotonic_ns()
                    normalized = self.books[key].observe(event, ref)
                    timing["source_apply_end"] = time.monotonic_ns()
                    normalized["input_timing"] = dict(timing)
                    self.books[key].input_timing = dict(timing)
                    self.books[key].last_input_ref = ref
                except (ValueError, KeyError, TypeError) as exc:
                    self.books[key].invalidate()
                    for token in self.books[key].assets:
                        self.transition((shard,lane,token), ready=False, book_seen=False,
                                        baseline_durable=False, status="NEEDS_SNAPSHOT")
                    if self.selected.get(market) == key:
                        self.select(market, exclude=key, reason="INVALID_GROUP")
                    results.append({"status": "INVALID_RAW_PRESERVED", "error": str(exc), "raw_ref": ref})
                    self.parse_errors += 1
                    yield None
                    continue
                kind = normalized["kind"]
                if kind == "SNAPSHOT":
                    token = event["asset_id"]
                    self.states[(shard,lane,token)].update(book_seen=True,
                        book_seen_ns=raw_rec.created_ns, baseline_offset=raw_rec.offset,
                        baseline_ref=ref, baseline_session=connection, status="BASELINE_PENDING_DURABLE")
                    heapq.heappush(self.baseline_pending,(raw_rec.offset, (shard,lane,token),raw_rec))
                if kind in ("SNAPSHOT", "CHANGE_GROUP"):
                    if self.usable(key):
                        for token in self.books[key].assets:
                            state = self.states[(shard,lane,token)]
                            if state["baseline_durable"]:
                                self.transition((shard,lane,token),ready=True,status="READY")
                    if not self.usable(key):
                        for token in self.books[key].assets:
                            self.transition((shard,lane,token), ready=False, status="BOOK_NOT_QUALIFIED")
                        if self.selected.get(market) == key:
                            self.select(market, exclude=key, reason="BOOK_NOT_QUALIFIED")
                    elif market not in self.selected:
                        self.select(market)
                    elif self.prefer_primary_lane and lane==0 and self.selected[market][2]!=0:
                        self.select(market,reason="HEALTHY_PRIMARY_READY")
                    elif self.selected[market] == key:
                        self.emit(market, normalized, self.route(key))
                    else:
                        self.nonselected += 1
                results.append({"status": kind, "raw_ref": ref})
                yield None
        except (ValueError, KeyError, TypeError) as exc:
            self.parse_errors += 1
            self.disconnect(shard,lane,connection,"PARSE_ERROR")
            results.append({"status": "INVALID_RAW_PRESERVED", "error": str(exc)})
        self.journal.append("OBSERVATION_RESULT", {"raw_ref": base_ref, "results": results})
        raw_rec.trace.update({k:v for k,v in timing.items() if k!="clock_id"})
        raw_rec.trace["native_book_count"]=sum(r.get("status")=="SNAPSHOT" for r in results)
        self.release_baselines(self.journal.durable_offset)
        self.journal.metrics.observe("frame_processing",(time.monotonic_ns()-meta["recv_mono_ns"])/1e6)
        return raw_rec

    def on_durable(self, rec):
        super().on_durable(rec)
        if rec.kind=="SOURCE_ADMISSION":
            key=tuple(rec.body["key"])
            self.admission_pending.discard(key)
            if key in self.books and self.connections.get(key[1:])==rec.body["connection"] and self.base_usable(key):
                old=self.active_routes.get((key[0],key[2]))
                self.admitted_keys.add(key)
                self.active_routes[(key[0],key[2])]=key
                if old is not None and self.selected.get(key[0])==old and old!=key:
                    self.select(key[0],reason="CANDIDATE_ADMITTED")
                self.dirty_transports.add(key[1:])
        if rec.kind == "PLANT_EMIT":
            self._committed_meta[rec.body["market"]] = rec.body
        if rec.kind == "RAW":self.release_baselines(rec.offset)

    def release_baselines(self,cut):
        while self.baseline_pending and self.baseline_pending[0][0] <= cut:
            offset, key, raw_record = heapq.heappop(self.baseline_pending)
            state = self.states.get(key)
            if state is None or state.get("baseline_offset") != offset:
                self.journal.metrics.counters['retired_baseline_receipt_ignored']+=1
                continue
            current = self.connections.get(key[:2]) == state["connection"]
            book = self.books[(state["market"], key[0], key[1])].assets[state["token"]]
            usable = current and self.healthy_transport(key[0],key[1]) and book["ready"] and uncrossed(book) and (not self.require_source_admission or (state["market"],key[0],key[1]) in self.admitted_keys)
            self.transition(key, baseline_durable=True, baseline_capture_durable_ref=raw_record.trace.get("receipt"), ready=usable,
                            status="READY" if usable else "WAIT_HEALTHY_PONG" if self.require_transport_proof else "RECOVERING")
            if self.require_source_admission:self.consider_admission((state["market"],key[0],key[1]))


class Gateway(Owner):
    def __init__(self, journal, scope, retry_window=4096):
        super().__init__(journal, scope, "TAPE")
        self.shadows, self.sessions, self.selected = {}, {}, {}
        self.market_plants = {}
        self.session_streams = {}
        self.seen = OrderedDict()
        self.retry_window = retry_window
        self.seen_bytes = 0
        self.retry_bytes = 64*1024*1024
        self.accepted = self.retries = 0

    def on_durable(self, rec):
        super().on_durable(rec)
        if rec.kind == "TAPE":
            self._committed_meta[rec.body["market"]] = rec.body

    def start_session(self, plant, session):
        self.drop_session(plant, self.sessions.get(plant), "PLANT_SESSION_REBUILT")
        # CURRENT_SNAPSHOT is authoritative for the new session. A freshly
        # restarted plant may initially have no committed books: retaining an
        # old generation's market sequence would reject every first RESET and
        # cause repeated full-cohort reconnect/bootstrap cycles during warmup.
        self.shadows = {key:value for key,value in self.shadows.items() if key[0] != plant}
        self.sessions[plant] = session
        self.session_streams[plant] = None
        self.seen = OrderedDict((key, value) for key, value in self.seen.items() if key[0] != plant)
        self.seen_bytes = sum(len(frame) for frame in self.seen.values())

    def choose(self, market, reason="PLANT_SELECT"):
        candidates = [(p,self.shadows[(p,market)]) for p in sorted(self.market_plants.get(market,()))
                      if market not in self.blocked_markets and (p,market) in self.shadows and self.shadows[(p,market)]["ready"]]
        if not candidates:
            self.selected.pop(market, None)
            if market in self.working:
                self.emit(market, {"kind": "GAP", "reason": reason}, self.routes[market])
            return
        plant, shadow = candidates[0]
        self.selected[market] = plant
        self.emit(market, {"kind": "SOURCE_RESET", "scope": "MARKET", "assets": state_view(shadow["assets"]),
            "reason": reason, "plant_session_id": shadow["session"],
            "bootstrap_snapshot_id": shadow.get("snapshot_id"), "plant_cut": shadow["seq"],
            "quality": {**shadow.get("quality",{}),"continuity_unknown": True, "upstream_completeness": "UNKNOWN"}}, shadow["route"], reset=True)

    def snapshot(self, plant, session, snapshot):
        if self.sessions.get(plant) != session:
            raise ValueError("STALE_SESSION")
        self.session_streams[plant] = snapshot["stream"]
        for item in snapshot["items"]:
            market = item["market"]
            if market not in self.scope or set(item["assets"]) != set(self.scope[market]["assets"]):
                raise ValueError("INCOMPLETE_MARKET_SNAPSHOT")
            ready = all(x["ready"] for x in item["assets"].values())
            self.shadows[(plant,market)] = {"ready": ready, "assets": apply({}, {"kind":"SOURCE_RESET","scope":"MARKET","assets":item["assets"]}),
                "seq": item["cut"], "session": session, "snapshot_id": snapshot["snapshot_id"],
                "quality": deepcopy(item.get("quality",{})),
                "route": {**item["route"], "plant_session_id": session}}
            self.market_plants.setdefault(market,set()).add(plant)
            if ready and market not in self.selected:
                self.choose(market, "CURRENT_SNAPSHOT")

    async def snapshot_async(self,plant,session,snapshot):
        started=time.monotonic_ns()
        for item in snapshot["items"]:
            self.snapshot(plant,session,{**snapshot,"items":[item]})
            if time.monotonic_ns()-started>2_000_000:
                await asyncio.sleep(0)
                started=time.monotonic_ns()
        self.session_streams[plant]=snapshot["stream"]

    def accept(self, plant, session, rec):
        if self.sessions.get(plant) != session:
            raise ValueError("STALE_SESSION")
        if rec.kind != "PLANT_EMIT" or rec.producer != plant:
            raise ValueError("UNEXPECTED_PLANT_IDENTITY")
        if rec.stream != self.session_streams.get(plant):
            raise ValueError("RESET_REQUIRED_STREAM_GENERATION")
        identity = (plant,rec.stream,rec.offset)
        if identity in self.seen:
            if self.seen[identity] != rec.frame:
                raise ValueError("IDENTITY_CONTENT_CONFLICT")
            self.retries += 1
            return False
        body, market = rec.body, rec.body["market"]
        if market not in self.scope:
            raise ValueError("OUT_OF_SCOPE_MARKET")
        shadow = self.shadows.get((plant,market))
        if shadow is None:
            if body["kind"] != "SOURCE_RESET" or body["prev_market_seq"] != 0:
                raise ValueError("RESET_REQUIRED")
            shadow = {"assets": {}, "seq": 0, "session": session, "ready": False}
            self.shadows[(plant,market)] = shadow
            self.market_plants.setdefault(market,set()).add(plant)
        if body["prev_market_seq"] != shadow["seq"] or body["market_seq"] != shadow["seq"]+1:
            raise ValueError("RESET_REQUIRED")
        expected_assets=set(self.scope[market]["assets"])
        kind=body["kind"]
        if kind=="SOURCE_RESET":
            if body.get("scope")!="MARKET" or set(body.get("assets",{}))!=expected_assets:
                raise ValueError("INCOMPLETE_MARKET_RESET")
        else:
            previous_route={k:v for k,v in shadow.get("route",{}).items() if k!="plant_session_id"}
            if previous_route and body["route"]!=previous_route:
                raise ValueError("RESET_REQUIRED_ROUTE_CHANGED")
        if kind=="SNAPSHOT" and not set(body["assets"])<=expected_assets:
            raise ValueError("OUT_OF_SCOPE_ASSET")
        if kind=="CHANGE_GROUP" and (not body.get("changes") or any(row["asset"] not in expected_assets or row["side"] not in ("BUY","SELL") for row in body["changes"])):
            raise ValueError("INVALID_COMPLETE_GROUP")
        # Reserve the identity before handing the complete group to the owner.
        self.seen[identity] = rec.frame
        self.seen_bytes += len(rec.frame)
        while len(self.seen) > self.retry_window or self.seen_bytes > self.retry_bytes:
            _,old_frame = self.seen.popitem(last=False)
            self.seen_bytes -= len(old_frame)
        prepared = prepare_record(body)
        shadow["assets"] = apply(shadow["assets"], body, prepared)
        shadow.update(seq=body["market_seq"], route={**body["route"], "plant_session_id": session},
                      quality=deepcopy(body.get("quality",{})),
                      ready=bool(shadow["assets"]) and all(a["ready"] for a in shadow["assets"].values()))
        self.accepted += 1
        if self.selected.get(market) == plant:
            if body["kind"] == "GAP":
                shadow["ready"] = False
                self.choose(market, body.get("reason", "PLANT_GAP"))
            else:
                event = ChangeGroup({**body, "source_plant_record": {"stream": rec.stream, "offset": rec.offset},
                         "plant_market_seq": body["market_seq"], "plant_session_id": session,
                         "gateway_receive_ns": time.time_ns()})
                event.prepared = prepared
                self.emit(market, event, shadow["route"], reset=body["kind"] == "SOURCE_RESET")
        elif market not in self.selected and shadow["ready"]:
            self.choose(market)
        else:
            self.nonselected += 1
        return True

    def drop_session(self, plant, session, reason="PLANT_DISCONNECT"):
        if not session or self.sessions.get(plant) != session:
            return
        for (p, market), shadow in self.shadows.items():
            if p == plant:
                shadow["ready"] = False
        for market, selected in list(self.selected.items()):
            if selected == plant:
                self.choose(market, reason)
