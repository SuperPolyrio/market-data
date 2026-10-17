"""Single event-loop owner: selection, publication and exact durable cuts."""
from collections import OrderedDict, deque
from copy import deepcopy
import json
import time
import uuid

from .book import SourceBook, apply, inspect
from .wire import ByteQueue

POLICY = "single_healthy_route_v2.0.1"


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
        self.journal.callbacks.append(self.on_durable)

    def emit(self, market, event, route, *, reset=False):
        previous = self.seq.get(market, 0)
        if reset:
            self.epochs[market] = self.epochs.get(market, 0) + 1
            self.routes[market] = deepcopy(route)
            self.resets += 1
        body = {**event, "market": market, "market_seq": previous+1,
                "prev_market_seq": previous, "book_epoch": self.epochs.get(market, 0),
                "route": deepcopy(route), "policy_version": POLICY,
                "scope_revision": 1, "scope_assets": self.scope[market]["assets"],
                "market_version": self.scope[market]["version"],
                "asset_id_kind": self.scope[market]["asset_id_kind"],
                "decision_created_ns": time.time_ns(), "decision_mono_ns": time.monotonic_ns(),
                "provenance":self.provenance,
                "durability": "LOCAL_DURABLE", "stream_semantics": "SOURCE_SELECTED_L2",
                "quality": event.get("quality", {"upstream_completeness": "UNKNOWN"})}
        rec = self.journal.append(self.record_kind, body)
        self.working[market] = apply(self.working.get(market, {}), body)
        self.seq[market] = previous+1
        self.emits += 1
        return rec

    def on_durable(self, rec):
        if rec.kind != self.record_kind:
            return
        body, market = rec.body, rec.body["market"]
        if body["prev_market_seq"] != self.committed_seq.get(market, 0):
            raise ValueError("COMMITTED_MARKET_SEQUENCE_GAP")
        self.committed[market] = apply(self.committed.get(market, {}), body)
        self.committed_seq[market] = body["market_seq"]
        self.latencies_ms.append((time.monotonic_ns()-body["decision_mono_ns"])/1e6)
        for subscription in tuple(self.subscribers):
            if market in subscription["markets"]:
                try:
                    subscription["queue"].put(rec.frame)
                except BufferError:
                    self.overflows += 1
                    self.subscribers.discard(subscription)

    def bootstrap(self, markets=None, max_bytes=64*1024*1024):
        markets = set(self.scope) if markets is None else set(markets)
        if not markets <= self.scope.keys():
            raise ValueError("UNKNOWN_MARKET_SUBSCRIPTION")
        # No await between freezing book@cut and registering cut+1 delivery.
        items = []
        for market in sorted(markets):
            state = self.committed.get(market, {})
            if state:
                items.append({"market": market, "assets": deepcopy(state),
                              "cut": self.committed_seq[market],
                              "book_epoch": self._committed_epoch(market),
                              "route": self._committed_route(market),
                              "quality": deepcopy(self._committed_meta.get(market,{}).get("quality",{}))})
        subscription = Subscription(markets, ByteQueue(max_bytes))
        self.subscribers.add(subscription)
        return {"snapshot_id": str(uuid.uuid4()), "kind": "CURRENT_SNAPSHOT",
                "stream": self.journal.stream, "items": items}, subscription

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
        self.raw_received = self.raw_bytes = self.parse_errors = 0
        self.states = {}
        self.history = deque(maxlen=65536)
        self.start_mono_ns = time.monotonic_ns()
        self.dispositions = {}
        self.baseline_pending = deque()
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

    def transport_quality(self, shard, lane, connection, healthy, *, rtt_ms=None, raw_ref=None, reason="PONG"):
        if self.connections.get((shard,lane)) != connection:
            return
        proof={"connection":connection,"healthy":healthy,"rtt_ms":rtt_ms,
            "proof_mono_ns":time.monotonic_ns(),"raw_ref":raw_ref,"reason":reason,
            "policy_version":"fresh_pong_rtt_v1"}
        self.transports[(shard,lane)] = proof
        self.journal.append("TRANSPORT_QUALITY",{"shard":shard,"lane":lane,**proof})
        if not self.require_transport_proof:
            return
        for key,book in self.books.items():
            if key[1:] != (shard,lane):
                continue
            usable=self.usable(key)
            for token,asset in book.assets.items():
                state=self.states[(shard,lane,token)]
                ready=usable and state["baseline_durable"]
                self.transition((shard,lane,token),ready=ready,transport_healthy=healthy,
                    pong_rtt_ms=rtt_ms,status="READY" if ready else "WAIT_HEALTHY_PONG")
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
        return super().emit(market,{**event,"quality":quality},route,reset=reset)

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
        self.journal.append("CONNECT", {"shard": shard, "lane": lane, "connection": connection,
                                        "connection_epoch": epoch})
        for market in markets:
            key = (market["market"], shard, lane)
            self.books[key] = SourceBook(market["market"], market["assets"])
            self.market_keys.setdefault(market["market"],set()).add(key)
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
        rec = self.journal.append("SUBSCRIPTION_WRITE_OK", {"shard": shard, "lane": lane,
            "connection": connection, "assets": tokens, "send_attempt_id": attempt})
        for token in tokens:
            self.states[(shard, lane, token)].update(written=True, write_succeeded_ns=rec.created_ns,
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
        return self.healthy_transport(key[1],key[2]) and book.ready and all(not a["bids"] or not a["asks"] or max(a["bids"]) < min(a["asks"])
                                  for a in book.assets.values())

    def select(self, market, exclude=None, reason="FIRST_READY"):
        candidates = [k for k in sorted(self.market_keys.get(market,())) if k != exclude and self.usable(k)]
        if candidates:
            key = candidates[0]
            self.selected[market] = key
            return self.emit(market, {"kind": "SOURCE_RESET", "scope": "MARKET",
                "assets": self.books[key].snapshot(), "reason": reason,
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
        for key, book in self.books.items():
            if key[1:] == (shard, lane):
                book.invalidate()
                for token in book.assets:
                    self.transition((shard,lane,token), ready=False, status="RECOVERING")
                if self.selected.get(key[0]) == key:
                    self.select(key[0], exclude=key, reason=reason)

    def ingest(self, shard, lane, connection, recv_seq, raw, recv_utc_ns=None, recv_mono_ns=None):
        # Identity assigned before parsing, routing or readiness filters.
        meta = {"shard": shard, "lane": lane, "connection": connection, "recv_seq": recv_seq,
                "connection_epoch": self.connection_epochs.get((shard,lane)) if self.connections.get((shard,lane))==connection else "STALE_UNKNOWN",
                "recv_utc_ns": recv_utc_ns or time.time_ns(),
                "recv_mono_ns": recv_mono_ns or time.monotonic_ns(),
                "clock_quality": "LOCAL_WALL_UNVERIFIED", "subscription_revision": 1}
        raw_rec = self.journal.append("RAW", meta, raw)
        self.raw_received += 1
        self.raw_bytes += len(raw)
        base_ref = {"plant": self.journal.producer, "capture_run": self.journal.stream,
                    "connection": connection, "recv_seq": recv_seq, "capture_offset": raw_rec.offset}
        results = []
        try:
            if raw == b"PONG":
                events = []
                results.append({"status": "HEARTBEAT"})
            else:
                events = json.loads(raw, parse_float=str)
                events = events if isinstance(events, list) else [events]
            for index, event in enumerate(events):
                ref = {**base_ref, "top_level_message_index": index}
                if self.connections.get((shard,lane)) != connection:
                    results.append({"status": "STALE_CONNECTION", "raw_ref": ref})
                    continue
                if not isinstance(event, dict):
                    raise ValueError("INVALID_EVENT")
                market = event.get("market")
                key = (market, shard, lane)
                if key not in self.books:
                    results.append({"status": "UNKNOWN_MARKET_OR_CONTROL", "raw_ref": ref})
                    continue
                try:
                    normalized = self.books[key].observe(event, ref)
                except (ValueError, KeyError, TypeError) as exc:
                    self.books[key].invalidate()
                    for token in self.books[key].assets:
                        self.transition((shard,lane,token), ready=False, book_seen=False,
                                        baseline_durable=False, status="NEEDS_SNAPSHOT")
                    if self.selected.get(market) == key:
                        self.select(market, exclude=key, reason="INVALID_GROUP")
                    results.append({"status": "INVALID_RAW_PRESERVED", "error": str(exc), "raw_ref": ref})
                    self.parse_errors += 1
                    continue
                kind = normalized["kind"]
                if kind == "SNAPSHOT":
                    token = event["asset_id"]
                    self.states[(shard,lane,token)].update(book_seen=True,
                        book_seen_ns=raw_rec.created_ns, baseline_offset=raw_rec.offset,
                        baseline_ref=ref, baseline_session=connection, status="BASELINE_PENDING_DURABLE")
                    self.baseline_pending.append((raw_rec.offset, (shard,lane,token)))
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
        except (ValueError, KeyError, TypeError) as exc:
            self.parse_errors += 1
            self.disconnect(shard,lane,connection,"PARSE_ERROR")
            results.append({"status": "INVALID_RAW_PRESERVED", "error": str(exc)})
        self.journal.append("OBSERVATION_RESULT", {"raw_ref": base_ref, "results": results})
        return raw_rec

    def on_durable(self, rec):
        super().on_durable(rec)
        if rec.kind == "PLANT_EMIT":
            self._committed_meta[rec.body["market"]] = rec.body
        if rec.kind == "RAW":
            while self.baseline_pending and self.baseline_pending[0][0] <= rec.offset:
                offset, key = self.baseline_pending.popleft()
                state = self.states[key]
                if state.get("baseline_offset") != offset:
                    continue
                current = self.connections.get(key[:2]) == state["connection"]
                book = self.books[(state["market"], key[0], key[1])].assets[state["token"]]
                usable = current and self.healthy_transport(key[0],key[1]) and book["ready"] and (not book["bids"] or not book["asks"] or max(book["bids"]) < min(book["asks"]))
                self.transition(key, baseline_durable=True, ready=usable,
                                status="READY" if usable else "WAIT_HEALTHY_PONG" if self.require_transport_proof else "RECOVERING")


class Gateway(Owner):
    def __init__(self, journal, scope, retry_window=4096):
        super().__init__(journal, scope, "TAPE")
        self.shadows, self.sessions, self.selected = {}, {}, {}
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
        candidates = [(p, shadow) for (p,m), shadow in self.shadows.items()
                      if m == market and shadow["ready"]]
        if not candidates:
            self.selected.pop(market, None)
            if market in self.working:
                self.emit(market, {"kind": "GAP", "reason": reason}, self.routes[market])
            return
        plant, shadow = candidates[0]
        self.selected[market] = plant
        self.emit(market, {"kind": "SOURCE_RESET", "scope": "MARKET", "assets": shadow["assets"],
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
            self.shadows[(plant,market)] = {"ready": ready, "assets": deepcopy(item["assets"]),
                "seq": item["cut"], "session": session, "snapshot_id": snapshot["snapshot_id"],
                "quality": deepcopy(item.get("quality",{})),
                "route": {**item["route"], "plant_session_id": session}}
            if ready and market not in self.selected:
                self.choose(market, "CURRENT_SNAPSHOT")

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
        shadow["assets"] = apply(shadow["assets"], body)
        shadow.update(seq=body["market_seq"], route={**body["route"], "plant_session_id": session},
                      quality=deepcopy(body.get("quality",{})),
                      ready=bool(shadow["assets"]) and all(a["ready"] for a in shadow["assets"].values()))
        self.accepted += 1
        if self.selected.get(market) == plant:
            if body["kind"] == "GAP":
                shadow["ready"] = False
                self.choose(market, body.get("reason", "PLANT_GAP"))
            else:
                event = {**body, "source_plant_record": {"stream": rec.stream, "offset": rec.offset},
                         "plant_market_seq": body["market_seq"], "plant_session_id": session,
                         "gateway_receive_ns": time.time_ns()}
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
