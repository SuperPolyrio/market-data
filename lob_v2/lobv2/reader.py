"""Bounded FIFO readers and market-by-market candidate handoff."""
import asyncio
from contextlib import suppress
from dataclasses import dataclass
import os
import random
import time
import uuid

import websockets

from .recovery import RecoveryBudget, TextProbe, classify
from .wire import ByteQueue, json_bytes


@dataclass
class Input:
    sequence: int
    raw: bytes
    utc: int
    received: int
    enqueued: int
    proof: object = None

    def __len__(self):
        return len(self.raw)+256


class Session:
    def __init__(self,plant,logical,shard,lane,epoch,markets,config,stop,lease):
        self.plant,self.logical,self.shard,self.lane,self.epoch=plant,logical,shard,lane,epoch
        self.markets,self.config,self.stop,self.lease=markets,config,stop,lease
        self.connection=str(uuid.uuid4())
        self.inbox=ByteQueue(config.get("ingress_bytes_per_socket",32*1024*1024))
        self.probe=TextProbe(config.get("maximum_healthy_rtt_ms",2000),config.get("recovery_healthy_samples",3))
        self.started=time.monotonic()
        self.reader_done=asyncio.Event()
        self.overflow=None
        self.ws=None
        self.closing=False
        self.last_inbound=self.started
        self.phase="CANDIDATE_INITIALIZING"
        self.pending=set(m["market"] for m in markets)
        self.pending_since={m['market']:self.started for m in markets}
        self.failure=None
        self.ever_admitted=False
        self.last_baseline=self.started
        self.baseline_frame_bytes=0
        self.scope_unsubscribed=set()

    def admitted(self):
        return {m["market"] for m in self.markets if self.plant.active_routes.get((m["market"],self.lane))==(m["market"],self.shard,self.lane)
                and (m["market"],self.shard,self.lane) in self.plant.admitted_keys}

    async def close(self,reason):
        self.failure=reason
        self.closing=True
        if self.ws is not None:
            await self.ws.close(code=1011,reason=reason[:100])
        elif getattr(self,'task',None) is not None and not self.task.done():
            # A budget lease also covers rate-credit and WS handshake waits.
            # Closing a not-yet-open socket must cancel those waits and release it.
            self.task.cancel()
            await asyncio.gather(self.task,return_exceptions=True)

    async def subscribe_more(self,markets):
        if not markets:return
        if any((m['market'],self.shard,self.lane) in self.plant.books for m in markets):
            raise ValueError('DUPLICATE_DYNAMIC_ASSIGNMENT')
        self.plant.assign(self.shard,self.lane,self.connection,self.epoch,markets)
        self.markets.extend(markets)
        now=time.monotonic()
        self.pending.update(m['market'] for m in markets)
        self.pending_since.update((m['market'],now) for m in markets)
        self.last_baseline=now
        tokens=[t for m in markets for t in m['assets']]
        for token in tokens:self.plant.states[(self.shard,self.lane,token)].update(
            logical_shard=self.logical,session_role='ACTIVE_WITH_PENDING_INITIALIZATION',desired_revision=self.plant.catalog_revision)
        attempt=str(uuid.uuid4())
        self.plant.journal.append('SUBSCRIPTION_WRITE_START',{'connection':self.connection,'assets':tokens,
            'send_attempt_id':attempt,'operation':'subscribe','catalog_revision':self.plant.catalog_revision})
        await self.ws.send(json_bytes({'operation':'subscribe','assets_ids':tokens,'initial_dump':True,
            'level':2,'custom_feature_enabled':True}).decode())
        self.plant.written(self.shard,self.lane,self.connection,tokens,attempt)

    async def reader(self):
        seq=0
        try:
            async for message in self.ws:
                utc,mono=time.time_ns(),time.monotonic_ns()
                self.last_inbound=time.monotonic()
                seq+=1
                raw=message.encode() if isinstance(message,str) else message
                proof=self.probe.observe(mono) if raw==b"PONG" else None
                item=Input(seq,raw,utc,mono,time.monotonic_ns(),proof)
                if self.inbox.bytes+len(item)>self.inbox.limit or self.inbox.queue.full():
                    # Preserve this just-observed frame AFTER every earlier accepted
                    # frame. The failed queue remains drainable by its sole owner.
                    self.overflow=item
                    self.inbox.failed=True
                    await self.close("INGRESS_BUDGET_EXHAUSTED")
                    break
                self.inbox.put(item)
                await asyncio.sleep(0)
        finally:
            self.closing=True
            self.reader_done.set()
            self.plant.transport_quality(self.shard,self.lane,self.connection,False,reason="TRANSPORT_DEAD")

    async def process(self):
        while not self.reader_done.is_set() or not self.inbox.queue.empty():
            try:
                item=await asyncio.wait_for(self.inbox.get(drain_accepted=True),.1)
            except asyncio.TimeoutError:
                continue
            await self.process_one(item)
            await asyncio.sleep(0)
        if self.overflow is not None:
            await self.process_one(self.overflow)

    async def process_one(self,item):
        self.plant.inflight_inputs[(self.shard,self.lane)]=item.received
        try:
            rec=await self.plant.ingest_async(self.shard,self.lane,self.connection,item.sequence,item.raw,item.utc,item.received,
                {"ingress_enqueue":item.enqueued,"ingress_dequeue":time.monotonic_ns()})
        finally:
            self.plant.inflight_inputs.pop((self.shard,self.lane),None)
        if rec.trace.get("native_book_count"):
            self.last_baseline=time.monotonic()
            self.baseline_frame_bytes+=len(item.raw)
        if item.raw==b"PONG":
            if item.proof and not self.closing:
                proof=item.proof
                self.plant.transport_quality(self.shard,self.lane,self.connection,proof["healthy"],
                    rtt_ms=proof["rtt_ms"],proof_mono_ns=proof["observed"],
                    raw_ref={"capture_run":rec.stream,"capture_offset":rec.offset,"connection":self.connection,"recv_seq":item.sequence},
                    reason="PONG" if proof["healthy"] else "WAIT_STABLE_PROBES")
                self.plant.journal.metrics.observe("text_ping_to_recv",proof["rtt_ms"])
            else:
                self.plant.journal.metrics.counters["unmatched_or_ambiguous_text_pong"]+=1

    async def heartbeat(self):
        while not self.stop.is_set():
            await asyncio.sleep(self.config["heartbeat_seconds"])
            now=time.monotonic_ns()
            if self.probe.expire(now):
                self.plant.transport_quality(self.shard,self.lane,self.connection,False,reason="AMBIGUOUS_TEXT_PROBE")
            if time.monotonic()-self.last_inbound>self.config.get("initial_pong_timeout_seconds",300):
                await self.close("TRANSPORT_DEAD")
                return
            probe=self.probe.begin(now)
            self.plant.journal.append("PING_SEND",{"connection":self.connection,"send_enqueued_mono_ns":now,
                "probe":probe,"ambiguous":self.probe.ambiguous,"heartbeat_type":"POLYMARKET_TEXT"})
            await self.ws.send("PING")
            completed=time.monotonic_ns()
            if probe:self.probe.written(completed)
            self.plant.journal.append("PING_WRITE_OK",{"connection":self.connection,"write_completed_mono_ns":completed,"probe":probe})

    async def run(self):
        from .node import WS_URL,connection_credit
        workers=[]
        plant=self.plant
        try:
            await connection_credit(self.config)
            plant.connect(self.shard,self.lane,self.connection,self.epoch,self.markets)
            for market in self.markets:
                for token in market["assets"]:
                    plant.states[(self.shard,self.lane,token)].update(logical_shard=self.logical,
                        session_role=self.phase,desired_revision=getattr(plant,"catalog_revision",1))
            plant.ingress_queues[(self.shard,self.lane)]=self.inbox
            proxy=self.config.get("outbound_proxy","env")
            if self.config.get("lane_proxies"):proxy=self.config["lane_proxies"][self.lane%len(self.config["lane_proxies"])]
            if proxy=="env":proxy=os.getenv("HTTPS_PROXY") or os.getenv("https_proxy") or True
            elif proxy=="direct":proxy=None
            async with websockets.connect(self.config.get("ws_url",WS_URL),proxy=proxy,max_size=self.config["ws_max_bytes"],
                    max_queue=self.config.get("ws_max_queue",min(16,self.config["queue_bytes"]//self.config["ws_max_bytes"])),
                    write_limit=self.config.get("ws_write_limit",32768),ping_interval=None,compression="deflate",
                    open_timeout=self.config.get("open_timeout_seconds",20),close_timeout=5) as ws:
                self.ws=ws
                tokens=[t for m in self.markets for t in m["assets"]]
                attempt=str(uuid.uuid4())
                plant.journal.append("SUBSCRIPTION_WRITE_START",{"connection":self.connection,"assets":tokens,"send_attempt_id":attempt})
                await ws.send(json_bytes({"type":"market","assets_ids":tokens,"initial_dump":True,"level":2,"custom_feature_enabled":True}).decode())
                plant.written(self.shard,self.lane,self.connection,tokens,attempt)
                workers=[asyncio.create_task(self.reader()),asyncio.create_task(self.process()),asyncio.create_task(self.heartbeat())]
                done,_=await asyncio.wait(workers,return_when=asyncio.FIRST_COMPLETED)
                for task in done:task.result()
        except asyncio.CancelledError:
            if not self.closing:raise
        except (OSError,websockets.WebSocketException,asyncio.TimeoutError) as exc:
            self.failure=repr(exc)
            plant.journal.append("WS_ERROR",{"connection":self.connection,"error":self.failure})
        finally:
            self.closing=True
            self.reader_done.set()
            try:
                if workers:
                    workers[0].cancel()
                    workers[2].cancel()
                    await asyncio.gather(workers[0],workers[2],return_exceptions=True)
                    # Accepted observations drain FIFO even on cancellation/overflow.
                    await workers[1]
            finally:
                if (self.shard,self.lane) in plant.connections:
                    plant.disconnect(self.shard,self.lane,self.connection,self.failure or "TRANSPORT_DEAD")
                self.lease.release()
                self.phase="CLOSED"


async def session_manager(plant,logical,lane,markets,config,stop):
    sessions=[]
    epoch=0
    failures=0
    next_attempt=0.
    stable_since=None
    role=plant.journal.producer
    domains=config.get("lane_provider_domains") or config.get("lane_proxies") or [config.get("outbound_proxy","env")]
    provider=domains[lane%len(domains)]
    try:
        while not stop.is_set():
            now=time.monotonic()
            for session in list(sessions):
                if session.task.done():
                    session.task.result()
                    failures+=1
                    next_attempt=now+random.uniform(.5,min(60,2**min(failures,6)))
                    sessions.remove(session)
                    await plant.forget_source(session.shard,lane)
                    continue
                blocked={m['market'] for m in session.markets if m['market'] in plant.blocked_markets}
                newly_blocked=blocked-session.scope_unsubscribed
                if newly_blocked and session.ws is not None and not session.closing:
                    tokens=[t for m in session.markets if m['market'] in newly_blocked for t in m['assets']]
                    await session.ws.send(json_bytes({'operation':'unsubscribe','assets_ids':tokens}).decode())
                    session.scope_unsubscribed.update(newly_blocked)
                    session.pending-=blocked
                    plant.journal.append('SCOPE_UNSUBSCRIBE',{'connection':session.connection,'markets':sorted(newly_blocked),
                        'catalog_revision':plant.catalog_revision})
                    if len(blocked)==len(session.markets):await session.close('CATALOG_SCOPE_RETIRED')
                admitted=session.admitted()
                if admitted:session.ever_admitted=True
                phase="ACTIVE_WITH_PENDING_INITIALIZATION" if admitted and session.pending else "ACTIVE" if admitted else "RETIRING_CAPTURE" if session.ever_admitted else "CANDIDATE_INITIALIZING"
                if phase!=session.phase:
                    session.phase=phase
                    for market in session.markets:
                        for token in market["assets"]:plant.states[(session.shard,lane,token)]["session_role"]=phase
                if admitted and not session.pending:session.lease.release()
                if session.pending and (now-min(session.pending_since[m] for m in session.pending)>config.get("first_snapshot_timeout_seconds",300) or
                        admitted and now-session.last_baseline>config.get("partial_initialization_idle_seconds",30)):
                    missing=session.pending-admitted
                    plant.journal.append("SNAPSHOT_TIMEOUT",{"connection":session.connection,"markets":sorted(missing),
                        "reason":"SUBSCRIPTION_NOT_READY","active_markets_preserved":len(admitted)})
                    if admitted:
                        tokens=[t for m in session.markets if m["market"] in missing for t in m["assets"]]
                        if tokens:await session.ws.send(json_bytes({"operation":"unsubscribe","assets_ids":tokens}).decode())
                        session.pending.clear()
                        session.lease.release()
                    else:
                        await session.close("SUBSCRIPTION_NOT_READY")
                else:
                    session.pending-=admitted
                if not session.ever_admitted and not plant.local_issue(session.shard,lane) and now-session.started>config.get("backlog_reconnect_grace_seconds",60) and (
                        session.probe.ambiguous or session.baseline_frame_bytes>config.get("max_snapshot_bytes_per_session",64*1024*1024)):
                    await session.close("UNKNOWN_INGRESS_LAG" if session.probe.ambiguous else "INITIALIZATION_BYTES_EXCEEDED")
            # New directory members can join an existing healthy socket without
            # invalidating its current books. A missing dump still needs recovery.
            assigned={m['market'] for s in sessions for m in s.markets}
            fresh=[m for m in markets if m['market'] not in assigned and m['market'] not in plant.blocked_markets]
            for session in sessions:
                if not fresh:break
                if session.phase!='ACTIVE' or session.ws is None or session.closing or not plant.healthy_transport(session.shard,lane):continue
                room=max(0,(config['assets_per_connection']-sum(len(m['assets']) for m in session.markets))//2)
                group=fresh[:room]
                if group:
                    lease=RecoveryBudget(config,role,provider,initial=False,tokens=2*len(group))
                    if not await lease.acquire_async():continue
                    session.lease=lease
                    try:await session.subscribe_more(group)
                    except (OSError,websockets.WebSocketException):await session.close('DYNAMIC_SUBSCRIPTION_FAILED')
                    fresh=fresh[len(group):]
            qualified={m["market"] for m in markets if any(plant.usable(k) for k in plant.market_keys.get(m["market"],()) if k[2]==lane)}
            missing=[m for m in markets if m["market"] not in qualified and m['market'] not in plant.blocked_markets]
            initializing=any(s.pending for s in sessions)
            local=next((plant.local_issue(s.shard,lane) for s in sessions if plant.local_issue(s.shard,lane)),None)
            grace=not sessions or min(now-s.started for s in sessions)>config.get("backlog_reconnect_grace_seconds",60)
            if missing and not initializing and not local and grace and now>=next_attempt and len(sessions)<config.get("max_sessions_per_logical_lane",8):
                lease=RecoveryBudget(config,role,provider,initial=epoch==0,tokens=2*len(missing))
                if await lease.acquire_async():
                    epoch+=1
                    physical=logical+(epoch-1)*1_000_000
                    session=Session(plant,logical,physical,lane,epoch,missing,config,stop,lease)
                    session.task=asyncio.create_task(session.run())
                    sessions.append(session)
                    plant.journal.append("RECOVERY_START",{"logical_shard":logical,"physical_shard":physical,"lane":lane,
                        "connection":session.connection,"markets":len(missing),"attempt":failures,
                        "reason":"INITIAL_SUBSCRIPTION" if epoch==1 else "UNKNOWN_INGRESS_LAG",
                        'causes':[{'shard':s.shard,'reason':classify(plant,s.shard,lane)} for s in sessions if s is not session]})
                else:
                    plant.journal.metrics.counters["recovery_budget_wait"]+=1
                    next_attempt=now+random.uniform(.5,1.5)
            # A connection is retired only after ALL its market responsibilities
            # move, including the durable RESET boundary for a published route.
            for session in sessions:
                if session.ws is None or session.closing:continue
                transferred=all(plant.active_routes.get((m["market"],lane))!=(m["market"],session.shard,lane) for m in session.markets)
                committed_elsewhere=all(plant._committed_route(m["market"]).get("connection")!=session.connection for m in session.markets)
                if session.ever_admitted and transferred and committed_elsewhere:
                    plant.journal.append("SOCKET_RETIRED",{"connection":session.connection,"reason":"ALL_MARKETS_TRANSFERRED"})
                    await session.close("ALL_MARKETS_TRANSFERRED")
                elif now-session.started>config.get("retiring_capture_seconds",300) and len(sessions)>1 and not any(
                        plant.base_usable((m["market"],session.shard,lane)) for m in session.markets):
                    plant.journal.append("SOCKET_RETIRED",{"connection":session.connection,"reason":"UNQUALIFIED_SCOPE_EXPLICIT_GAP",
                        "untransferred_markets":[m["market"] for m in session.markets if plant.active_routes.get((m["market"],lane))==(m["market"],session.shard,lane)]})
                    await session.close("UNQUALIFIED_SCOPE_EXPLICIT_GAP")
            if not missing:
                stable_since=stable_since or now
                if now-stable_since>config.get("recovery_stable_seconds",60):failures=0
            else:stable_since=None
            await asyncio.sleep(config.get("recovery_poll_seconds",.25))
    finally:
        for session in sessions:session.task.cancel()
        results=await asyncio.gather(*(s.task for s in sessions),return_exceptions=True)
        for result in results:
            if isinstance(result,Exception):raise result
