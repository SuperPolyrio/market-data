"""Session-scoped text probes and host/plant/provider recovery admission."""
import asyncio
import fcntl
import hashlib
from pathlib import Path
import time


class TextProbe:
    """A late unique response fails its own probe; overlap is ambiguous.

    Mandatory keepalive PINGs continue after ambiguity, without pretending they
    are distinguishable probes. Only a new session can clear that ambiguity.
    """
    def __init__(self, deadline_ms=2000, stable_samples=3):
        self.deadline_ns=int(deadline_ms*1e6)
        self.required=stable_samples
        self.pending=None
        self.ambiguous=False
        self.consecutive=0
        self.unmatched=0

    def expire(self, now):
        if self.pending and now-self.pending["enqueued"]>self.deadline_ns:
            self.consecutive=0
            return True
        return self.ambiguous

    def begin(self, now):
        self.expire(now)
        if not self.ambiguous and self.pending is None:
            self.pending={"enqueued":now,"write_completed":None}
            return True
        # The caller still sends the mandatory keepalive. If an older response
        # is outstanding, subsequent fixed-string PONGs cannot be correlated.
        self.ambiguous=True
        return False

    def written(self, now):
        if self.pending and not self.ambiguous:
            self.pending["write_completed"]=now

    def observe(self, now):
        self.expire(now)
        if self.ambiguous or self.pending is None or self.pending["write_completed"] is None:
            self.unmatched+=1
            return None
        probe=self.pending
        self.pending=None
        late=now-probe['enqueued']>self.deadline_ns
        self.consecutive=0 if late else self.consecutive+1
        return {"enqueued":probe["enqueued"],"write_completed":probe["write_completed"],
                "observed":now,"rtt_ms":(now-probe["enqueued"])/1e6,
                "healthy":self.consecutive>=self.required,"consecutive":self.consecutive,"late":late}


class RecoveryBudget:
    """Nonblocking crash-safe leases; never reserve future phantom capacity."""
    def __init__(self, config, producer, provider, *, initial=False, tokens=0):
        base=Path(config.get("recovery_lock_root",Path(config.get("connection_rate_file","/tmp/lob-v2-connections")).parent/"recovery-budget"))
        # Slots conservatively reserve a full socket of token/snapshot capacity.
        per_socket=max(2,config.get("assets_per_connection",2000))
        domain=hashlib.sha256(str(provider).encode()).hexdigest()[:12]
        # All modes see the same shared slots. A smaller recovery quota must not
        # mean only slots [0,4) in a 16-slot pool: initial work may hold those four
        # while other slots are free. Separate quotas bound each mode instead.
        mode='initial' if initial else 'recovery'
        self.domains=[]
        shared=[]
        for kind,label,initial_default,recovery_default in (
                ('host','host',16,4),('plant','plant-'+producer.split('-')[0],8,2),
                ('worker','worker-'+producer,2,1),('provider','provider-'+domain,8,2)):
            initial_limit=config.get('initial_sessions_per_'+kind,initial_default)
            recovery_limit=config.get('recovery_sessions_per_'+kind,recovery_default)
            total=max(initial_limit,recovery_limit)
            limit=initial_limit if initial else recovery_limit
            if kind=='host':
                token_limit=max(1,config.get('initializing_tokens_per_host',32000)//per_socket)
                total=min(total,token_limit);limit=min(limit,token_limit)
            self.domains.append((base/(mode+'-'+label),limit))
            shared.append((base/label,total))
        self.domains.extend(shared)
        self.locks=[]
        self.tokens=tokens

    def try_acquire(self):
        self.release()
        for directory,count in self.domains:
            directory.mkdir(parents=True,exist_ok=True)
            acquired=False
            for i in range(count):
                file=(directory/str(i)).open("a+")
                try:
                    fcntl.flock(file.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:
                    file.close()
                    continue
                self.locks.append(file)
                acquired=True
                break
            if not acquired:
                self.release()
                return False
        return True

    def release(self):
        for file in self.locks:file.close()
        self.locks=[]

    async def acquire_async(self):
        task=asyncio.create_task(asyncio.to_thread(self.try_acquire))
        try:return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            self.release()
            raise


def classify(plant, shard, lane):
    local=plant.local_issue(shard,lane)
    if local:return local
    proof=plant.transports.get((shard,lane),{})
    if proof.get("reason")=="TRANSPORT_DEAD":return "TRANSPORT_DEAD"
    if not proof.get("healthy"):return "UNKNOWN_INGRESS_LAG"
    return "SUBSCRIPTION_NOT_READY"
