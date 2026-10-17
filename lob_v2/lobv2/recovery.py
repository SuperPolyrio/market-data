"""Session-scoped text probes and host/plant/provider recovery admission."""
import asyncio
import fcntl
import hashlib
from pathlib import Path
import time


class TextProbe:
    """An ambiguous fixed-string PONG never authorizes a subsequent probe.

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
            self.ambiguous=True
            self.consecutive=0
            return True
        return self.ambiguous

    def begin(self, now):
        self.expire(now)
        if not self.ambiguous and self.pending is None:
            self.pending={"enqueued":now,"write_completed":None}
            return True
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
        self.consecutive+=1
        return {"enqueued":probe["enqueued"],"write_completed":probe["write_completed"],
                "observed":now,"rtt_ms":(now-probe["enqueued"])/1e6,
                "healthy":self.consecutive>=self.required,"consecutive":self.consecutive}


class RecoveryBudget:
    """Nonblocking crash-safe leases; never reserve future phantom capacity."""
    def __init__(self, config, producer, provider, *, initial=False, tokens=0):
        base=Path(config.get("recovery_lock_root",Path(config.get("connection_rate_file","/tmp/lob-v2-connections")).parent/"recovery-budget"))
        # Slots conservatively reserve a full socket of token/snapshot capacity.
        per_socket=max(2,config.get("assets_per_connection",2000))
        host_limit=config.get("initial_sessions_per_host",16) if initial else config.get("recovery_sessions_per_host",4)
        plant_limit=config.get("initial_sessions_per_plant",8) if initial else config.get("recovery_sessions_per_plant",2)
        provider_limit=config.get("initial_sessions_per_provider",8) if initial else config.get("recovery_sessions_per_provider",2)
        host_limit=min(host_limit,max(1,config.get("initializing_tokens_per_host",32000)//per_socket))
        domain=hashlib.sha256(str(provider).encode()).hexdigest()[:12]
        self.domains=[(base/"host",host_limit),(base/("plant-"+producer.split("-")[0]),plant_limit),(base/("provider-"+domain),provider_limit)]
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


def classify(plant, shard, lane):
    local=plant.local_issue(shard,lane)
    if local:return local
    proof=plant.transports.get((shard,lane),{})
    if proof.get("reason")=="TRANSPORT_DEAD":return "TRANSPORT_DEAD"
    if not proof.get("healthy"):return "UNKNOWN_INGRESS_LAG"
    return "SUBSCRIPTION_NOT_READY"
