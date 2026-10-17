"""Bounded cumulative histograms and per-record stage timing (milliseconds)."""
from bisect import bisect_left
from collections import Counter
import time

BOUNDS = (.05,.1,.2,.5,1,2,3,5,7.5,10,15,20,25,30,40,50,75,100,150,250,500,1000,2000,5000,10000,30000,60000,float("inf"))


class Metrics:
    def __init__(self):
        self.histograms = {}
        self.counters = Counter()

    def observe(self, name, milliseconds):
        row = self.histograms.setdefault(name,{"buckets":[0]*len(BOUNDS),"n":0,"sum_ms":0.,"max_ms":0.})
        row["buckets"][bisect_left(BOUNDS,milliseconds)] += 1
        row["n"] += 1
        row["sum_ms"] += milliseconds
        row["max_ms"] = max(row["max_ms"],milliseconds)

    def finish(self, rec):
        t = rec.trace
        for name,start,end in (
            ("ingress_wait","ingress_enqueue","ingress_dequeue"),
            ("parse","parse_start","parse_end"),
            ("source_apply","source_apply_start","source_apply_end"),
            ("working_apply","working_apply_start","working_apply_end"),
            ("journal_wait","journal_enqueue","journal_dequeue"),
            ("write","write_start","write_end"),
            ("sync_syscall","sync_syscall_start","sync_syscall_end"),
            ("callback_resume","sync_syscall_end","sync_callback_resume"),
            ("committed_apply","committed_apply_start","committed_apply_end"),
            ("commit_to_publish_enqueue","committed_apply_end","publish_enqueue"),
            ("recv_to_commit","recv_return","committed_apply_end"),
            ("normalized_to_commit","normalized_record_created","committed_apply_end")):
            if start in t and end in t:
                duration = (t[end]-t[start])/1e6
                if name=="recv_to_commit" and rec.body.get("kind")=="SOURCE_RESET":
                    self.observe("baseline_input_to_reset_commit",duration)
                else:
                    self.observe(name,duration)
                if name == "normalized_to_commit":
                    self.observe(name+":"+rec.body["kind"],duration)

    def snapshot(self):
        result = {}
        for name,row in self.histograms.items():
            percentiles = {}
            for label,fraction in (("p50",.5),("p95",.95),("p99",.99)):
                count = 0
                for bound,n in zip(BOUNDS,row["buckets"]):
                    count += n
                    if count >= max(1,row["n"]*fraction):
                        percentiles[label+"_upper_ms"] = bound if bound!=float("inf") else None
                        break
            result[name] = {**row,"buckets":list(row["buckets"]),**percentiles}
        return {"histogram_bounds_ms":[v if v!=float("inf") else None for v in BOUNDS],
                "histograms":result,"counters":dict(self.counters),"sample_policy":"ALL_COMPLETED_RECORDS"}


def queue_snapshot(queue):
    now = time.monotonic_ns()
    return {"items":queue.queue.qsize(),"bytes":queue.bytes,"high_water_bytes":queue.high_water,
            "oldest_age_ms":(now-queue.arrivals[0])/1e6 if queue.arrivals else 0.,
            "enqueued_items":queue.enqueued_items,"enqueued_bytes":queue.enqueued_bytes,
            "dequeued_items":queue.dequeued_items,"dequeued_bytes":queue.dequeued_bytes,
            "observation_mono_ns":now,"failed":queue.failed}
