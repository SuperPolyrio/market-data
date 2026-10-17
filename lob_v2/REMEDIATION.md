# V2 full-scope / low-latency remediation, 2026-10-17

User-authorized guide: `/home/jiahuaiyu/.codex/attachments/e3c06d55-d98b-4914-a1fa-d5a7a89a050c/Polymarket LOB V2 全量采集与低延迟整改实施指南.md`.

Status: IN_PROGRESS / PARTIAL. Preserve the original eligible scope, four full-depth observations, market-level source selection, original bytes, LOCAL_DURABLE, and the original normalized-created → committed-applied 50 ms p99 boundary. No new global market cap or weakened freshness gate.

## Frozen evidence

- Baseline source commit `958b903`; runtime Python 3.12.13 / websockets 15.0.1 asyncio.
- A/B/C canary source commit `becc85e` (version 2.1.0), immutable release `lob_v2/releases/becc85ed1f1f`; only P/Q/G-0038 use its per-instance systemd drop-ins. Other workers still use the pre-remediation entrypoint. Keep old LOB collectors paused.
- Evidence root: `lob_v2/runs/remediation-20261017` (ignored runtime artifacts).
- `baseline-tests.xml`: 54 tests; `abc-tests.xml`: 60 tests, before subsequent D changes.
- `every-cut/report.json`: genuine archived production Tape, 18,217 records / 2,640 markets, no skipped pre-reset records. Every cut's full state + original frame chain identical. Prepare/apply working+committed CPU total 6571.42 → 557.12 ms (11.80x); this excludes network, decode, encode, disk, and verification. It is not a 50 ms production latency pass.
- First canary still failed the end-to-end local submit SLO: journal queueing and callback-resume stalls persisted. Do not report overall PASS.
- `coverage-before/report.json`: same catalog revision, 278,915 census markets; 313,320 assigned; G census 181,122 (64.94%). Detailed token observations in Parquet. Do not mix with the older 18:14/18:19 values.

## Implementation progress

- A/B/C implemented: persistent exact level maps, incremental normal changes, shared immutable parsed group, cached best levels; separate working/committed; dedicated bounded batch journal writer, ordered short writes and rotation, four receipt-backed cuts; cumulative stage histograms and long-tail traces; archive jobs constrained separately from journal I/O.
- D in progress, uncommitted and not deployed: copy-on-write frozen bootstrap snapshots, off-loop iterative serialization, bounded gateway snapshot application; indexed per-market plant selection; deferred qualification processing; bounded FIFO WS readers; one distinguishable text probe per session with late-PONG ambiguity; independent candidate sessions and host/plant/provider admission.
- D remaining: finish candidate handoff fault test, mixed initialized/uninitialized session tests, durable receipt/eligibility detail, bounded queue telemetry and cleanup, live canary, grouping 2000/1000/500 and fsync 2/5/10/20 comparisons.
- E remaining: persistent versioned catalog / tombstones, durable lifecycle input, success-watermark-safe scans, explicit unadmitted set, current-session reason audit and dynamic route delivery.
- F remaining: production Tape Parquet projection with independent immutable query receipts and quiet checkpoints; actual failover/remote recovery tests; independent-host plant DR if available; fixed-build 24h soak spanning UTC midnight. Existing production archive must remain active.

## External implementation references

- Nautilus ladder: `7192feab7a0f2c595cdab6e2dbf31e33dd185fbb`, persistent levels and snapshot boundary; implemented in `lobv2/book.py`.
- websockets reference: `e87ea9be0373edd5065b5e94dfa714cfde23023b`; deployed runtime remains 15.0.1, not the guide's reference 17.1. DATA processing and protocol PONG are separate; Polymarket text PING remains mandatory every 10s.
- PendulumFlow: `815388e0c782fe3f026d4a59f5d9447c44e0d13b`, market groups and session baselines; not a claim of cross-source native-event union.
- Aeron: `7de72648b50304af35dba794456739f6abcfb12f`, explicit sync boundary before visible position; no dependence on default unsynced recording semantics.

Physical boundaries: P/Q still share SUN and provider dependencies. Archive NVMe/HDD share XUE. Host DR remains NOT_RUN until actual isolation and recovery are demonstrated. Source upstream completeness remains UNKNOWN by protocol; captured observation order/bytes and selected Tape replay are independently verifiable.
