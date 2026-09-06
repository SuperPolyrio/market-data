# Oracle settlement evidence

`oracle.market_settlement_evidence` retains one row for **every** current
`core.markets.id`, including unresolved markets and unknown identities. It
supports CTF conditions, native V2 Binary/NegRisk conditions, and combinations
of native V2 legs. Identity kind alone does not decide which protocol applies:
native condition IDs and actual source contracts determine that relationship.
Ensure `oracle_modules.ensure_schema(conn)` first, then install explicitly with
`market_data.oracle_evidence.install_settlement_view(conn)` and commit the
caller's transaction. `read_market_evidence(conn, ids)` reads
selected markets using the same SQL definition scoped before aggregation;
`coverage_summary(conn)` counts the full stored inventory by identity kind and
readiness in keyset batches of 10,000 existing IDs. Run coverage inside a
caller-owned `REPEATABLE READ READ ONLY` transaction for a coherent snapshot.
The maximum ID is frozen at the start, and results return only after every
batch completes. The optional `on_progress` callback reports progress, never
final totals. Coverage disables PostgreSQL JIT locally for the caller's
transaction because compiling the full evidence expression dominated each
small batch. Native lookups are driven only by actual native condition/leg
IDs, so a CTF batch does not scan all module history. These reads never update data.
Use the Python read helper for small inventories: filtering the global view
can still aggregate all CTF history even when only one market is requested.

```python
from market_data.oracle_evidence import read_market_evidence, payout_for_amount

requested = {123, 456}
by_id = {row["market_id"]: row for row in read_market_evidence(conn, requested)}
if requested - by_id.keys():
    raise LookupError("requested markets are absent from core.markets")
# payout_for_amount raises if the market or token lacks proven settlement.
payout = payout_for_amount(by_id[123], token_id, native_amount)
```

For CTF, `READY` requires one stored `ConditionResolution` for the exact condition,
valid transaction/log/block/time provenance, a nonnegative integer payout
vector with positive uint256 denominator, and a complete token registry with
matching condition IDs and contiguous outcome indices. The source contract is
the Polygon CTF contract. The event's own oracle and question ID are exposed.
Payouts are exact numerator/denominator **strings** per token and native
`outcome_index`; supplied outcome labels remain metadata. No YES/NO or winner
meaning is inferred from a token column name. A missing request does not
invalidate a final CTF payout; `ctf_request_count` exposes that separate gap.

| Readiness | Meaning |
| --- | --- |
| `READY` | Final CTF/native evidence or a proven terminal combo, and its positional token mapping, pass these checks. |
| `PENDING` | Active, open CTF/Binary/NegRisk market with no final payout and no past end time; CTF additionally has no UMA settlement. |
| `MISSING_CTF_SETTLEMENT` | No CTF settlement is stored; this does not prove one exists on chain. |
| `INVALID_CTF_PAYOUT` | Malformed, negative, oversized, zero-total, or unsupported payout vector. |
| `INCOMPLETE_CTF_PROVENANCE` | Required chain event coordinates or source fields are absent. |
| `TOKEN_MAPPING_CONFLICT` | Stored payout cannot be assigned to a complete, consistent token registry. |
| `CONFLICTING_CTF_SETTLEMENTS` | Multiple distinct stored resolution logs claim the same condition. |
| `MISSING_MODULE_SETTLEMENT` | A native Binary/NegRisk condition has no stored final module result. |
| `MISSING_COMBO_PREPARATION` | The native combo's authoritative ordered legs have not been stored. |
| `MISSING_COMBO_LEG_RESULT` | At least one leg result is absent and no resolved zero leg makes the payout terminal. |
| `INVALID_MODULE_PAYOUT` | A native result is not two nonnegative integers summing to 1,000,000. |
| `INCOMPLETE_MODULE_PROVENANCE` | A required preparation or resolution event lacks chain coordinates/time. |
| `MODULE_IMPLEMENTATION_UNVERIFIED` | No usable implementation observation or upgrade event establishes the current native module code. |
| `MODULE_IMPLEMENTATION_UNSUPPORTED` | The latest observed native module implementation differs from the verified calculation version. |
| `CONFLICTING_MODULE_SETTLEMENTS` | Multiple stored final resolution logs claim the same native condition. |
| `INVALID_COMBO_PREPARATION` | Legs violate supported native types, positions, ordering, or uniqueness. |
| `CONFLICTING_COMBO_PREPARATION` | Stored preparation events disagree on the condition's leg definition. |
| `UNSUPPORTED_MARKET_IDENTITY` | The identity is neither a CTF condition nor a supported native V2 condition. |

Native V2 evidence comes from `oracle.module_events`. Only the exact
`ConditionResolved(bytes31,uint256[])` event on the registered Binary or
NegRisk module supplies a final leg result; `ResultReported`, preparation,
and migration metadata alone do not. Results must contain two integers summing
to 1,000,000. The market's token pair is checked against its native condition
encoding as well as the shared token registry.

Combo evidence uses stored `CombinatorialConditionPrepared` legs and those
native final results. The collector verifies the ABI-derived condition hash.
The view checks canonical order, unique conditions, native module IDs,
outcome indices, and the exact encoded position ID for each leg. Every leg must
resolve unless a resolved leg's referenced
outcome has zero payout; that zero makes positions 0 and 1 terminal at 0 and
the full amount respectively. Missing and contradictory records remain
visible. `supporting_events` retains the preparation and leg event coordinates;
`settlement_basis='COMBO_LEG_RESOLUTIONS'` identifies a derived result rather
than an invented combo resolution log. `settlement_event_table` distinguishes
module event IDs from IDs in `oracle.oracle_events`.

The source implementation accumulates two factors at scale 10^36, rounding
down for position 0 and up for position 1 after each fractional leg. The view
preserves these exact integer operations. Its payout numerators are
`[factor_down, 10^36-factor_up]`, with **fixed denominator 10^36**, which must
not be replaced by the numerators' sum. `payout_for_amount(evidence, token_id,
amount)` returns the exact integer payout for native token units. Multiplying
a displayed payout for one token by an arbitrary position amount is not a
substitute for this rounding rule. The helper also enforces the source's
uint256 multiplication limits: position 1 checks the UP factor's product
before complementing. A proven zero leg returns before multiplication;
factor rounding to zero alone does not establish that shortcut.

Native module addresses are pinned to Polymarket's
[official deployment registry](https://github.com/Polymarket/contract-security).
The current combo calculation is pinned by `calculation_version` to the
[Sourcify exact-matched implementation](https://sourcify.dev/server/v2/contract/137/0x572cd48cce93b2e58f1cc0253a7fdd4b4952a9c2?fields=all).
Binary and NegRisk calculations are similarly pinned to implementations
`0x492fec596ec347459e1ebe30b9245eb3b49b1bba` and
`0xa61e7ca374f721d5b9fd5b0fee6fb90f27d448d7`.
`module_implementation_evidence` exposes the current version proof separately
from settlement events. The collector stores EIP-1967 implementation-slot
observations at an explicit confirmed block, with its block hash and observation
time, in `oracle.module_implementation_observations`. The view selects the
latest observation or subsequent `Upgraded` log; an end-of-block storage
observation takes precedence over a log in the same block. Unknown implementations
fail closed for native conditions, combos, and every referenced leg module,
including an unresolved leg in a zero-payout combo. A storage observation is
never represented as a historical chain event.
Preparation/resolution events are persisted and replayable; reads do not
substitute live RPC `getPayout` calls for historical evidence. A historical
acceptance policy must additionally select the effective implementation era
and its payout rules. The current view's calculation version does not prove
that an older proxy implementation used the same rounding rules.

UMA requests, proposals, disputes, and the latest settlement are reported
separately. A UMA settlement is a **protocol-adjudication label**; it does not
prove that the market's final CTF payout was reported. UMA `payout` is an oracle
bond/reward payment and must not be read as a token payout vector. Adapter
resets, emergency resolution, old adapters, and NegRisk translation can make
the UMA lifecycle differ from the actual condition resolution. Exact CTF
condition evidence applies across regular, UpDown, NegRisk, archived CLOB,
and older CTF markets without relying on a slug convention. Migrated native
V2 conditions still require their own module resolution; a legacy CTF payout
alone does not prove that migration resolution was executed.
Gamma can retain the legacy bytes32 CTF identity while a native module uses a
distinct bytes31 condition. Stored `MigrationResolved` and
`MigrationConditionRegistered` payloads expose that bridge; IDs must not be
padded, truncated, or silently substituted. A market retaining its CTF identity
continues to use its exact CTF token payout evidence.

This is a current database evidence interface, not a claim of complete chain
history or a historical point-in-time acceptance gate. A collector cursor or
one sample cannot prove all history complete. Consumers doing historical
evaluation must additionally validate the canonical/finalized block and their
decision cutoff, and retain ingestion/availability evidence. `event_time` is
chain time; `created_at` is the original stored row time and does not timestamp
later enrichment. The view deliberately does not call it `available_at`.

The legacy Polymonitor `core.market_status_snapshot` is owned by
`scripts/db/sync_trade_analytics.py`. Its incremental job scans increasing
`oracle_events.id`, so late `UPDATE market_id` changes below its watermark can
remain invisible. Existing snapshot consumers need an explicit refresh of
affected markets or must read this view. A raw event repair alone is not proof
that a snapshot, API, or backtest has incorporated that repair. This view reads
the current event-to-market links directly and has no such watermark.

Installing the view does not migrate a consumer. Consumers must read requested
market IDs explicitly, reject missing IDs and non-`READY` evidence for payouts,
and settle the exact token and native amount using the positional payout map
and `payout_for_amount`. They must preserve the event table namespace and
supporting events: a module event ID is not an `oracle.oracle_events` foreign
key, and a combo has several supporting events. Re-read affected markets after
late enrichment and backfill instead of relying solely on increasing event IDs.
Market closure remains a separate lifecycle signal.

The LOB registry in `src/market_data/lob/control_runtime/registry/` currently
also consumes the legacy snapshot and stores `winning_asset_id` /
`winning_outcome`. Those winner fields cannot represent a fractional or combo
payout vector. A registry integration must retain exact payout evidence and
clear stale winner claims where that evidence does not prove one unique
full-payout token; it must not convert every `READY` vector into one winner.
This document and the view alone do not certify that such a consumer has been
adapted or that any trading/backtest ledger used the repaired evidence.

## Acquisition and repair

The collector stores **all** CTF preparation/resolution events before market
metadata exists. It links CTF by the exact condition ID, preserving the chain
question/condition. UMA and adapter enrichment is retried in rotating bounded
pages after each collector cycle; a missing old market cannot block later
pages. Conflicting identities remain unlinked. Replaying an event cannot erase
an existing nonempty market link. The `ctf_events` counter measures this expanded
scope; `updown_events` is retained as its legacy counter alias.
`polydata-oracle-reconcile.service` runs
`oracle --mode reconcile --max-windows 0` independently; its timer runs every
10 minutes to finish the remaining pages of the rotating pass. Late market
links therefore do not rely only on each live collector cycle's bounded
enrichment page. A busy reconciliation lock reports `skipped_busy=1` and
`pass_complete=0`; a skipped run is not a completed reconciliation pass.

```bash
market-data oracle --mode reconcile --max-windows 0
market-data oracle --mode backfill --uma-only --window-blocks 10000 --max-windows 1000
python -m market_data.oracle_repair --audit-only --from-block 93317410 --to-block 93318409
python -m market_data.oracle_repair --apply --window-blocks 100000
python -m market_data.oracle_modules --apply --window-blocks 1000000
```

Historical acquisition has three independent resume paths:

| History | Origin | Cursor | Service and evidence |
| --- | --- | --- | --- |
| UMA, adapters, legacy NegRisk bridges | 23,569,780 | `oracle_backfill_full` | `polydata-oracle-backfill.service` runs `--uma-only`; events and cursor commit per window. |
| CTF preparation/resolution | 4,023,686 | `oracle_ctf_backfill_v2` | `polydata-oracle-ctf-backfill.service`; chain/database comparison in `ops.oracle_ctf_repair_windows`. |
| V2 Binary/NegRisk/Combo modules | 87,479,439, with each proxy's own deployment bound | `oracle_modules_backfill_v1` | `polydata-oracle-modules-backfill.service`; chain/database comparison in `ops.oracle_module_backfill_windows`. |

`--uma-only` is accepted for bounded audits and historical backfill. It skips
CTF and V2 module acquisition and advances only the UMA backfill cursor. The
independent CTF/module backfills retain their own origins, locks and receipts;
their timers continue hourly after each completed run. The live collector still
acquires all three groups, recording `oracle_sync`, `oracle_updown_sync_live`
and `oracle_modules_sync_live`. The historical legacy
`oracle_backfill_updown` cursor does not certify either complete CTF history or
native module history.

The UMA cursor records committed acquisition progress. It is not a CTF/module
style independent chain/database equality receipt, and a bounded UMA sample
does not establish a full-history equality audit.

The optional production lookup index is defined in
[`sql/oracle_indexes.sql`](../sql/oracle_indexes.sql). It indexes the fixed
64-character SHA-256 hex digest of adapter ancillary data, avoiding the B-tree
entry-size limit of a full ancillary hex value. `_refresh_adapter_projection`
keeps both the digest predicate and full ancillary equality; the digest is an
index access path, not a replacement for exact identity. Install this concurrent
index outside a transaction when provisioning another database. Creating the
settlement view does not create this acquisition index.

`--mode reconcile --dry-run` executes one transaction and rolls it back. It does
not change chain cursors. The standalone CTF audit defaults to read-only and
never creates schema or advances state. Apply mode uses a separate advisory
lock and the `oracle_ctf_backfill_v2` cursor. Each verified window's events,
`ops.oracle_ctf_repair_windows` receipt, and cursor commit atomically. Any
missing/extra/mismatched transaction/log identity prevents advancement.
Payout/condition/question/oracle/block fields are compared with decoded RPC
logs; repaired event timestamps are checked against RPC blocks. Unchanged
non-null historical timestamps are not independently re-fetched in this pass.

The full CTF origin is **4,023,686**, verified against the [pinned official
manifest](https://github.com/Polymarket/polymarket-subgraph/blob/7a92ba026a9466c07381e0d245a323ba23ee8701/networks.yaml)
and historical contract bytecode. The preceding block has no CTF code. This
predates the UMA/adapter origin, 23,569,780. Legacy UpDown/backfill cursors
cannot certify the expanded scope. `polydata-oracle-ctf-backfill.service` resumes
this independent history scan; its timer extends completed scans hourly.
It does not replace UMA/adapter/NegRisk backfill.

Completion requires each dedicated receipt to reach its recorded confirmed
head target with contiguous verified windows from the deployment origin.
Even then, `PENDING`, unsupported identities, and token mapping conflicts stay
separate from `READY`; no oracle collector can manufacture a future resolution.
