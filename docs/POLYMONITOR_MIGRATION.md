# Polymonitor acquisition migration

Inventory date: 2026-09-06 (Asia/Shanghai).

## Production cutover state

| Dataset | Installed service | Effective entrypoint | Former polymonitor entrypoint |
| --- | --- | --- | --- |
| Market | `polydata-market-sync.service` | `python -m market_data market sync --watch` | `scripts/market/market_discovery.py --incremental --watch` |
| OrderFilled | `polydata-trade-sync.service` | `python -m market_data orderfilled --watch` | `run_trades_indexer_clickhouse_live.sh` → `trades_indexer.py` |
| Oracle | `polydata-oracle-sync.service` | `python -m market_data oracle --mode live --watch` | `fetch_uma_oracle_chain.py --continue-sync --include-updown --watch` |

The three live services now run from the `market-data` working directory. The
Market revisit timer and the separate Market/Oracle backfill timers are also
installed. The backfill lanes have not yet established terminal full-history
receipts, so this cutover proves current acquisition but does not yet prove
complete historical coverage.

The installed base unit files now contain the new entrypoints directly; the
temporary cutover command overrides and obsolete monolith tuning fragments
have been removed. `polydata-local-collector.target` owns only these three
collectors and is enabled. The remaining Market and OrderFilled `network.conf`
files are host-specific proxy/RPC routing, not command overrides.

### Final live acceptance update

At 2026-09-06 15:03 CST the three installed collectors were still running from
this repository with no former acquisition process present:

- Market's 14:42 cycle persisted 3,953 canonical rows with zero rejection,
  conflict or failure.  Both revisit lanes subsequently reached
  `complete=true`.  A psycopg failure-ledger bug was exposed only when a bad
  upstream payload appeared: literal wildcard parsing was removed, and a real
  production revisit then ledgered two rejects and committed normally.
- Failure identity no longer hashes mutable raw JSON.  Existing terminal rows
  are reused by stable source identity, and the two shadow `open` rows for
  Gamma IDs 518589 and 535287 were retained but terminalized.  This prevents a
  changed upstream payload from reopening an already classified terminal item.
- OrderFilled had zero open registry gaps.  The formerly short window
  93,314,447--93,314,450 was restored from 222 to all 224 BUY/SELL facts.  A
  newer 100-block sample contained 5,643 PostgreSQL raw keys and exactly 5,643
  ClickHouse facts, with zero missing, extra, side or block mismatches.
- Combo-detail lookup now filters by encoded condition and no longer requires
  a lifecycle event transaction hash to equal the OrderFilled transaction.
  The restarted live service has repeatedly resolved new e333 conditions and
  projected their pending facts to zero unresolved rows.
- Oracle committed all three live cursors through block 93,314,624 while the
  finalized Polygon head was 93,314,654.  Its NUL-safe historical backfill had
  advanced through 82,599,538 with no repeat of the failing window.

Market backfill also now treats a terminal historical lane as sticky.  A
completed lane is skipped on later timer invocations instead of being restarted
from the head and incorrectly changed back to `complete=false`.  The previously
committed 14:28 markets-lane terminal receipt was restored from its systemd
journal evidence; a production slice then reported that lane as
`pages=0, complete=true` and continued only the unfinished events lane.

The former entrypoints remain in polymonitor only because repair tools, tests
and application consumers still import them. They are no longer the effective
live service commands and must not be deleted until those imports are removed.

### Cutover verification receipt

At 2026-09-06 12:43 CST all three services were active from this repository
with zero service restarts:

- Market persisted 2,434 canonical rows in its latest cycle with zero rejected
  rows; both created lanes were terminal and the legacy health watermark was
  projected successfully.
- OrderFilled raw/fact/compatibility cursors were aligned. A fixed recent
  100-block audit found 5,420 PostgreSQL raw keys and the identical 5,420
  ClickHouse keys, with 4,263 BUY, 1,157 SELL, zero duplicates and zero open
  registry gaps.
- Oracle's UMA, legacy health and UpDown cursors were aligned at block
  93,309,750. The completed cycle stored 1,820 UMA, 1,488 Adapter and 16 CTF
  events with source-key uniqueness.

The e333 migration audit also corrected every then-present structural combo
fact to `outcome_code=0`; 1,662,408 rows had the same number of unique keys and
zero non-structural codes. A separate 47-row historical raw-to-fact omission
was projected fact-only and verified exact. These operations did not write a
cashflow table.

After that receipt, the Market revisit lane was changed from a repeated
end-date traversal to a resumable `updatedAt`-ordered v2 traversal. Both open
and closed lanes reached their terminal boundaries at 12:46 CST and remain
scheduled hourly for newly updated events. Market full-history backfill now
runs under its own resumable timer. Oracle historical backfill adopted the
minimum proven legacy boundary at block 82,203,538 and also runs under a
separate timer.

Successive Market full-history slices have continued moving both source
cursors backwards with no ownership conflicts. The completed slices are
measured progress, not a terminal receipt.

The earlier revisit receipt exposed 22 valid Gamma markets whose 44 real tokens
were still owned by old local placeholders. The engine now permits only a
strictly normalized Gamma identity to replace an explicit placeholder or
`protocol_structural` shell, using compare-and-swap token ownership and a
durable `ops.market_canonical_promotions` ledger. The exact recovery promoted
22 market identities and rehomed all 44 tokens. Their 733 existing e111 facts
already referenced the correct 22 market/condition identities; a fact-only
mutation corrected 324 Over and 409 Under outcome codes while preserving 668
BUY, 65 SELL and 733 unique keys. The two other Gamma payloads (`518589` and
`535287`) have no condition or tokens and are now retained in
`ops.market_acquisition_failures`, rather than silently skipped or fabricated.

### Historical OrderFilled audit boundary

At the fixed block cutoff 93,310,310, ClickHouse held 1,922,066,043 physical
facts: 1,386,952,144 BUY and 535,113,899 SELL, with no invalid side. Each of the
89 million-block partitions was internally key-unique. All 507,457 historical
placeholder market IDs had zero fact references and all 175,134 sync windows
were complete with zero declared missing rows.

This is not a global PostgreSQL-to-ClickHouse equality proof. PostgreSQL raw is
about 396 GB; the bounded full-table comparison was stopped after eight
minutes, after reading 134 million rows. Cross-partition duplicate keys and the
global raw/fact anti-join therefore remain unproven.

The former 21,395-row ghost residual is now closed from official CLOB evidence.
Both `/markets-by-token/{token}` and `/markets/{condition}` agreed on all 955
source tokens, their seven previously absent complements and 481 unique binary
conditions, with no ambiguity. PostgreSQL now records 481 `canonical_clob`
markets, 955 compare-and-swap token rehomes and seven complement inserts in two
immutable evidence ledgers. A fact-only, 30-partition shadow replacement moved
all 21,395 references: 3,068 BUY and 18,327 SELL, with 10,801 source-slot-0 and
10,594 source-slot-1 outcomes. The terminal audit found 21,395 physical rows,
21,395 unique keys, zero old ghost-market references, zero invalid sides or
outcomes, zero scratch tables and zero unfinished mutations. No cashflow table
was read or written.

Two additional live e333 conditions exposed an unnecessary availability gate:
official `/activity` already bound the exact transaction, token, encoded
condition and `isCombo=true`, while the secondary combo-detail index had not
yet published legs. The resolver now accepts only that precise detail-pending
case as a structural `official_combo`; contradictory detail remains blocked
and logical outcome stays unknown. The three tokens were installed and all
four preserved raw facts were projected, leaving no open registry gaps.

Oracle backfill also encountered historical ancillary bytes containing NUL.
The authoritative hex value was already retained; the readable TEXT projection
now escapes NUL as a literal `\\x00`. A production restart advanced both Oracle
backfill cursors beyond the failing window.

## Keep in the new engine

### Market

- Gamma `/markets/keyset` and `/events/keyset` walkers
- strict canonical normalization (`gamma_market_id` and `condition_id` required)
- source-order outcome/token mapping
- `core.markets`, `core.market_tokens` idempotent writes
- evidence-backed archived CLOB identities kept distinct from Gamma identities
- durable normalization/conflict failure and shell-to-canonical promotion ledgers
- high-frequency created lane, lower-frequency event revisit, resumable backfill

### OrderFilled

- both current event ABIs and all five verified exchange addresses
- finalized Polygon range fetch, retries/fallback and exact raw-key audit
- immutable PostgreSQL raw rows and an independent raw cursor
- token-registry projection into ClickHouse BUY/SELL facts
- unresolved-token queue; retry after Market registry catches up
- exact official Data API evidence for e333 `official_combo` identities

An `official_combo` is not a Gamma market and has no fabricated Gamma ID or
question ID. Its two token positions remain structurally named
`position_0`/`position_1`; projected facts use `outcome_code=0` because logical
YES/NO semantics are unproven.

### Oracle

- official UMA OO/OOV2, Adapter, NegRisk and UpDown contract/event union
- requester allowlist and chain/address fail-closed checks
- finalized range fetch, rewind, idempotent writes and monotonic cursors
- adapter/request mappings and read-only `core.markets` semantic bridge
- bounded live mode and explicit full-history mode

## Do not migrate into runtime

- placeholder creation, remapping, canonical bridge and identity repair
- historical outcome/source-proof/registry-gap repair workers
- category/tag repair epochs and serving-cache projections
- cashflow, non-trade cashflow, PnL and position materializations
- Data API `/trades` snapshots (not the OrderFilled chain fact)
- SQLite/MySQL compatibility and migrations
- dashboard/API/quant/strategy consumers
- one-off JSON/Parquet exporters and investigation scripts

These tools explain much of the old size but are not acquisition dependencies.
Audit receipts may be retained as cold evidence; executable repair code should
not be copied into this repository.

## Measured bloat in the source repository

- Market directory: 50 Python files / 75,133 lines; 37 untracked repair scripts
  alone account for 58,985 lines.
- Current Market monolith: 9,300 lines; its tracked version is 4,198 lines.
- Current OrderFilled monolith: 3,674 lines and directly imports Market and
  multi-thousand-line placeholder/structural repair modules.
- Oracle acquisition: 3,594-line entrypoint plus a separate UpDown module and a
  large multi-backend database layer.

The new implementation extracts behavior, not those files.

## Remaining migration work

1. Commit this repository and record the deployed commit SHA; production must
   not depend on an unversioned working tree.
2. Run the remaining Market events lane and both Oracle backfill cursors to
   terminal boundaries and retain their final receipts. Live head proximity is
   not historical proof.
3. Complete a partitioned global PostgreSQL raw-to-ClickHouse fact anti-join;
   current per-partition uniqueness and exact bounded receipts are not a full
   equality proof.
4. Reclassify the 203,737 structurally valid legacy Data API combo shells only
   after each condition has an exact official evidence receipt. Their
   historical BUY/SELL facts are already complete and use `outcome_code=0`;
   this remaining work concerns registry classification, not raw acquisition.
5. Correct the broader historical `identity_kind` label: about 712,166 old
   no-Gamma shell rows still carry the schema's `canonical_gamma` default.
   Consumers must require a real Gamma ID until those audited subtypes are
   reclassified.  Also rebuild the 83 old 2021--2022 `clob_token_ids` JSON
   projections whose canonical YES/NO token columns are already valid.
6. Port or retire the remaining polymonitor imports, then replace the former
   entrypoints with small compatibility shims or remove them at zero references.
7. Delete proven one-off repair executables only after preserving their final
   receipts; never copy them into `market-data`.

Physical deletion remains the final step because a systemd rollback and the
remaining compatibility imports still need the former code today.

The OrderFilled service deliberately keeps the installed
`polydata-trade-sync.service` name. Enabling a second differently named writer
would create an unsafe dual-writer deployment. The cutover used the one-time
`market-data orderfilled --adopt-legacy-cursor` migration while the former
writer was stopped. Do not rerun it: the command correctly refuses once either
new cursor exists.

## Install and rollback

Copy `deploy/systemd/acquisition/polydata.env.example` to the private runtime
environment file and replace all example values. Install the unit templates
only after replacing `/__MARKET_DATA_ROOT__` with the absolute repository path.
The live units are grouped by `polydata-local-collector.target`; revisit and
backfill timers are enabled separately.

For rollback, stop only the affected unit, restore its prior working directory
and `ExecStart`, run `systemctl --user daemon-reload`, and restart that unit.
Never delete or regress the new cursor rows during rollback, and never run old
and new OrderFilled writers simultaneously; the shared flock is a secondary
guard, not the migration protocol.

## Verification contract

- Market requires terminal created/revisit lane states and canonical identity
  checks; processed/upserted counters alone are not counts of newly created rows.
- OrderFilled requires finalized-head proximity for both new cursors, exact raw
  window keys, and an explicit open registry-gap count. `trade_sync` is only a
  compatibility cursor and pauses when gaps are open.
- Oracle requires both `oracle_sync` and `oracle_updown_sync_live`; UMA progress
  alone does not prove UpDown coverage.
- Full history remains unproven until both Market backfill lanes and both Oracle
  backfill cursors reach their terminal source boundaries.
