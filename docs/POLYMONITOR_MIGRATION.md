# Polymonitor acquisition cutover

Cutover date: 2026-09-06 (Asia/Shanghai).

## Production owner

`market-data` is the only owner of these acquisition paths:

| Data | Command | Storage |
| --- | --- | --- |
| Market | `python -m market_data market sync --watch` | `core.markets`, `core.market_tokens` |
| OrderFilled | `python -m market_data orderfilled --watch` | `core.orderfilled_raw`, ClickHouse `orderfilled_fact` |
| Oracle | `python -m market_data oracle --mode live --watch` | `oracle.*` event tables |

OrderFilled stores only immutable chain raw and BUY/SELL facts. It does not
write cashflow, PnL or positions. Unknown tokens remain in the durable registry
gap queue until official Market or combo evidence supplies an owner. Non-combo
gaps use a bounded, rate-limited official Gamma token lookup before replay;
the collector never creates placeholders.

Consumers keep using the existing PostgreSQL and ClickHouse contracts. They
may import the installed `market_data` package for the small canonical Gamma
token lookup helper, but must not import the retired polymonitor collectors.

## Installed services

- `polydata-market-sync.service`
- `polydata-trade-sync.service`
- `polydata-oracle-sync.service`
- `polydata-market-backfill.timer`
- `polydata-market-revisit.timer`
- `polydata-oracle-backfill.timer`
- `polydata-oracle-ctf-backfill.timer`
- `polydata-oracle-modules-backfill.timer`

The unit templates in `deploy/systemd/acquisition` run this repository's
`.venv/bin/python`, set `PYTHONDONTWRITEBYTECODE=1`, and share the existing
private `~/.config/polydata/polydata.env`. There must never be two OrderFilled
writers; the existing `polydata-trade-sync.service` name and flock are retained.

## Acceptance receipt

After the repository-owned interpreter cutover on 2026-09-06:

- all three live services were `active/running`, with zero restarts and no old
  collector process;
- a Market cycle persisted 2,140 canonical rows with zero rejects, conflicts
  or failure-ledger entries;
- 1,949 canonical rows created in the preceding two hours had zero missing
  Gamma ID, question ID, category or tags;
- an exact recent 100-block OrderFilled comparison found 6,704 PostgreSQL raw
  keys and 6,704 ClickHouse facts: 5,362 BUY and 1,342 SELL, with zero missing,
  extra, duplicate, unresolved, invalid-side or field-mismatch rows;
- `ops.orderfilled_registry_gaps` had zero open rows and
  `core.orderfilled_sync_windows` had zero incomplete rows;
- Oracle tables continued through the finalized Polygon head window and held
  8,300,770 Oracle plus 1,137,615 adapter rows at the check time.

The 712,166 old no-Gamma shells are no longer mislabeled canonical. One
transactional migration reclassified exactly:

- 507,457 `legacy_placeholder`
- 203,737 `legacy_combo`
- 957 `legacy_ghost`
- 14 `chain_v2`
- 1 `invalid_shell`

The receipt is `ops.sync_state['market.identity_kind.reclassification.v1']`.
All source classes were disjoint, with zero unclassified rows. Validated
database constraints now reject `canonical_gamma` without a Gamma ID and a
Gamma ID on an incompatible identity kind.

## Historical boundary

Live head proximity and bounded exact comparisons prove current acquisition;
they do not by themselves prove every historical row. Market and Oracle
backfills therefore remain resumable timers. Their terminal lane receipts are
the full-history gate.

The previous 1.9-billion-row ClickHouse snapshot was unique inside each block
partition and contained only BUY/SELL sides, but a single global PostgreSQL to
ClickHouse anti-join was intentionally not made a live-service prerequisite.
Historical completeness is instead checked in bounded/partitioned receipts so
an audit cannot starve acquisition.

Legacy combo rows are kept as `legacy_combo` until a matching official e333
proof is observed. Exact evidence promotes that row to `official_combo`; no
Gamma or question ID is fabricated. Historical placeholder/ghost rows may stay
as audit identities, but collectors and canonical consumers exclude them.

The retired Polymonitor acquisition entrypoints and their systemd units have
been removed after consumer imports were switched to this package. The old
cashflow, placeholder-remap and position-snapshot units are not part of the
collector target.

## Install and rollback

Create the repository virtual environment, install this package, render
`/__MARKET_DATA_ROOT__` in the unit templates to the absolute checkout, then
enable the three live units and five timers above. Server-side Python
consumers should install the repository editable with `--no-deps` so imports
resolve to this checkout.

For rollback, stop only the affected unit and restore its previous unit file.
Do not delete or regress cursor rows, and never run an old and new OrderFilled
writer together.

## Verification contract

- Market: completed created/revisit lanes plus canonical field constraints.
- OrderFilled: raw and fact cursors near finalized head, exact bounded raw/fact
  keys and fields, zero open registry gaps, zero incomplete windows.
- Oracle: both UMA and UpDown live cursors near finalized head.
- Full history: terminal Market, UMA, CTF and V2-module backfill receipts,
  assessed separately from live health.
