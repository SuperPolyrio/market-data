# OrderFilled acquisition acceptance — 2026-09-06

OrderFilled acquisition is owned by this repository and the single
`polydata-trade-sync.service` writer. Raw logs are durable in PostgreSQL before
projection to ClickHouse. A raw window marked `complete` proves raw key
equality for that window; fact completeness additionally requires projection
and registry-gap checks.

## Repaired gap lifecycle

Previously, a token could have old raw events waiting for market metadata.
When its owner became available, projecting a fresh event marked that token's
gap resolved immediately. The subsequent retry selected only open gaps, so it
could permanently skip the older raw events.

Ordinary live and bounded projections now leave existing gaps open. Only
`retry_resolved_gaps`, after loading the token's full raw history, can close
its gap. ClickHouse must accept the projection first, and a token with any
unresolved rows remains open. Gap closure is committed in the same PostgreSQL
transaction as the existing replay bookkeeping. Retrying a partially
completed write still uses the existing fact-key check.

The regression covers owner arrival followed by a fresh event, successful
historical replay, ClickHouse failure, and an owner disappearing before
replay. Validation: **34 tests passed** with:

```bash
.venv/bin/python -m pytest -q tests/test_orderfilled.py tests/test_shared.py tests/test_combo.py
git diff --check
```

## Production evidence

The service was restarted with the fix at **2026-09-06 16:45:41 +08:00**.
Acceptance receipts and the read-only audit script are retained locally in
`/home/jiahuaiyu/.local/state/market-data/orderfilled/`:

- `audit-before-20260906.json`: 1,575 tokens resolved by the new engine since
  local midnight; all 2,662 PostgreSQL raw events through block 93,319,491
  matched ClickHouse facts, with no missing, extra, duplicate or field-mismatch
  rows. The observed premature-closure code path was therefore not shown to
  have caused loss in this scope; no production data repair was necessary.
- `restart-progress-20260906.json`: post-restart cursor and gap snapshots.
- `audit-after-20260906.json`: post-restart chain/PG, PG/ClickHouse and resolved
  token history comparison, including exact block bounds and source checksum:
  - chain/PG: blocks 93,319,619–93,319,638, **1,281** identical rows;
  - PG/ClickHouse: blocks 93,319,539–93,319,638, **5,204** identical rows;
  - all **1,609** engine-resolved tokens in the stated daily scope: **2,708**
    historical raw rows matched facts;
  - all three comparisons had zero missing, extra, duplicate or field-mismatch
    rows; no open registry gaps or incomplete raw windows remained at the check.
- `audit_20260906.py`: reproduces these read-only checks using the running
  service's configuration, without printing credentials or writing databases.

These are bounded acquisition receipts, not a proof of all historical chain
coverage. The 39 pre-existing `source_absent_terminal` registry entries are
distinct from open gaps. The older 3,925-window recovery has a terminal exact
audit in the retired project's artifacts and is not outstanding acquisition
work.
