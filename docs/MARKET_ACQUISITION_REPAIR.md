# Market acquisition repair — 2026-09-06

The events backfill resumed after fixing an INSERT/ON CONFLICT interaction
with the existing immutable identity-alias trigger. The trigger runs before
conflict resolution, so a generated INSERT ID incorrectly looked like a third
owner when refreshing an existing market. The shared upsert now supplies the
existing ID obtained from its ownership check. New markets still use the
sequence; caller-supplied IDs cannot override the verified owner.

## Normalization and recovery

Two competitors may share a display label. Gamma market `2162691` represents
Karan Singh versus Digvijaypratap Singh and has two distinct tokens but two
`Singh` labels. The normalizer preserves those labels, token order and prices.
Repeated logical YES/NO/UP/DOWN labels remain rejected.

`market retry-failures` rechecks a bounded number of open normalization failures
through the official [market-by-ID endpoint](https://docs.polymarket.com/api-reference/markets/get-market-by-id).
It reuses the existing ownership checks, registry upsert and promotion receipts.
Only exact persisted Gamma/condition/token identities close a failure. Each
update also checks the selected failure key and occurrence count, so a newer
concurrent failure remains open. Original and latest recovery evidence is kept
in ledger detail. Scans preserve that detail when reobserving a failure.

A completed non-dry-run `market revisit` automatically retries up to 200
identities, oldest retry first. The existing hourly revisit timer owns this
work; there is no additional service or table.

```bash
.venv/bin/python -m market_data market retry-failures --limit 200 --dry-run
.venv/bin/python -m market_data market retry-failures --limit 200
```

## Original 108-failure cohort

| Result | Count | Evidence |
| --- | ---: | --- |
| Recovered and resolved | 9 | Gamma IDs 4278228–4278236 already had exact local market/token associations; their old failures were stale. |
| Recovered and resolved | 1 | Same-surname market 2162691 now passes normalization and exact registry persistence. |
| Still open | 98 | Direct official responses still lack both condition ID and CLOB token IDs. They remain quarantined and eligible for retry. |

An expanded dry-run examined 300 failures: 10 normalized successfully and 290
still lacked condition ID. Continuing historical scans expose additional
failures, so 98 is the remainder of the original cohort, not a global total.

## Validation and remaining boundaries

The final recorded snapshot is **2026-09-06 16:59:23 Asia/Shanghai**:

- The preceding 24 hours contained **38,260 canonical Gamma markets**, with
  **zero missing token pairs and zero token-owner/outcome mismatches**.
- Both created lanes and both updated-event revisit lanes were complete for
  their current scans. The verification revisit persisted 67,972 markets,
  then automatically rechecked 200 older failures; all 200 remained incomplete
  upstream and stayed open.
- Global open failures were **788 normalization errors for missing condition
  ID** (98 from the original cohort plus 690 newly exposed by historical
  scans) and **2 protected historical identity conflicts**. They are separate
  from the 10 resolved original failures and four pre-existing terminal cases.
- Live sync was running, and both backfill/revisit timers remained enabled.
  The next backfill slice was already in progress.

- 106 Market/shared/acquisition tests passed. Tests cover ID reuse, source-slot
  alignment, completed-scan recovery, dry runs, ownership conflicts and
  concurrent failure generations.
- Real PostgreSQL rollback probes reproduced the old alias error for all three
  protected canonical markets; the new shared upsert verified each market and
  both token rows without changing its ID.
- A real PostgreSQL rollback probe proved that recovery leaves a changed
  failure generation open and only resolves the matching generation.
- The final service-log adjustment was rechecked with all 57 Market/recovery
  tests. Automatic recovery logs aggregate counts; per-market evidence stays
  in the ledger, avoiding journald splitting oversized JSON receipts.
- Three consecutive 250-page backfill slices committed successfully and moved
  the events cursor from May 3 to April 11. Their persisted counts were 60,970,
  63,438 and 60,845 respectively; these are processing counts, not counts of
  newly created or globally distinct markets.
- The markets backfill lane retains its prior terminal receipt. The events
  lane is still running, so full-history completion is not claimed.

Two additional `condition_identity_conflict` entries are real local historical
identity gaps: official markets `2036788` and `2036789`, with four distinct old
tokens, conflict with protected alias-source rows. Those rows mix the old
condition/CLOB array with the newer Gamma/question/YES-NO identity of markets
`2132423` and `2132424`. Direct official responses confirm both old markets;
their Gamma owners and four tokens are absent locally. Original alias-proof
hashes match, but those proofs did not establish that the old official markets
could be discarded. These failures stay open. Correcting that historical
alias model requires a separate evidence-preserving migration; redirecting the
old markets or disabling ownership protection would produce incorrect data.

## Evidence

Local receipts are stored under
`/home/jiahuaiyu/.local/state/market-data/market-repair-20260906/`:

- `market-anomaly-audit-20260906.json`: the original 108 payloads, current
  official responses and exact database identity checks.
- `alias-trigger-probe.json` and `recovery-cas-probe.json`: actual PostgreSQL
  rollback validations.
- `recovery-dry-run.json` and `recovery-apply.json`: bounded replay results.
- `legacy-alias-conflicts.json`: official and database evidence for the two
  remaining historical identity gaps.
- `runtime-receipt.json`: timestamped live states, original-cohort/global
  failure counts, recent token integrity, service receipts and source hashes.
- `sha256.json`: evidence checksums.

The changes are present in the production checkout and are uncommitted.
Unrelated concurrent work in this checkout is preserved.
