# market-data acquisition

Minimal production engine for Polymarket data:

- Gamma canonical markets and token registry in PostgreSQL
- Polygon OrderFilled immutable raw plus BUY/SELL facts in ClickHouse
- Polygon UMA, Adapter, NegRisk and UpDown oracle events in PostgreSQL

It never creates placeholder markets and contains no cashflow, PnL, dashboard or one-off repair pipeline. Unknown OrderFilled tokens remain in raw storage and an unresolved registry queue until a source-proven owner exists. Non-combo gaps use a bounded, once-per-minute official Gamma token lookup before their full raw history is projected; e333 combo gaps require exact official Data API evidence.

## Commands

```bash
python -m market_data market sync --watch
python -m market_data market backfill --max-pages 250
python -m market_data market revisit --max-pages 100
python -m market_data market classify-identities
python -m market_data orderfilled --watch
python -m market_data oracle --mode live --watch
python -m market_data oracle --mode backfill
```

`market classify-identities` is read-only unless `--apply` is supplied. Apply is a single CAS transaction, validates database constraints and stores one receipt in `ops.sync_state`; it creates no local artifact.

Production systemd templates are in `deploy/systemd/acquisition`. Consumers read the existing PostgreSQL and ClickHouse storage contracts; Python helpers resolve to this installed repository. The retired Polymonitor acquisition entrypoints and cashflow/placeholder-remap units have been removed.
