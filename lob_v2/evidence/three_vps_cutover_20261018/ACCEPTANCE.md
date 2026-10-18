# Three-VPS LOB V2 routing acceptance — 2026-10-18

**PASS for three-VPS integration and the bounded real LOB sample. Overall full-market/lifecycle acceptance remains PARTIAL.**

## Installed routes and policy

| VPS | P SOCKS port | Q SOCKS port | Logical lane assignments, 127 cohorts |
|---|---:|---:|---:|
| New York `208.92.225.172` | 18463 | 18464 | 170 |
| Los Angeles `179.254.94.224` | 18090 | 18091 | 169 |
| Ireland `23.230.139.132` | 18465 | 18466 | 169 |

For cohort index i, P lanes use hosts `(i, i+1) % 3`; Q lanes use `(i+2, i) % 3`. This makes each plant's two routes terminate at different VPS hosts, gives P/Q different primary hosts, and rotates the fourth lane's shared host across cohorts. These are route assignments, not proof that all desired markets are simultaneously qualified on four lanes. The collector P/Q processes still share SUN.

Four new dedicated SSH SOCKS services are enabled: `market-data-lob-v2-vps@ny-p`, `@ny-q`, `@ie-p`, `@ie-q`. Official Market Channel TLS remains between the lane and Polymarket through the SOCKS transport. No alternative relay event schema was introduced. A dedicated forwarding key allows only official Market WS/CLOB/Gamma port 443; existing keys and services were retained. Existing NY management-key login on port 443 failed, so the already configured management credential on port 22 was used to append the scoped forwarding key. No credentials are included in this receipt.

Existing P/Q instance files and persistent future-cohort settings were updated. Discovery runs controller commit `c3450da86d1e`, which selects routes when creating new cohorts. Production plants continue using their existing application release and receive the new route configuration. `rabisu_route_guard.py` rejects the main proxy, implicit environment/direct fallbacks, external NO_PROXY bypasses, mislabeled VPS/port pairs, and production instances that disagree with the three-VPS cohort policy.

## Real sample

The existing live acceptance entrypoint ran for **120 seconds**, with **8 markets / 16 tokens**, two independent plants and two lanes each, using explicit production VPS routing. Receipt directory: `canary/`; complete raw run: `lob_v2/runs/three-vps-20261018-canary`.

- At the live cut, every lane had all **16 tokens ready**; P, Q and gateway each had **8 complete markets**.
- Each of the four lanes captured **16 native book snapshots**, plus native price changes and PONG receipts. Three distinct VPS addresses are bound to the per-lane configuration in `canary/per-lane-native-receipts.json`.
- P/Q independent source-book replay: **PASS**, no source-book mismatch, zero parse errors.
- Gateway: **40 Tape records**, **8 SOURCE_RESET**, no incomplete reset, market-sequence gap or route-qualification error.
- Actual consumer: **40 frames**, byte fidelity **PASS**. Query Tape frame readback **IDENTICAL**. Archive readback also preserved original frames.
- Startup/recovery time is included: at-least-one-plant complete-market time coverage was **73.76%** over the test window. The final 8/8 result is not a claim of 100% continuous coverage.
- **17 targeted routing, guard and catalog/dynamic-scope regression tests passed**, receipt `tests.xml`.

## Production rollout and actual checks

All **254 P/Q processes** across **127 cohorts** were reconnected in batches, P before Q. Gateways were not restarted. Discovery was stopped while editing settings and instances under the catalog-owner lock, then restarted with the controller that persists the rotation for newly discovered cohorts. The rollout causes baseline reinitialization; no zero-gap claim is made.

Two actual checks at **15:40:03** and **15:41:16 Asia/Shanghai** both found:

- **254/254 P/Q services running**, no route-config mismatch or unsafe process environment.
- No production P/Q sockets to the previous XUE relay ports 18460/18461 or main proxy 25172.
- Main Mihomo had **0 Polymarket connections**. Its selector/configuration was not changed.
- All three VPS hosts had actual production connections and native-book evidence.

At the second check:

| VPS | Established production sockets | Token observations with native book evidence | Ready token observations |
|---|---:|---:|---:|
| Los Angeles | 25 | 24,471 | 14,518 |
| New York | 34 | 29,121 | 20,532 |
| Ireland | 36 | 30,809 | 27,520 |

Observation counts can include independent lanes/candidate sessions and are **not unique token or market coverage counts**. Exact cuts, process states and per-node stream counters are saved in `production-check-1.json` and `production-check-2.json`.

## Limits

This change connects the two missing VPS hosts and verifies native reception, independent book reconstruction and original-frame delivery on the three-host topology. It does not certify full-market four-lane health, complete lifecycle history, the 50 ms latency target, long-term queue drainage, or permanent host/power-loss recovery. No production VPS was shut down for this routing test. The existing small SUN P-DR/XUE Q-DR experiments remain separate from this full production route rollout.
