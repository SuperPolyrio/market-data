# V2 remediation — current handoff

Status: **PARTIAL**, never promote from unit tests or service uptime. The current actual report is [production_acceptance_20261018.md](production_acceptance_20261018.md); case mapping is [semantic_cases.md](semantic_cases.md).

Implemented in the existing V2: persistent CatalogOwner/full+incremental lifecycle reconciliation; current-session token evidence; bounded independent candidate sessions and complete-market handoff; exact persistent ladders, immutable groups, four journal cuts, fixed RESET input and atomic bootstrap; physical payload reuse with every occurrence preserved; immutable production Tape/observation query commits; live checkpoint archive-prefix gating; archive subprocess isolation and bounded query workers; persistent full-fleet observation.

Latest application: 2.2.9 (`9d31df3793b9`), SSH path follow-up `c549fa8dd7b3`. G-0038/G-0105 use 2.2.8 (`730b27eaefba`), P-0021/Q-0069 use 2.2.9. Other fleet workers are mixed older versions; changing the service default does not update running processes. Production archives remain enabled and the legacy separate LOB collector remains paused.

Evidence root: `runs/remediation-20261017`. 96 regression tests pass. Latest 2.2.9 live 180-second sample has 8/8 complete markets and 30 identical consumer frames; actual 2.2.8 360-second fault run has independent source-book replay, lane switch, plant and gateway restart, 121 Tape records and 113 byte-identical consumer frames. Fixed production 18,217-record every-cut replay is identical. Full-production depth sample has 46/48 exact stable matches and 2 missing baselines.

Open mandatory gates: global same-revision full coverage, all standby lanes, 50 ms p99, fair connection admission under mixed old/new fleet, complete query backlog and cold-copy drain, net storage saving after payload reference custody transfer, independent-host qualified Q, and fixed-build full-scope 24h across UTC midnight. Full host/power loss has not been tested. Upstream native-event completeness remains UNKNOWN.

Do not modify the user's main Mihomo selector. SUN 25172 is the existing user-managed main egress (earlier profile12 label was wrong). XUE 28172 is the separate authorized profile12 canary; its 443/8443 exits currently fail official endpoint probes. A usable independent exit has been requested, while local work continues.

Runtime services: `market-data-lob-v2@*`, discovery, checkpoints, soak on SUN; 16 query partitions and cold mirror on XUE. Query repartition recovery preserved all 2,560 prior completed inputs; 3 duplicate receipts were retained in an audit directory, no remote originals deleted. Inspect current evidence and runtime before changing deployment or making readiness claims.
