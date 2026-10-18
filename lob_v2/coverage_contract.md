# Coverage contract v2.0.0

资格分母是冻结 scope 的所有 market/token，起点为验收启动的单调时钟；不使用首次 book 时间缩短分母，不在失败时移除 ID。binary identity 必须有两个不同 token、outcome 映射、Gamma ID、condition ID 和版本。按 market 成组放置，连接容量是试验参数，不是官方上限。

每个 `(plant, lane, token, connection)` 分别记录 assigned、written、book_seen、baseline_durable 和 ready。WRITTEN 只来自当前连接实际 WS 写成功；PONG 不增加覆盖。READY 需要当前会话有效原生 WS book、基线已本地同步，且未遇到已知解析、断连或盘口资格故障。合法空 book 可以 READY。REST 与派生快照不增加 WS book 数。

每条 raw 具有唯一入口身份、接收 UTC/monotonic、连接代次、归档 offset 和 disposition。原始无效内容、未选来源、初始化前增量以及旧连接观察都保留。进程崩溃留下的合法 raw 若尚无 disposition，必须记为 NORMALIZATION_PENDING_CRASH_RECOVERED，不能排除分母。

`E` 为应订阅 token；分别导出 `E-A`、`A-W`、`W-B`、`E-R`、`B-U` 及各阶段完整集合。总数相等不构成集合等价。状态切点使用各进程明确的本地 cut，报告不将依次抓取状态文件称为跨进程原子切点。

单路可用是 P 或 Q 至少一路 lane READY。双 plant 可用要求 P、Q 各自至少一个 READY lane；两个 lane 不充当两个独立 plant。完整 market 必须在同一 plant 的同一 lane 上拥有全部 token，不能把 P 的 token0 与 Q 的 token1 拼成完整来源。

时间覆盖用状态转换的区间长度求和，并以整个固定窗口的应采集 token 秒数为分母。静默已初始化 token 保持可用，不能凭无增量判漏采。断连、处理失败和外部故障注入关闭对应区间；SIGKILL 的关闭边界来自独立控制器，不能等待死进程写自己的断线记录。所有节点本次同机，使用同机 monotonic；跨机运行须另验墙钟误差。

结构完整性分层：原生快照完整替换、完整数组原子应用、无已知本地序号缺口、raw 独立重建等价、消息自报 BBO 一致、Tape 与实际消费者原帧一致。上述证据都不证明上游不存在共同遗漏。REST 同时采样只能诊断状态，不能凭毫秒时间戳拼接无缝历史。

CaptureLog、MarketTape、query 三类完成范围独立。remote progress 只推进连续已提交前缀，后续已提交范围与洞单列。目录对象恢复通过与真实远端恢复通过必须使用不同状态。LOCAL_DURABLE 不提供主机永久损失后的未归档尾部保证。

## Production reconciliation (2026-10-18)

`tools/coverage_snapshot.py` takes the existing CatalogOwner publication lock for a bounded capture. It preserves the committed catalog/head, ownership scopes, controls and instance configurations, then captures fresh gateway cuts followed by plant cuts at the same catalog revision. Reception and journal writers continue. Every process cut and the full observed time span are reported. This is an explicit vector of local cuts, not a globally atomic snapshot or time-window continuity proof.

All desired tokens generate four rows even when routing or reception is missing. Active routes and candidate observations are separate; candidate states cannot overwrite an active state. Qualification verifies current connection identity, subscription write evidence, native baseline reference and its matching durable generation/sequence. Market qualification requires both assets on one source. Gateway evidence comes from committed metadata, never the working selection, and its parent plant generation/sequence/record must lie inside the captured plant prefix. Missing evidence fails the relevant gate.

`coverage_run.json` identifies SHA-256 protected compressed inputs. `catalog_reconciliation.parquet`, `token_state_at_cut.parquet`, `candidate_observations.parquet`, `market_gap_attribution.parquet`, `unresolved_catalog.json` and `node_cuts.json` are independent queryable outputs. `--reconcile RUN_DIRECTORY` regenerates results from those fixed inputs without contacting production. Capture/lineage consistency, service coverage, time coverage, remote readback and upstream native completeness remain separate results. A passing catalog accounting gate never upgrades a failed service gate.
