# LOB V2 全量生产逐项对账 — 2026-10-18

**结论：全量覆盖验收 FAIL，本阶段尚未完成。** 已完成可复现的目录、分片、当前来源与提交引用对账，并修复三个实际问题。不能据此宣称所有 token 已订阅、所有盘口完整、四路持续可用或全生命周期数据齐全。

本报告使用固定目录和逐条证据，动态监控总数不作为验收输入。当前结果为 7 项对账/证据门禁 PASS，1 项目录身份门禁 FAIL，4 项覆盖门禁 FAIL。没有把失败项降级为通过。

## 固定输入与切点

- 验收 ID：`52c488ac-e600-40fc-bfcf-a80d3dc7356a`。
- 目录版本：`462551`；成员摘要：`404420aa1fa99523a1f2cc541967383dd2c6d0e54ca43b2b880fc69a8f3f70c8`。
- 采集窗口：北京时间 **16:40:57.722930–16:41:57.729892**。
- 期望集合：**279,583 markets / 559,166 个不同 token**。
- 全部期望 token 均展开 P0/P1/Q0/Q1，共 **2,236,664 条 token×lane 行**，包含未建立会话、未发订阅和不合格记录。
- 127 个 cohort、381 个节点；每个节点保留独立的 generation、配置/源码摘要、catalog revision、written/durable/applied/published cut 及凭据。
- 节点切点跨度 **59.454 秒**。先采 G，再采覆盖 G 父记录的 P/Q 切点；这是一组明确的进程本地切点，**不是全局同一时刻，也不是一分钟持续覆盖率**。
- CatalogOwner 发布锁仅用于有界采集，未暂停 WS 接收或 journal 写入；锁持有约 67.6 秒。

固定输入保存在 [原始验收目录](../../runs/coverage-reconciliation-20261018-verified/coverage_run.json)。输入逐个有 SHA-256；报告和六类明细离线重新生成后，**7 个输出文件逐字节一致**，见 [复现凭据](reproducibility-verified.json)。复现期间不读取生产状态。

## 逐项门禁

| 项目 | 结果 | 实际证据 |
|---|---|---|
| 分母逐行保留 | PASS | 2,236,664 / 2,236,664；不因容量或会话缺失删 token |
| 已知 market/token 身份及分配唯一 | PASS | 重复 market owner、token 冲突、活动来源重复状态均为 0 |
| 路由计划与目录一致 | PASS | 127 个 scope/control/ownership 与同一目录版本核对 |
| 所有已知期望 market 已分片分配 | PASS | 279,583 / 279,583；仅证明分配，不证明 WS 写入 |
| 历史分配退出有依据 | PASS | 83,942 个已分配但不再期望的 market 均有排除或终态证据 |
| 节点切点新鲜、同目录、提交凭据有效 | PASS | 381 / 381；无缺失节点或 cut 顺序错误 |
| G 引用属于已捕获的 plant 前缀 | PASS | generation/market sequence/record 引用越界为 0 |
| 发现记录身份闭合 | **FAIL** | 16 条待验证记录、44 条全扫未解析身份；分别保留，不假造 token，也不合并计数 |
| 每个期望 market 至少一个完整来源 | **FAIL** | 65,985 / 279,583 |
| 每个期望 market 在 P/Q 均有完整来源 | **FAIL** | 3,192 / 279,583 |
| 每个期望 market 四路完整 | **FAIL** | 0 / 279,583 |
| 每个期望 market 在 G 合格发布 | **FAIL** | 72,624 / 279,583，涉及 145,248 个 token |

31,214 个期望 market 不在最新全扫结果中，仍保留在期望集合内，未用“扫描没看到”作为退订依据。16 条待验证记录中可见新市场通知已持久化、Gamma 查询暂时返回 404 的案例；仍按待验证处理，不能称为订阅完成。

G 与 P/Q 使用上述不同本地切点，G 数量可能大于后取的 P/Q 当前合格来源并集。不能相减推断丢失事件，也不能把它们描述为同一时刻的端到端覆盖。

## 每个 token 的实际阶段

下表按 token ID 跨四条**活动路由**取并集；每个 token 只计一次。候选会话另外保存，未拿来冒充活动路由。`written` 只表示当前会话 WS 发送成功，不是官方订阅 ACK。

| 阶段 | 不同 token 数 |
|---|---:|
| 目录期望 / 已分片分配 | 559,166 |
| 活动路由存在当前会话状态 | 378,910 |
| 当前会话有订阅写成功证据 | 378,724 |
| 有当前会话原生 book 引用 | 337,490 |
| 原生基线引用与本地持久化 cut 匹配 | 337,490 |
| 在完整同源 market 中取得资格 | 131,970 |

完整 market 要求两个 token 在同一 plant、同一 lane、同一 shard、同一连接代次中同时合格。不能拼接不同来源的两边盘口。

主因互斥归类如下。单位是 **token×lane 行**，同一 token 可以在四路各占一行；不是不同 token 数，也不是丢失事件数。

| 主因 | 行数 |
|---|---:|
| 无当前活动来源 | 1,706,282 |
| 等待健康 PONG | 326,166 |
| 缺少当前会话原生基线引用 | 57,584 |
| 原生盘口仍待基线 | 500 |
| RTT 超过资格门限 | 4,870 |
| 缺少订阅写成功证据 | 186 |
| 同源双 token 未同时就绪 | 6 |
| 合格 | 141,070 |

所有原因（可多选）与互斥主因都在逐 token 明细中。冻结状态中 P 有 46 个、Q 有 25 个进程无 token 会话状态；254 个 plant 均有累计 recovery-budget 等待记录。这说明初始化/恢复问题仍在，**累计计数不足以证明每个缺失 token 此刻都在等待配额**。

## 本次修复及实测

1. **恢复配额的槽位饥饿错误。** 旧实现中 recovery 只能尝试低编号共享槽，initial 占住这些槽时，即使高编号槽空闲也无法恢复。现将模式配额与全局共享槽分开，仍保留 host/plant/provider/worker 和初始化 token 总量限制。针对低编号占用的回归通过；全局调度公平性和全量恢复耗时仍未通过生产验收。
2. **晚到、但仍唯一可对应的 PONG 导致永久失去资格。** 这类响应现在只判旧探针失败，后续连续三次 RTT≤2,000 ms 才恢复资格；存在重叠探针时继续保持歧义，不拿旧 PONG 证明新探针。实际生产固定样本独立校验了 51 次资格决策、11 次晚到响应，并验证 **3 次同连接恢复资格**。见 [原始探针样本与校验结果](probe-acceptance.json)、[可执行独立校验器](verify_probes.py)。这不是全量连接的稳定性证明。
3. **原生替换快照错误沿用旧持久化标记。** 首轮审计在 Q-0087 抓到两条明确反例：新基线 offset 为 81479/81485、已提交 cut 为 81379，状态却仍引用 offset 8 的旧凭据并报告 ready。见 [反例](baseline-evidence-counterexample.json)。修复在新快照进入 working book 时清除旧凭据并等待该 RAW 的精确提交；committed book 保持原 cut。

第 3 项另从 XUE 远端读回并完整校验包含 **94,505 条记录**的 9,044,625 字节归档对象，提取原样保留的 3 条生产帧进行离线重放。新快照提交前旧 committed book 不变、提交后 receipt 精确匹配新 RAW，均通过。见 [样本身份/远端对象摘要](baseline-production-sample.json)、[回放凭据](baseline-replay-receipt.json)、[可执行回放](replay_baseline.py)。只证明该样本和该对象，不推广到全部历史或所有远端归档。

完整回归：**123 passed / 10.15 秒**，见 [JUnit](tests.xml)。本报告的生产覆盖结论来自完整明细，未用回归通过代替生产验收。

## 实际部署和流量

- 本次升级了全部 127 个 G 的目录/精确 committed-source 状态输出；此前 121 个旧 G 缺少完整验收字段。
- 恢复修复滚动处理 254 个 plant：183 个当时无合格发布的进程重启、2 个已经是目标代码、69 个当时仍有合格来源的旧进程保留。见 [部署逐实例记录](recovery-rollout-summary.json)。运行进程仍混有不同源码摘要，不能声称全量同一构建。
- 替换快照修复随后部署到当时无合格发布的 Q-0087。冻结切点中该进程仍无原生输入；因此线上修复覆盖仍是局部，实际修复语义由上面的生产原帧回放证明，不宣称该 canary 已恢复实时覆盖。
- 代码提交：`cfb8891`（逐项验收、恢复配额与探针）；`385ce22`（替换快照持久化证据）；`6191115`（待持久化与错误凭据分开、未解析目录身份阻止通过）。运行源代码摘要逐实例保留在 `node_cuts.json`。
- **16:44:25 路由核查：**254 个 plant 配置/进程环境均符合三台 Rabisu 路由；旧代理 socket 为 0，主 Clash 中 Polymarket 连接为 0。三台 VPS 均有实际连接，见 [路由凭据](routing-final.json)。这些连接数不作为覆盖验收。未修改主 Clash 选择器。

## 其他生产目标的验收边界

| 目标 | 本次结论 |
|---|---|
| 本地提交/发布 p99≤50 ms | **FAIL**：有样本的 P 78 个节点中 68 个超标；Q 102 中 89；G 119 中 95。各进程 p99 范围 P 21.51–1452.16 ms，Q 20.44–2571.09 ms，G 21.55–658.92 ms |
| 工作态与精确 committed cut、G 父引用核对 | 本次固定切点 PASS；不能替代断电/主机故障验收 |
| 全量原文逐路独立重建、深度完整性、Tape 原帧消费 | 本次覆盖审计 **NOT_EVALUATED**；原生基线身份/持久化引用核对不等于全深度回放 |
| 去重净空间节省、压缩/归档/查询队列长期回落 | **NOT_EVALUATED**；未用压缩倍数或物理复用样本代替净节省/排空证明 |
| 全量 REMOTE_ARCHIVED 与本地清理安全 | **NOT_EVALUATED**；本次仅新增一个真实远端对象读回样本 |
| 独立主机故障域、UTC 跨日≥24h连续验收 | **NOT_EVALUATED**；三台 Rabisu 出口不等于 P/Q 独立采集主机 |
| 上游所有原生事件精确并集 / 全生命周期无缺口 | **UNKNOWN**；主流仍为 SOURCE_SELECTED_L2 |

延迟统计来自各进程保留的样本，样本时间窗不同，不能合成“全系统同窗 p99”。队列切点也不能证明长期积压已经排空。完整节点指标、队列及构建差异见 [冻结输入诊断](failure-diagnostics.json)，可用 [诊断脚本](summarize_frozen_runtime.py)重新生成。

LOCAL_DURABLE 只证明本地 journal 对应前缀已同步；不覆盖主机永久丢失时尚未归档的尾部。REMOTE_ARCHIVED 需要对应不可变对象与最后提交的 manifest，并核对连续前缀及洞；FLOW_ACK 不替代这类证据。

## 可直接核查的交付

- [机器报告](report.json)：全部门禁和互斥主因。
- [目录集合差](../../runs/coverage-reconciliation-20261018-verified/catalog_reconciliation.parquet)：每个 market 的期望、分配、最新全扫及退出依据。
- [逐 token×lane 明细](../../runs/coverage-reconciliation-20261018-verified/token_state_at_cut.parquet)：每条当前会话、订阅、原生基线、durable 引用、所有失败原因及主因。
- [候选会话明细](../../runs/coverage-reconciliation-20261018-verified/candidate_observations.parquet)：与活动来源分开。
- [逐 market 资格及 G 提交引用](../../runs/coverage-reconciliation-20261018-verified/market_gap_attribution.parquet)。
- [381 个精确切点](../../runs/coverage-reconciliation-20261018-verified/node_cuts.json)、[未闭合目录记录](../../runs/coverage-reconciliation-20261018-verified/unresolved_catalog.json)。
- [保留输入/输出的文件摘要清单](artifacts.json)。固定大样本留在原验收目录，小样本与可执行校验器随本报告交付。

复现（在本仓库根目录）：

```bash
PYTHONPATH=lob_v2/.vendor312:lob_v2 .venv/bin/python lob_v2/tools/coverage_snapshot.py --reconcile lob_v2/runs/coverage-reconciliation-20261018-verified
PYTHONPATH=lob_v2/.vendor312:lob_v2 .venv/bin/python lob_v2/evidence/coverage_reconciliation_20261018/replay_baseline.py
.venv/bin/python lob_v2/evidence/coverage_reconciliation_20261018/verify_probes.py
```

剩余关闭条件：目录身份闭合；每个期望 market 至少一个完整来源、P/Q 备用和四路资格逐条通过；不重启可用来源的恢复调度达到可接受收敛时间；同一固定时间窗的低延迟与队列回落；之后执行全深度/传输/远端/清理和持续窗口验收。**本次结果不能作为这些条件已经完成的证明。**
