# LOB V2 整改实施与实际验收 — 2026-10-18

报告生成：2026-10-18T12:40:49+08:00。总体 **PARTIAL**：已实现和实测不等于全量验收完成。最新应用版本 2.2.9；采集修复 commit `9d31df3793b9`，SSH 连接路径补丁 `c549fa8dd7b3`。较大 gateway 实测版本 2.2.8 / `730b27eaefba`，不同版本结果明确分列。主流合同始终为 `SOURCE_SELECTED_L2`。

覆盖审计观测窗口：2026-10-18T12:13:00+08:00 至 2026-10-18T12:14:46+08:00；冻结目录版本 379415。其他样本各自的时间、版本和范围保存在对应原始 JSON 中。

## 七项实施与未通过项

| 指导项 | 已落地的实现 | 实际证据及结论 |
|---|---|---|
| 市场、token、目录对账 §9–10 | SQLite WAL/FULL 单一 CatalogOwner；全扫分页持久续跑；增量发现及生命周期 outbox；失败不推进成功水位；持久终态、身份冲突隔离；稳定 market 对路由；填充尾分片；同版本集合差 | 固定审计 desired **324,184 market / 648,368 token**，desired 未分配 **0**。历史分配 351,831，全扫 census 289,611。采样中目录从 379415 推进，372 个实例均未与冻结版本匹配；严格同版本全覆盖 **FAIL**。不能把历史分配数当当前活跃数。 |
| 四路接收、恢复积压 §8 | 有界 FIFO reader、入口身份与顺序；文本 PONG 单探针/迟到歧义；网络、本地队列和 loop/I/O 分因；独立候选；按完整 market 持久移交；全局/plant/provider/worker 预算；错峰、退避；未建立 socket 的超时也取消任务并释放预算 | 两个全范围实例 P-0021/Q-0069 保留各自 4,000 market 范围，改用每连接 500 token。2.2.9 实测分别恢复到 **562 / 353 个合格 market**，仍远非全覆盖。主机共享连接额度竞争仍严重，不能称恢复积压清零。 |
| 盘口完整性和备用资格 §10–11 | 当前会话 assigned、订阅写成功、native book、Capture durable receipt、应用及 PONG 证据；全部原因与主因；同来源两 token 同时合格；新增 SOURCE_ASSIGNED 使未发送订阅的空状态也可审计 | 独立源内重建和原帧回放通过。生产随机固定种子抽取 **24 market / 48 token**：**46 全深度 EXACT_STABLE_BRACKET_MATCH，2 无合格基线**。本次全量审计 P/Q lane 1 只有 **132 / 216 个 ready token observations**，备用能力 **FAIL**。 |
| 选源与物理去重 §16 | plant 选 lane、gateway 选 plant；切换完整 RESET；Tape 与观察历史分离；有界 exact-byte payload 字典 + 每次 occurrence；逐来源原帧重建校验 | 实际生产观察投影保留 117,462 occurrence，其中 41,695 RAW；RAW payload 33,138,874 B → 33,046,070 B，复用 **92,804 B（约 0.28%）**；删除独立观察 **0**。原始远端 Capture 对象仍保留，**未取得整个存储库净去重节省**，查询投影不授权删除它们。 |
| LOB 写入和提交 §12–14 | 常驻精确 Decimal 价位；完整变化组一次准备；编码一次；working/committed 分离；固定 RESET 输入；有界专用 journal I/O、四个连续水位；组间让出；快照并发/入队限额；重启恢复扫描不再常驻上一段解码 Tape | **18,217 条真实 Tape、2,640 market 的每个 cut 与原帧链一致**。该输入盘口 prepare/apply 区间耗时加速 **8.96×**，这是局部墙钟测量，不含网络/I/O。较大 gateway p99 **111.59 / 198.91 ms**；新 plant **108.17 / 182.79 ms**。50 ms 合同 **FAIL**。 |
| 压缩、归档、清理 §15、§17.4 | 长度/CRC journal；独立 Zstd 帧及 footer；数据对象先写、条件 manifest 后提交；完整远端读回；连续前缀与洞；日志私有 owner/生命周期依赖证据；压缩与校验移到受全局预算约束的独立进程；查询 16 个受 CPU/内存限额的 worker | 本地累计 75,689 个归档 receipts，原 journal / 压缩对象 **9.25×**。封口待归档 **6.32 GiB**，开放尾部 **17.33 GiB**，最老封口 **77.0 秒**，已提交范围前的洞 **0**。查询队列短窗口回落，但仍有约 7 万段；查询转冷盘队列继续增长，积压整体 **未解决**。 |
| 查询、恢复、完整验收 §11.3、§15、§17–18 | Production Tape Parquet 独立 immutable commit；真实输入 checkpoint；整点 CURRENT_SNAPSHOT 派生 checkpoint 等精确 Tape prefix 归档后才提交；持久长稳观察；异机 Q/G 验收实例 | 生产 18,217 Tape 查询原帧一致；另 6,055 帧在 XUE 用 PyArrow 16.1.0 生成、SUN 用 25.0.1 读回，与远端 canonical Tape 完全一致。已提交 3 个 live checkpoint（保留真实缺基线数量，0 伪造 native events）。XUE 独立 profile 12 两出口失败，Q 未合格，整机故障 **NOT_RUN**。固定候选全量 24 小时跨 UTC 日界 **NOT_COMPLETE**。 |

## 实测的范围与版本

- `live-2.2.9/`：最新 reader 真实运行 180 秒，8 market / 16 token 最终全部合格，P/Q 独立回放通过；30 条 Tape、30 帧实际消费者原文一致，无序号、路由资格或完整 RESET 错误。该次没有注入故障。
- 同一次新样本跨来源 payload 复用：812 个 occurrence（198 个 RAW），RAW 正文 232,620 B → 98,139 B，复用 134,481 B（57.81%），独立 occurrence 删除 0，逐来源原帧重建完全一致。其重复分布与历史生产样本不同；这是正文层复用，不是包含旧归档及查询投影后的净空间节省。

- `live-faults-730b27/`：2.2.8，8 market / 16 token，两个 plant、每 plant 两 lane，360 秒，同一 SUN 主机与现有用户出口。先验证备用，再关闭主 lane；随后强杀并重启一个 plant、强杀并重启 gateway。121 条 Tape、72 个 RESET、2 个 Tape UUID；无 market sequence gap、无残缺 RESET、无 route qualification error。最终 8 market 均合格。实际消费者收到 113 帧，与 Tape 原帧逐字节一致。**该范围 PASS，不包含宿主机断电。**
- `live-faults-2.2.7/`：前一代码版本同样真实执行三种故障；96 条 Tape / 88 帧消费者原文一致。保留为历史证据。
- `live-faults-5c1784/`：P/Q 独立回放通过，但该次 lane 故障前提未满足，因此 lane 注入明确 NOT_RUN；不能与后续成功试验混为同一次。
- `live-faults-48b44c/`：发现参考模型缺少尚未发出订阅的候选空簿，原始结果保留 FAIL。之后增加 SOURCE_ASSIGNED 与独立回放，不改写旧验收结果。
- `live-faults-latest/`：旧测试 relay 停止顺序错误导致中止，保留失败日志；修复并重跑，不计通过。
- `candidate-2.2.9-tests.xml`：96 项回归通过。最后 SSH 控制路径后缀修复另有专项通过。测试数量不等于 47 项真实故障全部通过。

## 2×1 与 2×2

同一固定 8 market / 16 token，分别真实运行 240 秒，两个时间窗口并非同步对照；都包含启动等待。

| 指标 | 2×1 | 2×2 |
|---|---:|---:|
| 最终完整 market | 8/8 | 8/8 |
| 至少一个 plant 完整 market-seconds | 86.0316% | 86.1896% |
| 两 plant 同时完整 market-seconds | 70.7286% | 70.7326% |
| 原文 RAW 字节 | 300,849 | 400,044 |
| 压缩归档字节 | 138,660 | 150,206 |
| journal / archive 压缩倍率 | 10.9828× | 11.2281× |
| 消费者原帧一致 | 73 帧 | 65 帧 |

2×2 在这两个安静短窗口的覆盖增益很小，RAW 成本约 +33.0%，归档字节约 +8.3%。这不能证明全量成本或可靠性收益；真实备用切换另由资格在先的故障试验验证。故障恢复产生重复快照会改变压缩倍率，不能把它当独立观察去重率。

## 延迟与队列

固定同一生产输入、1×原到达速率、同一 `/data` 文件系统，后台归档保持运行：

| group fsync 窗口 | baseline 20 ms | candidate 2 ms | candidate 5 ms | candidate 10 ms | candidate 20 ms |
|---|---:|---:|---:|---:|---:|
| 5,541 条规范记录总提交 p99，ms | 95.12 | 64.31 | 83.34 | 58.17 | 41.65 |

该候选比较版本为 2.2.5，单流回放，不替代生产全量 SLO。窗口并非越短越好，继续保留实际 20 ms 配置。生产采样另记录未完成数、年龄、同记录阶段 trace、各事件类型直方图；不平均 p99，不剔除恢复/RESET 延迟。

远端库存独立对账：74107 段→75004 段时，查询完成 2430→3585，待处理 71677→71419；后续库存 75644、查询完成 5096，待处理 70548。与此同时，查询冷盘待搬运 manifest 从 3723→5862，说明新的瓶颈转向冷盘队列。冷搬运批次已由 2 提到 8，并在待处理充足时继续工作；尚需继续实测证明总体回落。

查询从 8 分区调至 16 分区期间出现提前启动 worker 的操作错误，已停止并重新对账：原有 2560 个已完成输入全部保留，遗漏 0，3 个重复任务凭据移入审计目录，远端对象未删除。证据 `query-partition-migration-16.json`。

## 耐久与故障边界

- **LOCAL_DURABLE**：只有包含该记录的 fdatasync 完成并按序应用 committed book 后，才允许正式发布。原帧发送复用。已测进程 SIGKILL、半写/CRC、短写/EIO/ENOSPC/sync 故障注入。单机永久损失时，尚未远端提交的本地 durable 前缀可能丢失；设备断电、OS 崩溃不以进程故障代替。
- **REMOTE_ARCHIVED**：独立 XUE 已存在并完整读回验证的对象 + 最后提交的 manifest，才能形成证据；进度按连续前缀计算，洞单列。已验证跨主机读回、解压、原帧重建及私有本地缓存回收。SUN 损失时可恢复 XUE 上已提交前缀，不能保证未归档尾部。
- XUE NVMe/HDD 同宿主机，不是两个独立宿主故障域；若 XUE 永久损失，不能从“双盘”推出已有远端副本一定存活。
- 主生产 P/Q 仍共用 SUN。XUE DR 样本的 Q 未取得本会话合格基线，因此不注入 SUN 故障，不宣称异机持续采集已通过。
- SUN `25172` 实际为用户管理的主 Mihomo 出口；早期材料将其标为 profile 12 的标签错误已记录更正。未修改主选择器。XUE `28172` 才是独立 profile 12 实例，两节点探测均失败。
- `FLOW_ACK` 仅为当前会话流控；无持久输入接收/删除授权。跨 Tape generation 返回 RESET_REQUIRED；旧 Tape 只重放原帧。

## 原方案 A–G 交付位置

| 阶段 | 可执行实现/样本 | 验收结论 |
|---|---|---|
| A 协议与录制 | `lobv2/reader.py`, `catalog*.py`, `journal.py`, `tests/fixtures/native.jsonl` | 固定样本与真实 RAW 顺序/保真通过；全目录同版本覆盖未通过 |
| B book/schema | `book.py`, `core.py`, `wire.py`, `envelope.proto` | 每 committed cut 对照与原帧 roundtrip 通过 |
| C 双 lane plant | `reader.py`, `recovery.py`, `core.py` | 小范围已资格验证的 lane 故障通过；全量备用 FAIL |
| D 两 plant/gateway | `node.py`, `core.py`, `validation.py` | 小范围 plant/gateway 重启、bootstrap、原帧通过 |
| E 归档/回放 | `archive.py`, `archive_worker.py`, `ssh_store.py`, `tools/archive_*.py` | 远端样本恢复与清理证据通过；长期队列/宿主故障未闭环 |
| F 查询/质量 | `projections.py`, `tools/project_archive.py`, `tools/checkpoint_live.py` | 生产输入查询及 checkpoint 样本通过；历史查询回填仍积压 |
| G 压力/长稳 | `tools/benchmark_durable_replay.py`, `coverage_snapshot.py`, `storage_snapshot.py`, `soak_watch.py` | 实测已交付；50 ms、全覆盖、24h、完整 DR 未通过，总体 PARTIAL |

## 部署和未完成工作

本轮不是全 fleet 已升级声明。新 reader 为 P-0021/Q-0069；较大新 gateway 为 G-0038/G-0105；P/Q-0038 仍为较早的 2.2.0，尾分片 P/Q/G-0123 为 2.2.1，其余大量工作进程仍是早期 V2。服务默认下一次启动入口已更新到 2.2.9，但这不会把存量运行进程自动变成新代码。旧的另一套 LOB 采集继续暂停。

`market-data-lob-v2-soak.service` 已实际采样所有 372 个配置节点。版本/配置指纹变更或采样中断会重置连续观察窗口；旧 summary 不带 provenance 时明确 UNKNOWN。**该服务目前只证明持续留存观测，不证明固定候选全量 24h 验收已开始合格计时。** 当前固定版本、全范围、备用和延迟前置门槛仍未满足。

剩余必要工作：完成受控全量 rollout 与同 catalog revision 审计；解决共享连接额度竞争及可用独立出口；使各 lane 的同 market 两 token 长期合格；将真实全量总提交 p99 降至 50 ms；证明查询、转冷盘两层队列持续回落；完成原始 payload 引用转移后的净空间验收；完成全量固定版本跨 UTC 日界至少 24h 及真实宿主故障域验收。用户无需再提供设计方案，独立出口当前仍缺运行条件。

完整证据目录：`/home/jiahuaiyu/develop/polymarket/repos/market-data/lob_v2/runs/remediation-20261017`。固定样本、配置、构建信息、失败记录和原始测试结果均保留；索引见 `evidence/remediation_20261018_index.json`。适用语义用例见 `semantic_cases.md`。

最终远端队列复核：canonical 76,488 段、已完成查询输入 5,928 段、尚待查询 70,560 段；查询转冷盘 pending 6,461 个 manifest。16 个查询 worker 全部 active，但 active 不代表完成；最终库存见 `remote-queue-census-final.json`。
