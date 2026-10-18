# 附件 47 项用例对照

测试入口：`PYTHONPATH=lob_v2/.vendor312:lob_v2 .venv/bin/python -m pytest lob_v2/tests -q`。早期小范围 40 个测试；本轮 2.2.9 共 96 个可执行测试实例覆盖多个用例；测试数量不等同 47 项全部真实环境通过。真实试验及固定输入见 `evidence/` 和 `runs/production-20261017/`，局部注入明确标注，物理故障保留未验收。

| ID | 结果范围 | 证据 |
|---|---|---|
| T01 | PASS | 两 lane raw 各存，只有 selected 输出；duplicate_observations 测试 |
| T02 | PASS | 同 lane 相同正文两次保留，raw_ref 不同 |
| T03 | PASS | 同 session 重试十次幂等，窗口外 RESET_REQUIRED；retry_outside 测试 |
| T04 | PASS，单 owner 机制 | 身份在无 await 的 accept 中先占用；正式 fsync 前重试不重复入队 |
| T05 | PASS | 同 ID 不同帧明确 CONFLICT；无成功 ACK |
| T06 | PASS | 相同毫秒/native hash 的重复独立观察均留存 |
| T07 | PASS | shadow 旧 20 不覆盖 selected 30 |
| T08 | PASS | RESET 替换完整深度，continuity_unknown 保留 |
| T09 | PASS | plant lane 变更 RESET 传播 gateway；gateway_propagates 测试 |
| T10 | PASS | 半 market baseline 不可选；gateway 同时拒绝残缺 RESET |
| T11 | PASS | 绝对 size 10→15→0 |
| T12 | PASS | 组内同档两次变更按原数组顺序 |
| T13 | PASS | 组内第二项非法，第一项也不公开 |
| T14 | PASS | 首 book 前 delta 留存，不补应用到新基线 |
| T15 | PASS | 非 JSON、未知事件、未知字段原文均可恢复 |
| T16 | PASS | Decimal 键 0.50=0.5，raw 写法独立保留 |
| T17 | PASS，当前支持边界 | 高精度数量测试、真实 256 位 ID；超数值/深度预算显式失败 |
| T18 | PASS | 旧 connection 数据保留但不修改新 book |
| T19 | PASS | 实际 Tape 两 market 过滤测试；offset 跳号不等于 market_seq 缺口 |
| T20 | PASS | committed@c 冻结并注册 c+1；随后十组完整交付 |
| T21 | PASS，注入 | 半包、队列溢出取消 bootstrap；END/READY 不发送 |
| T22 | PASS | bootstrap 具有独立 snapshot_id，不占 canonical 事件 ID |
| T23 | PASS，注入 | 慢消费者队列满 resync，其他组继续持久/提交 |
| T24 | PASS | raw/emit 共用日志，sync 前 plant 订阅队列为空 |
| T25 | PASS | gateway sync 前 committed book 不推进 |
| T26 | PASS，进程故障 | 实际 SIGKILL/恢复、消费者 receipts、归档同 ID 原帧；不代表断电 |
| T27 | PASS | 实际 gateway 重启两个 UUID；旧 session/错误 stream generation 拒绝 |
| T28 | PASS | 半写尾部报告截断；CRC/归档块损坏拒绝 |
| T29 | PASS，目录后端注入 | data 已写、manifest 失败时 local 保留；重试幂等 |
| T30 | PASS，S3 API 模拟 | PUT 成功但响应超时，读回相同内容判成功；真实 S3 未验收 |
| T31 | PASS，S3 API 模拟/目录 | 条件写冲突比对内容；不同内容失败；真实 S3 未验收 |
| T32 | PASS，目录后端 | 不依赖本地 catalog，列举 commit 后独立恢复 |
| T33 | PASS | query commit 独立，Parquet 原帧回读；raw 成功不冒充 query 完成 |
| T34 | PARTIAL | 离线对象后端/低 spool 预算注入通过；未执行两小时 S3 断网 |
| T35 | NOT_EVALUATED | 同机目录恢复通过，未测试永久丢失真实 plant 主机/盘 |
| T36 | NOT_CLAIMED | SOURCE_SELECTED_L2 能力边界；内部守恒不推导上游无遗漏 |
| T37 | PARTIAL，真实全深度诊断 | 本轮 24 market / 48 token，46 稳定前后夹取的全深度一致、2 无 committed baseline；无共同 cursor，不做 timestamp 拼接 |
| T38 | PASS，注入 | 墙钟回拨后源序号/单调耗时保持；跨机时钟质量仍未验证 |
| T39 | PASS，格式演进 fixture | protobuf 字段重排/未知字段保持原编码帧；实际消费者与 Tape 同字节 |
| T40 | PASS，产品边界 | 晚归档不改旧 Tape，重新建会话/进程不倒填历史发布时间 |
| T41 | PASS | working 比 durable 多十组时 bootstrap 仍精确读取 committed cut |
| T42 | PASS，有限压力 | 单 market 多组单 sync；固定真实样本超过 50 条/s，无逐组 sync 串行限速；50 ms 延迟目标未达 |
| T43 | PASS | 提交 1–9、11–12 后连续水位=9，洞=10 |
| T44 | PASS，注入 | 跨 segment 大记录、不可压缩超限、非法长度、队列小于单帧、book 预算均 fail closed |
| T45 | PARTIAL | 新 header/目录先同步的调用顺序与进程恢复通过；OS 崩溃/断电未验收 |
| T46 | PASS | 新 session 必走 CURRENT_SNAPSHOT，新 RESET 后恢复；旧 callback 无效 |
| T47 | PASS，限定范围 | cut+1 有界缓冲、overflow 取消、跨 UUID RESET_REQUIRED fixture；实际多代原帧回放 |

额外故障：EIO、ENOSPC、fdatasync 失败，均不得推进 committed book。尚未执行真实远端 Range GET/并发 writer 资格测试、设备断电、打开文件句柄延迟释放的 OS 级测试；清理当前仅对停止并释放依赖的隔离副本执行。

广域部署新增回归：plant 重启后的空 CURRENT_SNAPSHOT 不保留旧源序号；持续到达的多块初始快照不因延迟 PONG 被丢弃；取消连接任务不遗留未来启动配额；大对象 SSH stdin 在超时轮询中继续完整传输。生产已启用真实 XUE 归档及 writer 私有缓存回收；设备断电、跨进程文件句柄和全部远端故障矩阵仍未完成，不以当前测试数量替代附件用例映射。

2.0.1 新增：无健康 PONG 的已有基线不能取得发布资格；健康主 lane 接管必须完整 RESET；归档策略版本从原记录读取，不能用当前软件版本改写旧 Tape 的身份。NVMe/HDD tier 回归包含条件冲突、冷端 manifest 失败保留热端和活跃读取租约阻止热端删除。真实 SSH 远端恢复与消费者原帧核验见生产验收报告，真实 S3 和设备断电仍未验收。

2.0.2/2.0.3 新增：延迟 PONG 时保持 PING 周期，按最早未确认 PING 计算 RTT；已有原生帧持续到达但排队超限时显式重建连接、撤销旧 book 资格，保留全部已经捕获的 RAW。真实 BACKLOG_RESYNC、连接代次与完整 RESET 记录见 `live-backlog-resync-evidence.json`。


## 2026-10-18 本轮结果与 guide §18 对照

当前报告 `production_acceptance_20261018.md` 优先于上方历史部署说明。96 项回归通过不构成 47 项全部真实物理故障通过。`live-faults-730b27` 是 2.2.8 的 360 秒真实官方 WS 测试：先资格验证，再关闭主 lane、SIGKILL plant、SIGKILL gateway；121 条 Tape、113 帧实际消费者原文一致。T26/T27/T46/T47 在该限定范围有新实证；T35、T45 的真实宿主/断电部分仍未验收。

| guide §18 用例 | 本轮证据 | 判定范围 |
|---|---|---|
| 协议存活但 DATA 应用落后 | `test_pong_liveness_cannot_hide_local_queue_and_quiet_token_is_valid`、生产 ingress/commit 队列指标 | fixture PASS；全量时效 FAIL |
| 连续旧行情刷新 last_inbound | 同上、`test_text_probe_ambiguity_cannot_be_reused_by_late_pong` | fixture PASS；不以 any-frame 取得资格 |
| 安静 token 无变化 | 当前会话原生基线+健康 PONG、quiet checkpoint 两项测试 | fixture PASS；真实派生 checkpoint 已归档，不伪造更新时间 |
| 同 socket 部分 token 无 book | `test_partial_candidate_never_retires_socket_still_serving_other_market`、真实 2/48 无基线 | fixture PASS；实际缺口保留 |
| 动态目录超过旧计数 | `test_incremental_catalog_fills_tail_and_recovers_scope_index_crash`、cohort 0123 持续增长 | fixture + 真实新增 PASS；全覆盖另判 |
| 全扫失败/cursor 循环 | scan absence/partial、resume committed pages 测试、持久 scan cache | fixture PASS；失败不推进成功水位 |
| resolved 后迟到 new_market/重启 | `test_tombstone_survives_restart_and_stale_full_sweep` | fixture PASS |
| 临时发现被正式目录接管 | dynamic identity、tail recovery、stable ownership | fixture PASS；期望缺配为 0；同版全覆盖 FAIL |
| 同价多次、删除、空簿 | semantic absolute size/group order + every-cut replay | PASS，固定真实输入与 fixture |
| 组中非法字段 | `test_invalid_middle_group_never_exposes_partial_state` | fixture PASS |
| 旧 RESET 结果晚到 | generation/cut fence、retired pending baseline 测试 | fixture PASS；旧回调不得重新安装 |
| working B / sync A | `test_working_epoch_B_cannot_leak_before_committed_epoch_A` | fixture PASS |
| write 后 sync 前崩溃 | journal failure/kill fixtures；实际进程 SIGKILL | 进程/注入范围 PASS；断电 NOT_RUN |
| sync 后 apply 前崩溃 | journal durable-prefix replay、actual Tape replay、working/committed fixture | 组件边界 PASS；独立 OS/设备崩溃 NOT_RUN |
| 短写/磁盘满/sync/轮转失败 | short-write、ENOSPC/EIO、rotation failure tests | 注入 PASS；不推进错误前缀 |
| 重复晚到/旧代同步回调 | complete_durable old-generation guard tests | fixture PASS |
| 部分来源集体失格 | recovery budget/cancel tests、worker cap、未建立连接超时回收测试；P/Q 实际竞争记录 | fixture PASS；全量恢复压力仍 FAIL |
| 主 lane 停止 | `live-faults-730b27/lane-backup-before-fault.json`、`lane-after-fault.json` | 8 market 真实 PASS |
| plant/宿主故障 | 同次 plant SIGKILL；XUE Q 当前不合格 | 进程 PASS；宿主 NOT_RUN |
| object 成功但 manifest 失败 | archive/projection/cold-mirror failure fixtures | 注入 PASS；原件保留 |
| payload 复用后逐路重建/删除 | observations actual 117462 occurrences、完整帧 SHA 重建；capture deletion 未授权 | 重建 PASS；canonical 原件替换及净存储释放 NOT_RUN |

本轮生产私有 sealed journal 清理确实在运行，且必须通过远端对象/manifest/恢复校验、callback cut 和 lifecycle outbox 依赖检查。`test_archive_process_readback_and_dependency_guard` 验证在独立后台进程中缺依赖证据不删除，补齐后读回原帧一致。真实多消费者跨进程打开文件句柄、设备断电、两小时远端离线仍未完成，不能据此将 T34/T35/T45 升为完整 PASS。
