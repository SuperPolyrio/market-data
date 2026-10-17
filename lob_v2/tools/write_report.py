#!/usr/bin/env python3
"""Render the bounded acceptance report from retained measurement receipts."""
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
rows=json.loads((ROOT/"evidence/measurements.json").read_bytes())
by_name={r["name"]:r for r in rows}
current=by_name["batched-2x2"]
one,two=by_name["batched-2x1"],by_name["batched-2x2"]
fault=by_name["batched-faults"]
delay=fault["plant_kill_to_reset_creation_ms"]
benchmark=json.loads((ROOT/"evidence/benchmark/benchmark.json").read_bytes())

text=f"""# V2 小范围开发与实采验收

日期：2026-10-17（Asia/Shanghai）。最终核心版本：2.0.0。完整协议、实现边界和运行命令见 [README](README.md)；逐项故障范围见 [47 项对照](semantic_cases.md)。

**结论：固定 12 market / 24 token 的多链路采集、独立建簿、单一发布、持久 Tape、实际消费者、目录对象恢复和查询链路通过小范围验收。不是 A–G 全部生产验收通过。** 100/250-token 订阅批次在切点到达完整 gateway 范围；500-token 初次批次曾仅发布 144/500 token，后改为有界批次 FLOW_ACK 并重新实测，最终范围见下表。50 ms p99、真实远端存储、全量持续发现/迁移、压力长稳未通过或未实施。

## 最终版本的真实试采

运行：`batched-2x2`，开始 {current['started_at']}，测量窗口 {current['window_seconds']:.2f} 秒，P/Q 各两 lane，单 TLS gateway、真实 TLS 消费者。各进程同机，既有 HTTPS 代理出口；未修改路由选择器。官方 WS 是实际行情入口。

| 检查 | 实测 |
|---|---|
| 资格分母 | 12 markets、24 个不同 token、明确 outcome/版本，完整成组分片 |
| 当前连接实际订阅写出 | P0/P1/Q0/Q1 各 24/24 |
| 当前会话原生 WS 基线已持久且 READY | P0/P1/Q0/Q1 各 24/24 |
| gateway 已提交完整市场 | 12/12 markets、24/24 tokens |
| 独立 Decimal 参考重建 | 四份来源均与运行时 book 完全一致 |
| native BBO 检查 | {current['native_bbo_comparisons']} 次比较，0 次不一致；不当作上游无遗漏证明 |
| raw 身份/处置 | 无重复入口 ID、无缺失 disposition、无 normalization pending |
| Tape 连续性/来源资格 | 全代市场序号缺口=0，未 RESET 的 route 变化=0 |
| 实际消费者 → Tape | {current['consumer_frames']} 条原帧逐字节相同 |
| Parquet → Tape | {current['tape_records_all_generations']} 条，4 个固定 bucket，范围与原帧回读相同 |
| 顺序日志 → 独立 Zstd 块 | {current['journal_bytes']:,} → {current['archive_bytes']:,} bytes，{current['compression_ratio']:.2f}× |
| 删除后恢复 | 已验证对应 manifest 和摘要、释放依赖后删除隔离副本，再独立恢复相同 raw/Tape 帧 |

全窗口从资格启动计算，未剔除初始化等待：单 plant 可用 token 时间覆盖 {current['one_plant_coverage']:.4%}，双 plant {current['both_plants_coverage']:.4%}，均不是 100%。切点 READY 不替代整个窗口持续覆盖。逐 token ID、连接、基线 raw_ref、失败集合、市场成组状态、raw 到 manifest 引用见 `evidence/batched-2x2/*.parquet`、`coverage.json` 和 `archive_observation_audit.parquet`。

状态文件的切点来自各进程明确的本地 mono 时间，不是假设的跨进程原子快照。关闭进程之后的 RECOVERING 状态不反写到关闭前切点；来源参考回放另以 before_shutdown 的明确 journal offset 比较。

## Token 批量实验

三组为同一 500-token 目录的确定前缀；每个市场两个 token 始终在同一 lane。容量是试验变量，不是官方上限。试验相互有时间重叠，主机负载很高，不能据此计算隔离的硬件容量。

| 每 lane 请求 token | P/Q written | P/Q READY 切点 | gateway READY token | 单 plant token 时间覆盖 | 结论 |
|---|---|---|---|---|---|
"""
for size in (100,250,500):
    r=by_name["batched-capacity-500"] if size==500 else by_name[f"accepted-capacity-{size}"]
    text+=f"| {size} | {size}/{size} | {size}/{size} | {r['gateway_ready_tokens']}/{size} | {r['one_plant_coverage']:.2%} | {'小范围范围齐全，性能未达标' if r['gateway_ready_tokens']==size else '发布范围未收敛，FAIL'} |\n"
text+=f"""
500-token 例子明确证明：目录数、WRITE_OK、有效 book/持久基线、最终发布是不同关卡。旧实现每帧等待 FLOW_ACK，在高负载下逐帧往返拖慢发布，曾缺 356 个 token。最终实现将原帧按最多 128 条、目标 4 MiB 分批发送，批末确认一次；不改变事件边界和持久化门槛。旧失败结果与新的复测均保留，不把旧 FAIL 擦掉。

## 2×1 与 2×2 收益和成本

同一 24-token 目录，各约 55 秒，开始时刻不同且窗口重叠；下面是观察对照，不能把差值归因为独立网络冗余或证明独有原生事件增益。

| 项目 | 2×1 | 2×2 |
|---|---:|---:|
| 接收 raw bytes（明确状态切点） | {one['raw_bytes']:,} | {two['raw_bytes']:,} |
| 全部已封口日志 bytes | {one['journal_bytes']:,} | {two['journal_bytes']:,} |
| 压缩对象 bytes | {one['archive_bytes']:,} | {two['archive_bytes']:,} |
| 压缩比 | {one['compression_ratio']:.2f}× | {two['compression_ratio']:.2f}× |
| 三进程 RSS 峰值之和 | {one['max_rss_sum_bytes']/1024**2:.1f} MiB | {two['max_rss_sum_bytes']/1024**2:.1f} MiB |
| 三进程累计 CPU | {one['cpu_seconds_sum']:.2f} s | {two['cpu_seconds_sum']:.2f} s |
| 单 plant token 时间覆盖 | {one['one_plant_coverage']:.2%} | {two['one_plant_coverage']:.2%} |
| 双 plant token 时间覆盖 | {one['both_plants_coverage']:.2%} | {two['both_plants_coverage']:.2%} |

2×2 的接收 raw 约为 2×1 的 {two['raw_bytes']/one['raw_bytes']:.2f} 倍，压缩对象约 {two['archive_bytes']/one['archive_bytes']:.2f} 倍。正文始终内联保留，节省来自 Zstd 以及主流单一发布资格。没有内容指纹删除，未选来源数不冒充已证明重复事件数。RSS 是各进程峰值之和，并非同一时刻的总驻留峰值。

## 故障、盘口语义和性能

真实 SIGKILL 一个 selected plant 并重启，12 个 market 创建完整 RESET 的耗时约 {delay['min']:.2f}–{delay['max']:.2f} ms；这是 RESET 决定创建时间，不冒充持久发布或客户端收到时间。gateway 随后 SIGKILL 重启形成两个 Tape UUID，重新 CURRENT_SNAPSHOT；旧帧保持身份和内容，所有代的 per-market 序号检查无缺口。实际客户端收到的记录由 receipts 独立限定；未收到的已写 Tape 记录不被宣称已送达。

另一次较活跃的 500-token 探索样本在两个来源各有 8 次瞬时 BBO 不一致：多个原生 price_change 先报告撤单后的 BBO，而实际档位删除分散在后续原生对象中。原文显示 recv_seq=1558–1562、同一原生 timestamp；后续显式删除到达后一致。保留全部独立消息和未被明确删除的档位，不按 timestamp/hash 扩大原子组，也不自动 prune。完整原文、归档和定位在 `evidence/bbo-counterexample/`；此窗口不能宣称每个原生组都与自报 BBO 一致。

最终 24-token 样本 decision→LOCAL_DURABLE p99：P={current['decision_to_durable_p99_ms']['P']:.2f} ms、Q={current['decision_to_durable_p99_ms']['Q']:.2f} ms、gateway={current['decision_to_durable_p99_ms']['G']:.2f} ms。**50 ms 目标未达。** 该指标不包括完整的 WS 入口之前网络延迟，不能称为 exchange→client 延迟。试验主机观测到负载超过 3000（256 个可用 CPU），保留 `host-load.txt`；未调整其他任务优先级或终止其他服务。

固定真实 raw 样本的有限 admission 压力（不是原时间间隔历史回放，也不是 2 小时全链路压力）：

| 设定消息/s | 实际消息/s | 最大单 sync 记录数 | decision→durable p99 ms | 排空后 pending bytes |
|---:|---:|---:|---:|---:|
"""
for r in benchmark["results"]:
    text+=f"| {r['offered_messages_per_second']:.1f} | {r['messages_per_second']:.2f} | {r['max_sync_batch_records']} | {r['decision_to_durable_ms']['p99']:.2f} | {r['pending_bytes_after_drain']} |\n"
text+="""
实测说明没有每组等待一次同步造成的固定 50 组/s 限速；不能说明已达到设定输入负载或生产容量。吞吐/尾延迟仍需隔离资源和持续压力验证，不能调低持久化或删掉失败 token 使测试通过。

## 耐久和归档故障范围

| 层次 | 本次证据 | 仍未证明 |
|---|---|---|
| LOCAL_DURABLE | header 与目录先同步；完整组 group fdatasync；实际进程退出/重启后恢复相同记录；EIO/ENOSPC/sync 失败不发布 | OS 崩溃、断电、设备写缓存行为；未同步尾部损失时长上界 |
| 目录 ARCHIVE_COMMITTED | 不可变数据 + 最后条件 manifest、独立帧/footer、全量下载/解压/摘要、隔离副本删除后恢复 | 目录与源日志同机，不是 REMOTE_ARCHIVED，不保护整机/整盘永久损失 |
| S3 实现 | 单 PUT + SHA256 + If-None-Match、整对象回读；超时/412 的 API fixture | 未配置真实隔离 bucket；真实远端条件写、Range GET、并发提交、网络中断和远端恢复未验收 |
| Query | Tape events、token/market/raw audit Parquet 独立范围与回读 | 自动 5 分钟 checkpoint、完整 source_observations 查询产品 |

连续归档进度按 archive kind/producer/stream 分开；T43 注入 10 段缺口后 11/12 提交不推进前缀。接受试验的封口范围已在目录对象 backend 完成；不把这项结论改写成真实 remote RPO=0。原始接收、生产者决策、消费者接收三种时间和身份独立。

## A–G 交付与未达标项

| 阶段 | 交付 | 状态 |
|---|---|---|
| A | 当前接口实采、版本身份、原文顺序 CaptureLog、半写恢复、固定样本 | 小范围通过 |
| B | Decimal、完整原生组、ASSET snapshot/MARKET RESET、protobuf Envelope/reader、独立参考建簿 | 小范围通过；完整类型化 proto 为附件建议，实际使用版本化 JSON body |
| C | 两 lane 独立 book、单一发布资格、来源硬故障切换 | 小范围通过 |
| D | 独立 P/Q 进程、mTLS、gateway Tape、committed cut、有界 bootstrap、generation/session 隔离 | 小范围通过；生产跨主机故障域未验收 |
| E | Zstd/footer、不可变对象/manifest、恢复、清理证据 | 目录 backend 通过；真实远端仍未验收，默认不自动删除 |
| F | Tape events 查询导出、verify-market、逐 token/market/raw Parquet 审计 | 已列范围通过；自动 checkpoint/完整来源查询视图未交付 |
| G | 2×1/2×2、100/250/500 批次、有限压力、实际故障、runbook、47 项对照 | PARTIAL；延迟目标未达，2 h/24 h 未执行，500 的最终复测单列 |

运行中使用冻结目录；连续全市场 discovery、在线成组迁移、持久 lifecycle 终态调度未接入。不能将该实现描述为已具备全量全天候覆盖，也没有修复/改动旧系统历史积压。保持 UNKNOWN、PARTIAL 和 NOT_EVALUATED。

本次项目改动仅在 `lob_v2/`。全部试采进程结束，保留固定 raw/Tape 对象、manifest、配置、源码/依赖摘要、逐 token 审计、真实消费者 receipts、测试 XML 和实际测量。`evidence/build.json` 绑定当前代码；早期对照与容量试验保留其各自源码摘要，不冒充最终版本的全量 readiness。最终批量流控版本另有 batched-2x1/2x2、batched-capacity-500、batched-faults 实采。
"""
(ROOT/"acceptance_report.md").write_text(text)
print(ROOT/"acceptance_report.md")
