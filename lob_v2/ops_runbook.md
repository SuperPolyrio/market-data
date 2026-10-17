# V2 运维与故障边界

1. 新建冻结目录，核验两个 token 的身份与每路实际 WRITE_OK/原生 book 集合。保留 unresolved，不用容量不足作为排除原因。
2. 用独立数据根启动 P/Q，再启动单 writer gateway；同 host 试验不能认证多 host 容灾。跨机使用受信 mTLS 和独立存储/出口。
3. 关注 journal pending bytes、最老输入、raw/emit/tape 速率、进程 RSS/CPU、断连、基线等待以及归档错误；`status.json` 的分位数为最多 65,536 个最近提交样本。
4. WS 出错撤销当前 lane，合格目标通过完整 RESET 接管；重连等待当前会话 book。生产每 300 s 对缺失基线的 token 在原连接上重订阅，避免单个无盘口 token 重置整分片。token 留在分母。低活跃 token 不凭时间戳静默过期。
5. plant 会话重建执行 CURRENT_SNAPSHOT → chunks → END → ACK → READY，cut+1 使用有界缓冲；溢出取消并重建。FLOW_ACK 仅当前会话额度。
6. gateway 进程重启先扫描旧 `.open` 到合法边界并封口，再创建新 UUID。旧合法帧仅作为旧 Tape 历史；CRC 损坏隔离并停止，不能截掉已坏的中间记录后宣称连续。半写尾部记录恢复报告，未同步损失仍为 UNKNOWN。
7. 归档失败保留 journal。检查对象数据、整对象 SHA256、footer、manifest；确认连续前缀及洞，不能拿最大上传 offset 替代连续进度。索引损坏时列举 commits 重建。
8. base 配置关闭自动清理，生产仅回收 writer 私有目录的封口缓存。必须具有不可变 manifest、远端完整读回及原帧摘要证据，提交回调已越过段末且没有本地文件依赖；消费者从归档读回。开放段和未归档日志禁止清理。
9. 磁盘/队列预算达到阈值时 fail closed。扩大容量前比较生产生成率、压缩排空率、未归档唯一范围和 inode；不能删除唯一未归档数据释放空间。

回滚只需停止 V2 进程，保留其 run/evidence；它不修改旧版服务。旧采集器的 backlog、生命周期覆盖和历史补录不在这次新建试验的完成范围。

未验收项目：真实 S3 后端的条件写/Range GET/并发提交/超时恢复资格；整机/整盘永久损失与断电；2 小时压力、24 小时长稳；持续目录变化与热迁移。不得将对应状态改成 PASS。

## Broad deployment (2026-10-17)

`tools/production.py` manages immutable market ownership cohorts. One market has
one cohort with two independent plant worker processes (P/Q) and one gateway.
Each plant retains two independent official Market Channel connections per WS
shard. Cohorts append without moving existing ownership. Identity changes stop
the affected cohort with `MIGRATION_REQUIRED`; absence from a later catalog does
not by itself prove terminal closure. Query projections remain separately rebuilt.

The current deployment state is in `runs/production-20261017`, live journals and
status are under `/data/jiahuaiyu/market-data/lob-v2`, and conditional immutable
objects/manifests live on XUE at
`/mnt/hdd22t/prediction-market-quant/lob_v2_archive`.
The SSH receiver uses directory fsync, conditional hard-link publication, and
full readback. Cache eviction is restricted to the writer-owned sealed directory;
readback consumers use remote immutable objects. Do not scan or retain private
journal files as a live replay consumer. FLOW_ACK grants no deletion permission.

P/Q currently run as separate processes on SUN, with separate official WS connections on the existing dedicated profile-12
VPN and LA routes. P/Q share the SUN host and these egress providers; neither
host nor egress failures are independent between plants. This protects against a plant process or route failure; it does not
provide independent capture hosts. LOCAL_DURABLE can lose the SUN disk/host;
REMOTE_ARCHIVED objects survive SUN loss but depend on the single XUE archive
host. Unsealed data and archive failures remain local until committed. There
is no claim of cross-source event union or upstream event completeness.

`market-data-lob-v2@*.service` instances, `market-data-lob-v2-discovery.service`,
`market-data-lob-v2-health.timer` and `market-data-lob-v2-la-tunnel.service` are
user systemd services. `python tools/production.py health --root <deployment>`
reports assigned, written, baseline, ready, publication and archive counters.
A full event sweep is a supplementary census, not a complete market sweep.
Failures retain raw journals. Restarting gateway creates a fresh Tape UUID and
all plant sessions require CURRENT_SNAPSHOT. Old generation replay uses the
original archived frames.

Five-minute/128 MiB segments reduce small-object and inode growth. A host-wide
SSH archive limit bounds upload/readback work. The local spool budget and minimum
free-space thresholds fail closed. Archive prefix, internal holes, uncommitted
tail, sealed bytes and oldest age are distinct; zero sealed bytes does not imply
that the currently open segment is remotely archived.

The initial attempt used 500 assets/socket at full scale and suffered PONG timeout
storms. Its raw logs and deployment receipts are retained; it is not acceptance
evidence. Python 3.10 also exposed a TLS transport close race. The production
launcher uses the project's Python 3.12 and `.vendor312`, and capacities are tested
against native subscriptions before promotion. Do not infer readiness from unit
activity or sent subscription counts.

The 2.0.3 collector uses 2,000 tokens per socket after broad deployment
showed tens of seconds of receive backlog at 8,000. It requires a current-session
PONG with RTT <= 2,000 ms and proof age <= 30 s before source qualification.
Heartbeat checks revoke expired proof; native observations continue to be saved.
A healthy lane 0 replaces lane 1 with a complete SOURCE_RESET. This checks queue
freshness and does not establish exchange event completeness. Full-depth REST
bracket comparisons are recorded separately, including failed samples.
PING continues every 10 s while PONGs are delayed, as required by the official
Market Channel. At most 32 outstanding sends are correlated FIFO; using the most
recent send would incorrectly hide accumulated RTT. Queue exhaustion reconnects
and requires a fresh native baseline.

After a 60 s connection grace, an outstanding PING older than 30 s causes an
explicit BACKLOG_RESYNC and reconnect, even when late native frames still arrive.
This reacquires a current native baseline instead of draining stale upstream data
indefinitely. Captured bytes remain immutable; undelivered upstream continuity is
UNKNOWN and source recovery emits SOURCE_RESET. This bounds continued use of a
stale connection; it does not create network bandwidth or guarantee every market
has a qualified source. Existing 2.0.2 gateways use the unchanged wire, book and
selection contract; collector transport recovery is versioned separately.

The logical archive has two physical XUE tiers: immutable conditional commits
first land on `/mnt/polygon/lob-v2/remote-archive-hot` (NVMe), with >= 2 TiB and
100,000 inodes reserved. A separate cold mirror verifies data and conditionally
publishes manifests on the HDD path above. Hot cache deletion requires verified
cold custody and no active read lease. The cold tier has its own continuous
prefix, holes and pending ranges; an NVMe commit does not imply HDD completion.
Both tiers share one XUE host and are not two independent remote failure domains.
The local completed receipt index can be compacted; the immutable remote manifest
and object footer retain all block indexes. No observation bytes are removed by
receipt compaction.
