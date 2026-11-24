# 输入输出与后续装配

完整链路为 Sampler → Buffer → EventSource 到达 → 立即复制前窗 → 继续收后窗 → Window.Select → Exporter → Replayer。目前这些在线步骤仅有接口，不能用 inspect 输出宣称已跑通。

## 当地真实材料核对

本机 `.local/n9e-cpu-quota-lab/results.Yxwx7E/constrained.requests.csv` 来自客户端 load.py。前两条是 200/1448.018ms 和 200/1545.574ms，第三条 TimeoutError/2002.405ms。request 是提交序号，elapsed_ms 是本次客户端调用耗时，dispatch_delay_ms 是实际开始相对计划的延迟。CSV 没有墙钟请求时间，不能凭序号把请求塞入精确告警窗口。

同目录 constrained.before.time 为 2026-11-19T16:01:59.971819343+08:00，after 为16:02:32.076564812。时间文件在读取计数前产生，代表近似读取时刻，不是原子跨层快照。cpu.max 为30000/100000；cpu.stat 的 nr_throttled 为69→390，throttled_usec 为12723559→70914680，分别相差321和58191121微秒。原脚本检查相同容器PID；历史 config 留有容器ID和cgroup路径，但这些不是今日正在运行的身份。请求300条，291条错误，成功P99仅覆盖9条。

旧通知已在 `/home/imicola/imicolaRes/n9e/lab/evidence/20261115T111219Z-w48/snap-4b-true-recovery/receiver-events.json` 核对。首条 received_at_utc 为2026-11-15T11:12:49.678951674Z，payload.trigger_time 为1794741168，tags_map 指向 job=http_probe、instance=http://test-http:8080/healthz。它属于11月15日探测故障，不与11月19日配额数据拼成一个事件。此处只列必要字段，原始通知仍留本地，不复制 headers、地址或全量 payload。

## 谁拥有数据和资源

CLI/装配者创建采样器、缓冲、接收器和导出器，负责生命周期及退出取消；采样器只借用读取路径。通知进入时记录本地到达时间，目标映射必须显式绑定运行身份；Prometheus instance 是逻辑探测目标，不能当作容器运行ID。字段含义需按现场版本核对，trigger_time 不能当作故障起点。

Buffer 必须有容量上限；Snapshot 深拷贝后由事件任务持有，后窗任务数和单窗大小也要有上限。Select 只借用完整快照，输出持有自己的样本。后窗未结束不能伪称完整导出；取消和缺样应落入覆盖信息。读失败不得填零；计数差仅在同运行来源且计数未回退时计算。

Exporter 后续保存版本、数据来源、事件、申请窗口、实际覆盖、缺口和样本；失败交还装配层，不能继续宣称导出成功。离线 CLI 负责重新读取文件，不启动故障。现有 [internal/replay/replay.go](../internal/replay/replay.go) 的 Judge 只接受同目标同轮 up/probe 样本；本项目 CPU 点不能强转进去。恢复回放适配器与 CPU 证据分析分开实现。

## 后续装配顺序

先完成固定夹具的选窗和反例，再实现有界缓冲与固定时钟事件任务，验证前窗复制/后窗收集/导出读取。之后接 Linux cgroup 与请求统计，再接已有 notify-receiver；每接一段记录真实通过范围。运行前按交接核现场和归还方式；本次没有重跑上周实验。
