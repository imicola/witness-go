# Linux 单机 CPU 配额限流实测复跑说明

本页记录 `witness-go` 在本地 Linux（cgroup v2 + Nightingale `notify-receiver`）上完成的一次单机、单实例、单类延迟告警闭环实测与复跑方式。

## 1. 环境与前置

- Linux 内核启用 cgroup v2（`/sys/fs/cgroup/cgroup.controllers` 含 `cpu`）且 `/proc/pressure/cpu` 可读。
- 本地运行隔离监控栈（Prometheus + Blackbox Exporter + Nightingale + `notify-receiver`），其中：
  - Blackbox 探测目标为 `http://test-http:8080/healthz`；
  - Nightingale 延迟告警规则 `http-probe-latency-high`：`probe_duration_seconds{job="http_probe",instance="http://test-http:8080/healthz"} > 0.05`（`eval=10s`，`for=10s`，回调 `http://notify-receiver:18090/n9e/webhook`）；
  - 被测 Go 服务暴露 `/healthz`（`204`）与 CPU 密集型 `/work`（SHA256 循环），启动时配额为 `max 100000`。

## 2. 复跑步骤（基线 → 收紧配额 → 捕获导出 → 撤销恢复）

1. **构建命令行并定位目标容器 cgroup v2 路径**：
   ```sh
   go build -o /tmp/witness-scene ./cmd/scene
   CID=<target-container-id>
   PID=$(docker inspect --format '{{.State.Pid}}' "$CID")
   CGROUP="/sys/fs/cgroup$(awk -F: '$1 == "0" { print $3 }' "/proc/$PID/cgroup")"
   SINCE_SEQ=$(curl -sS http://127.0.0.1:18090/healthz | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')
   ```
2. **启动旁路持续采样（必须在故障发生前启动）**：
   ```sh
   /tmp/witness-scene -mode run \
     -receiver-url http://127.0.0.1:18090 \
     -since-seq "$SINCE_SEQ" \
     -target http://test-http:8080/healthz \
     -run-id "${CID:0:12}:pid-${PID}" \
     -cpu-max "$CGROUP/cpu.max" \
     -cpu-stat "$CGROUP/cpu.stat" \
     -host-psi /proc/pressure/cpu \
     -sample-interval 1s -pre 35s -post 15s -max-gap 5s \
     -out /tmp/live-export.json -stop-after-export=true
   ```
3. **阶段一：基线负载（15 秒，`cpu.max = max 100000`）**：向 `/work` 施加 10 req/s 固定负载，核对 `delta_nr_throttled = 0`、`/healthz` 探测正常。
4. **阶段二：单变量收紧配额并等待真实告警导出**：
   ```sh
   docker update --cpu-quota 10000 "$CID"
   ```
   保持同一 10 req/s 负载，等待 Nightingale 推送真实告警至 `notify-receiver`；`witness-scene` 自动冻结 35s 前窗、收集 15s 后窗、原子写出 `/tmp/live-export.json` 并退出。
5. **阶段三：归还配额并核对恢复（无论告警是否触发都必须执行）**：
   ```sh
   docker update --cpu-quota -1 "$CID"
   cat "$CGROUP/cpu.max"   # 确认回到 max 100000
   ```
6. **离线严格重读校验**：
   ```sh
   /tmp/witness-scene -mode replay -export /tmp/live-export.json
   ```

## 3. 2026-11-28 实测观察摘要（对应脱敏样例 `examples/live-cpu-quota-export.json`）

- **时间差与前窗留存**：`13:40:15` 收紧配额至 `10000/100000`，`13:40:16`（第 17 拍）即捕到 `delta_throttled_us = 1,022,168`、`delta_nr_throttled = 6`；真实 Nightingale 告警（`val = 4.52s`）于 `13:40:32.075` 到达（晚 17 秒），`13:40:47` 原子导出 48 条连续 1 秒样本并通过 `-mode replay` 校验（`leading`/`trailing` 诚实标出首尾边界，0 个 `internal` 缺口）。
- **跨层判据与内核细节**：收紧期间 `cpu.max = 10000/100000`、`delta_usage_us` 被钉在约 `99,000 us/s`（恰好贴住 0.1 核上限）、`delta_nr_throttled = 10/s`；同时宿主 `/proc/pressure/cpu` 的 `some_avg10` 升至 `41.77`（Linux cgroup v2 根层 `some` 会汇总子 cgroup 内受限任务等待），但 `full_avg10` 始终为 `0.00`。
- **撤销恢复对照**：归还 `max 100000` 后，15 秒复测 `delta_nr_throttled = 0`、`delta_throttled_usec = 0`，Blackbox 探测从超时回到 `2.43 ms`（`probe_success = 1`），Nightingale 发出恢复通知，`/work` 排空积压后回落至 `45~60 ms`。

## 4. 安全与范围边界

- 本复跑仅覆盖单机、单容器、单类延迟告警；若撤销配额后 `delta_throttled_usec` 归零而延迟仍高，应撤回容器限流归因，转查宿主 CPU 争用（高 `psi_some` 且无配额限制）或网络/DNS/下游耗时。
- 使用本仓库随附样例可直接离线重读验证，无需启动 Docker：
  ```sh
  go run ./cmd/scene -mode replay -export examples/live-cpu-quota-export.json
  ```
