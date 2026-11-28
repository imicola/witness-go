# witness-go

一个基于 Go 标准库的 Linux 服务延迟诊断、告警现场留存与离线回放工具（配合 Prometheus / Nightingale 使用）。

项目围绕“慢请求或异常告警到达时，异常往往已经发生甚至结束”的痛点构建：旁路持续采样并写入有界环形缓冲，告警到达时冻结前窗并继续收集后窗，按同一逻辑目标与运行实例（`Target` + `RunID`）校验缺口与覆盖边界后原子导出为带来源与版本标识的 JSON 工件，支持严格离线重读与告警恢复语义核验。

这是持续迭代的学习与工程实践项目，开发过程使用了 AI 辅助教学、脚手架搭建、代码审查与文档整理；核心状态流转、窗口规则、原子落盘、严格重读与反例测试由作者主导实现。下文如实区分已验证范围与待接入边界。

## 当前实现与状态

| 模块 | 职责与当前状态 |
| --- | --- |
| `cmd/scene` | 命令行入口，支持离线 `inspect`/`select`/`export`/`replay` 与 Linux 在线 `run` 模式 |
| `internal/scene` | 核心类型定义：`Identity`（分离逻辑目标与运行实例）、`Sample`（含 `CPU`/`PSI` 与 `Clone`）、`Event`、`Window`、`Gap`、`Selection`、`Export` |
| `internal/fixture` | 严格 JSON 构造夹具读取（拒绝未知字段、多文档尾随及缺 `provenance`） |
| `internal/window` | `Select` 点样本闭区间筛选、排序/重复校验、指针深拷贝与 `leading`/`internal`/`trailing`/`no_samples` 缺口计算 |
| `internal/collector` | Linux cgroup v2（`cpu.max`、`cpu.stat`）与宿主 `/proc/pressure/cpu` 跨层解析，以及同 `RunID` 防负增量/重启重置状态机 `DeltaTracker` |
| `internal/notify` | Nightingale `notify-receiver` 增量游标轮询 `ReceiverSource` 与 `TranslateRecord`（过滤恢复通知、`hash:first_trigger_time` 稳定去重键、逻辑目标到 `RunID` 绑定） |
| `internal/pipeline` | 有界环形缓冲 `Ring`、单事件前后窗任务 `Task`、串行去重协调器 `Coordinator` 与单 owner 并发驱动循环 `Runner` |
| `internal/exporter` | `FileExporter`（同目录临时文件 + 显式 `Close` 检查 + `os.Rename` 原子替换）与严格 `Read`/`ReadFile`/`Summarize` |
| `internal/replay` | 独立恢复判定 `Judge`（校验同目标、同次抓取及样本新鲜度，区分 `probe failed`、`probe path recovered` 与 `insufficient evidence`） |
| `examples` | 2026-11-28 本地 Linux 实测脱敏导出样例（`live-cpu-quota-export.json`） |
| `testdata` | 明确标明来源为构造教学夹具的演示数据，非实测故障现场 |
| `docs` | 缓冲任务设计、在线装配边界与一页 Linux 配额限流复跑说明（`reproduce-linux-quota.md`） |

当前**已验证范围**：全仓六包（`collector`、`exporter`、`notify`、`pipeline`、`replay`、`window`）单元测试与 `-race` 竞态回归，以及 2026-11-28 本地 Linux 单机实测闭环（持续采样 → 收紧容器 CPU 配额 → Nightingale 延迟告警触发 → 原子导出 48 条真实样本 → 撤销恢复对照 → `cmd/scene -mode replay` 严格重读，见 [docs/reproduce-linux-quota.md](docs/reproduce-linux-quota.md) 与 [examples/live-cpu-quota-export.json](examples/live-cpu-quota-export.json)）。  
当前**暂缓扩展范围**：多实例并行采集、跨网络采集、记录器跨进程持久去重以及采集开销正式基准压测。

## 快速开始

需要 Go 1.23 或更高版本（见 [go.mod](go.mod)）。当前离线测试与构造夹具 CLI 可在 Windows 与 Linux 上直接运行，不依赖 Docker 或运行中的 Nightingale。

```sh
git clone https://github.com/imicola/witness-go.git
cd witness-go
go vet ./...
go test -race -count=1 ./...
```

使用构造夹具演示一次“选窗 → 原子导出 → 严格离线重读”闭环：

```sh
go run ./cmd/scene -mode inspect
go run ./cmd/scene -mode select
go run ./cmd/scene -mode export -out tmp-export.json
go run ./cmd/scene -mode replay -export tmp-export.json
```

> 构造夹具输出会明确携带 `"provenance": "synthetic teaching fixture; not a historical incident or real Nightingale notification"`，用于验证数据结构与覆盖语义，不代表实测告警现场。

## 设计与可靠性边界

- **资源所有权与原子写**：`FileExporter.Write` 在目标文件同目录下创建临时文件，编码完成后显式检查 `Close()` 错误再执行 `os.Rename`，任何失败路径均删除临时文件并保持既有目标文件完整。
- **严格重读校验**：`exporter.Read` 开启 `DisallowUnknownFields()`、拒绝尾随多文档，先检查样本严格递增，再复用 `window.Select` 核对样本序列、`First`/`Last` 与 `Gaps` 是否与事件和申请窗口一致，防止静默排序或过滤掩盖损坏的现场文件。
- **CPU 现场与探测恢复分离**：`internal/replay.Judge` 专门校验 `up` 与 `probe_success` 的同目标同轮新鲜度以拦截“停探测器引发的假恢复”；CPU 节流与延迟现场由 `Selection`/`Export` 独立承载，两者不在数据结构上混用。

## 许可协议

本项目采用 [Apache License 2.0](LICENSE) 开源。
