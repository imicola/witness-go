# 从缓冲快照到事件窗口

2026-11-23指导实现用；buffer.go/task.go现已由本人实现，并通过正式回归测试。以下保留原教学时间线与实现指导。验证范围为固定时钟、单线程、单活动事件；不接Docker、HTTP、ticker或goroutine。真实时间晚到样本和并发调度另行设计。

## 数据所有权

装配者创建Ring并串行处理采样和事件。每次新样本先Ring.Append，若有活动Task，再Task.Offer；发生错误停止当前成功路径，不继续假装数据已保存。Start时只调用一次Snapshot，取得前窗；Start之后只投递新样本，不再次把整份快照追加给任务。Snapshot和Append都需要复制CPU指向的值，任务自己的Offer也需要复制；只复制切片头或结构体中的CPU指针都会共享底层数据。

Task.Start使用window.Select(buffer.Snapshot(), event, requested)校验和筛选已有点，把返回的Samples保存为任务自己的候选数据。此时后窗尚未到齐，Select给出的trailing等缺口只是暂态，不作为最终导出。Finish在截止点使用任务全部候选数据再次Select，生成最终覆盖信息。Select失败就返回错误，不把不合法数据送去导出。

## 固定时间示范

以下00/10/20等均指同一分钟内的秒数，输入为同一个逻辑目标及运行实例，MaxGap=10秒。缓冲容量3，事件收到时间20秒，申请窗口[00,40]，任务上限5。

| 动作 | 缓冲Snapshot | 事件任务持有 |
| --- | --- | --- |
| 依次写入00/10/20 | 00/10/20 | 尚未创建 |
| 20秒Start | 00/10/20 | 00/10/20的独立副本 |
| 30秒Append再Offer | 10/20/30 | 00/10/20/30 |
| 40秒Append再Offer | 20/30/40 | 00/10/20/30/40 |
| 40秒Finish | 不再从缓冲重取 | Select得到五点，无缺口 |

若只在最后读取缓冲，前两个点已丢失。若Start后每次追加整个快照，则10/20会重复，最终Select报重复时间戳。40秒须先交付端点样本再Finish，否则最终覆盖会缺尾点。本轮调用者保证：无迟到/乱序交付，截止点的样本交付后才结束；时间戳本身不证明采集已经完成。

提前Finish(39秒)返回ErrNotReady，不改变任务；仍可Offer40秒点。任务超上限返回ErrCapacity并保存failure，后续Finish也返回该失败，不能静默截断后导出。Finish成功后finished为true，后续Offer/Finish返回ErrFinished。本轮由装配者在完成或失败后释放任务引用，不做重复通知或任务队列。

## Ring索引

slots长度就是容量；next指下次写入位置，count是有效样本数。容量3时：写入00/10/20后next回到0；再写30覆盖slots[0]，next变1。物理数组是30/10/20，而Snapshot应是10/20/30。

最早位置可用(next-count+len(slots)) % len(slots)计算。从那里走count次，每次索引用取模绕回。Append先验证身份各字段与非零时间，再复制CPU并写slots[next]，更新next，count最多增加到容量。空Snapshot返回空切片。缓冲按写入顺序保留，最终时间排序由Select负责。

## 本人实现与验收

先写Ring.Append和Snapshot，用容量3手动写四个点，检查返回顺序；分别修改Append输入与Snapshot输出的CPU值，确认缓冲内容不变。然后实现Start/Offer/Finish，按上表调用。Start拒绝nil Buffer和非正maxSamples；对象只经构造器创建，不要求零值Ring可用。Offer对新点校验身份和时间，允许CPU为空，过滤同实例及闭区间；不使用ReceivedAt划分样本，以实际Start前后的串行投递避免重复。

测试至少覆盖前窗在缓冲覆盖后仍存在、提前Finish可重试、任务满后明确失败；无须把原窗口反例重新写一遍。初始样本数大于任务上限时Start直接返回ErrCapacity；后续Offer超上限使任务保持失败。任务有界不代表多个任务总体有界，本轮只有一个活动任务。

装配伪代码中错误都需接收和传播：

```go
buffer, err := NewRing(3)
// 检查err；依次Append 00/10/20，每次检查错误
task, err := Start(buffer, event, requested, 5)
// 检查err；之后每个新点先Append，再Offer，任一失败不继续成功路径
selection, err := task.Finish(end)
// 检查err；成功才把selection交给未来Exporter
```

本轮没有真实Exporter或离线重读实现；这里只验证交给Select的集合正确。正式测试位于internal/pipeline/buffer_test.go与task_test.go，运行go vet ./...、go test -race ./...可复验；窗口边界测试位于internal/window/boundary_test.go。
