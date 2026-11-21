# 河口悬浮物本地采集服务（estuary-collector）

把三家机构（alpha / bravo / charlie）的悬浮物采样记录，通过本地 HTTP
服务周期性汇聚成**统一口径的 JSON Lines 批次**，下游用自己保存的读取位置
（全局单调序号 `seq`）续读已发布的批次。全部验证仅使用项目自带的本地模拟源，
不依赖任何外部网络。

## 三家机构与分页方式

| 机构 | 分页方式 | 认证 | 响应格式 |
|------|----------|------|----------|
| alpha | 偏移分页 `offset/limit` | 共享短期令牌 | 新格式 |
| bravo | 不透明游标分页，过期后需从头回读（会重放旧修订） | 静态 Bearer | 旧格式 |
| charlie | 上传时间窗 + 窗内多页 | 共享短期令牌 | 新格式 |

alpha 与 charlie **共用同一代短期访问令牌**：一代令牌失效后，采集服务对认证端
**只发起一次刷新**，并发的 401 调用合并等待同一次刷新结果（见
`internal/auth/manager.go`）。

## 目录结构

```
cmd/collector        采集服务主程序（HTTP 接口 + 编排引擎）
cmd/mocksrv          三家机构 + 认证端的本地模拟服务（确定性数据）
internal/domain      统一后的规范记录、变更、批次信封类型
internal/normalize   新旧两种响应格式识别、字段名与浓度单位统一
internal/mockdata    可复现的分阶段数据集（修订/改期/撤回/旧修订重放/积压造数）
internal/mockserver  模拟 HTTP 接口（三种分页、401/410/429、控制端点）
internal/auth        共享令牌管理器（单飞刷新，同代只刷一次）
internal/source      三个来源的 HTTP 客户端（401 重试、429/410 分类错误）
internal/state       每来源读取位置，原子写盘，支持断点续传
internal/store       JSON Lines 批次存储：全局 seq、流式整批发布、按位置续读
internal/collector   采集引擎：修订判定、撤回防复活、改期同批、崩溃恢复
internal/memstats    Go 堆高水位采样器（内存有界验证）
internal/service     HTTP 装配（/v1/sync、/v1/batches、/v1/records、/v1/status、/debug/mem）
tests                端到端测试（12 个场景 + HTTP 层测试）
scripts/verify.sh        一条命令完成的可复现验证（约 30 秒）
scripts/verify_memory.sh 64 MiB 积压下的堆峰值专项验证（打印两个峰值）
```

## 快速开始

前置：Go 1.23+（仅使用标准库，无需 `go mod download`）。

```bash
# 1) 启动模拟机构 + 认证端（终端 A）
go run ./cmd/mocksrv -addr 127.0.0.1:18081

# 2) 启动采集服务（终端 B）
go run ./cmd/collector -addr 127.0.0.1:18080 -mock 127.0.0.1:18081 -data ./data
```

也可以先 `go build -o bin/ ./cmd/...` 再运行 `bin/mocksrv`、`bin/collector`。

### 触发同步

```bash
curl -X POST http://127.0.0.1:18080/v1/sync
```

返回每个来源的状态、抓取页数、见到的记录数、产生的变更数与批次号。部分来源
被限流时 HTTP 状态为 `206 Partial Content`，其他来源照常完成；重复触发会得到
`409 Conflict`（同一时刻只允许一轮同步）。

### 下游读取批次（按保存位置续读）

```bash
# 首次读取，保存响应里的 next_seq 作为自己的读取位置
curl "http://127.0.0.1:18080/v1/batches?after_seq=0&limit=100"

# 之后带上次保存的 next_seq，只会读到新批次，重启/文件轮转后位置依然有效
curl "http://127.0.0.1:18080/v1/batches?after_seq=34"
```

其他接口：

| 接口 | 说明 |
|------|------|
| `GET /healthz` | 健康检查 |
| `GET /v1/status` | 是否正在同步、当前最大序号 `max_seq` |
| `GET /v1/records` | 当前可见（未撤回）的规范记录快照 |
| `GET /v1/batches?after_seq=N&limit=M` | 从位置 N 续读，返回 `next_seq` |
| `POST /v1/sync` | 触发一轮同步 |
| `GET /debug/mem` | Go 堆高水位（进程启动以来 / 上次重置以来） |
| `POST /debug/mem/reset` | GC 后重置高水位，返回稳态基线 |

批次直接落盘在 `-data` 目录：每个批次一个文件
`batches/env-<首seq>-<末seq>-<batchID>.jsonl`（单行信封，与旧格式同形状，
先写临时文件再原子改名发布）；读取位置在 `state/state.json`。旧版本写
出的 `batches.jsonl` 与轮转归档 `batch-NNNN.jsonl` 仍可被正常读取、
参与恢复与去重，已发出的 `seq` 书签在新旧文件混排时依然有效。

### 模拟源控制端点（仅模拟服务提供）

```bash
curl -X POST http://127.0.0.1:18081/mock/control/stage     # 推进一个数据阶段
curl -X POST "http://127.0.0.1:18081/mock/control/ratelimit?source=alpha&on=true"
curl -X POST http://127.0.0.1:18081/mock/control/reset      # 全部复位
curl -X POST http://127.0.0.1:18081/mock/control/bulk \     # 造积压：少量样本反复修订
  -H 'Content-Type: application/json' \
  -d '{"source":"alpha","samples":8,"revisions":220000,"page_size":5000,"page_delay_ms":25}'
curl http://127.0.0.1:18081/mock/stats                      # 刷新次数/请求计数等
```

## 一条命令的可复现验证（目标 80 秒内，实测约 30 秒）

```bash
scripts/verify.sh
```

脚本会构建二进制、运行全部 Go 测试（核心包带 `-race`），再通过真实 HTTP 接口
依次演示：三种分页首轮采集、新旧格式与 `g/m³ → mg/L` 统一、修订/撤回/改期同批、
幂等重跑、限流隔离、共享令牌单次刷新、游标过期头回读防复活、崩溃后续跑、下游
按保存位置续读，最后调用 `scripts/verify_memory.sh` 做内存有界验证。依赖安装
时间不计入。

### 内存有界验证（≥64 MiB 积压，两个堆峰值）

```bash
scripts/verify_memory.sh
```

本地造数：8 个采样号反复修订 22 万次（≥64 MiB 变更，含一对改期），随后一条
命令完成：慢源采集期间其他来源先行发布、整批发布与改期同批、旧 JSONL 与已发
书签续读、重启恢复，并打印**同步**与**重启恢复**两个额外 Go 堆峰值（预算各
32 MiB，实测约 12 MiB / 4 MiB）。

只跑 Go 测试：

```bash
go test ./...            # 功能测试（含 64 MiB 堆峰值回归测试 Test12）
go test -race ./...      # 含竞态检测
```

## 关键语义与实现方式

### 1. 修订先后与去重

采样编号在机构内唯一，同一条记录的修订号（`rev` / `revisionId`）单调递增。
引擎为每个归档键（机构|采样号|采样日）维护已知最大修订号；**修订号不大于已知
值的回读记录一律忽略**，因此重复同步、游标过期后的头回读都不会再次产生变更。

### 2. 撤回后旧修订不能让记录复活

撤回本身是一个更高修订号的变更，索引中该键进入 `withdrawn` 状态。游标过期后
从头回读到的旧版"有效"修订，因修订号更小而被直接丢弃，记录保持撤回。
对应测试：`Test7_CursorExpiryReplay`。

### 3. 改期必须在同一批发布

改期在数据上表现为相邻的两条修订：旧日的 `withdrawn` 与新日的 `upsert`。
单个来源一轮内的所有变更**收集后一次性发布为一个批次**（一个 JSONL 信封），
因此撤除与补入对外同时可见，不会出现只删未补的中间态。
对应测试：`Test4_RescheduleAtomic`。

### 4. 新旧格式与单位统一

每条记录按字段名独立识别格式（新格式 `sample_id/rev/...`，旧格式
`sampleId/revisionId/...`，旧格式状态 `A/D`）。浓度统一用 `math/big.Rat`
精确换算为 mg/L（`1 g/m³ = 1 mg/L`），以十进制字符串保存避免浮点漂移。

### 5. 断点续传与崩溃恢复

- 单个来源一轮的变更随抓随写入暂存文件（不在内存累积），来源读完后才
  一次性拼装为批次文件（临时文件 + `rename` 原子落盘，`fsync` 后返回），
  **再**保存下一轮的读取位置；位置落盘同样是临时文件 + 原子替换。
- 进程重启后，记录索引通过**流式重放全部已发布批次**逐条重建，序号计数器
  由批次文件名携带的序号区间恢复（旧格式文件则流式扫描），所以读到一半
  崩溃只会从上一个已确认位置继续，已发布修订不会重复生成变更。
- 下游保存的 `seq` 永远有效：读取时按变更粒度过滤 `seq > after_seq`，
  序号区间小于等于位置的批次文件直接跳过，新旧文件布局混排不影响旧位置。
对应测试：`Test10_InterruptResume`、`Test6_DownstreamReadPosition`。

### 6. 大积压下的内存有界（同步与恢复各 < 32 MiB）

机构积压再多修订，同步与重启恢复的堆占用也不随积压增长：

- **发布流式化**：变更逐条落暂存文件，提交时按序分配连续 `seq` 区间并
  流式拼装出最终批次文件；整批可见性由 `rename` 保证，空批次不落盘。
- **恢复流式化**：信封用 `json.Decoder` 逐变更解码，内存里只有当前一条
  变更加上按归档键索引的最新状态（键数 = 不同采样数，而非修订数）。
- **每批次一个文件**：`env-<首seq>-<末seq>-<batchID>.jsonl`，内容仍是
  单行信封（与旧格式同一形状），文件名即序号索引；旧版 `batches.jsonl`
  与 `batch-NNNN.jsonl` 由同一流式解析器读取，巨型单行也不再受扫描器
  行宽限制。
- 三源并发不变：慢来源还在采集时，其他来源的批次先提交先可见；同一
  来源的改期两半仍在同一个批次文件里。
对应测试：`Test12_HeapBoundedSyncAndRecovery`；专项脚本：
`scripts/verify_memory.sh`（实测同步约 12 MiB、恢复约 4 MiB）。

### 7. 限流隔离与令牌刷新

三个来源在同一轮同步中以独立 goroutine 运行；429 被包装成
`RateLimitedError`，只让该来源本轮标记为 `rate_limited`，其余两个继续完成。
共享令牌的 401 在客户端透明刷新一次后重试，刷新由 `auth.Manager` 单飞合并，
同一代令牌失效无论多少并发调用都只产生一次刷新请求。
对应测试：`Test8_TokenSingleRefresh`、`Test9_RateLimitIsolation`。

## 数据文件格式

每个批次一个文件 `batches/env-<首seq>-<末seq>-<batchID>.jsonl`，内容为单行
批次信封（与旧版 `batches.jsonl` / `batch-NNNN.jsonl` 中每行的信封完全同
形状，旧文件照常参与读取与恢复）：

```json
{
  "seq": 1,
  "batch_id": "bravo-1795143282879085674",
  "changed_at": "2026-11-20T02:54:42.879087965Z",
  "changes": [
    {
      "seq": 1,
      "op": "upsert",
      "institution": "bravo",
      "sample_id": "B-200",
      "sampled_on": "2026-11-10",
      "revision": 1,
      "record": { "unit": "mg/L", "mg_per_l": "6.4", "...": "..." }
    }
  ]
}
```

`op` 取值 `upsert`（含完整规范记录）或 `withdraw`（仅定位键与修订号）。
