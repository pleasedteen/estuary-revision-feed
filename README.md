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
internal/mockdata    可复现的分阶段数据集（修订/改期/撤回/旧修订重放）
internal/mockserver  模拟 HTTP 接口（三种分页、401/410/429、控制端点）
internal/auth        共享令牌管理器（单飞刷新，同代只刷一次）
internal/source      三个来源的 HTTP 客户端（401 重试、429/410 分类错误）
internal/state       每来源读取位置 + 待发布变更日志，原子写盘，支持断点续传与旧版升级
internal/store       JSON Lines 批次存储：全局 seq、原子追加、按位置续读
internal/collector   采集引擎：修订判定、撤回防复活、改期同批、窗口补齐、崩溃恢复
internal/service     HTTP 装配（/v1/sync、/v1/batches、/v1/records、/v1/status）
tests                端到端测试（16 个场景 + HTTP 层测试）
scripts/verify.sh    一条命令完成的可复现验证（约 3 秒）
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

批次直接落盘在 `-data` 目录：`batches/batches.jsonl`（每行一个批次信封，
超过 1 MiB 自动轮转为 `batch-NNNN.jsonl`），读取位置在 `state/state.json`。

### 模拟源控制端点（仅模拟服务提供）

```bash
curl -X POST http://127.0.0.1:18081/mock/control/stage     # 推进一个数据阶段
curl -X POST "http://127.0.0.1:18081/mock/control/ratelimit?source=alpha&on=true"
curl -X POST "http://127.0.0.1:18081/mock/control/charlie/arm-append?window=2026-11-10T00:00:00Z&n=1"  # 抓取中追加
curl -X POST http://127.0.0.1:18081/mock/control/reset      # 全部复位
curl http://127.0.0.1:18081/mock/stats                      # 刷新次数/请求计数等
```

## 一条命令的可复现验证（目标 80 秒内，实测约 3 秒）

```bash
scripts/verify.sh
```

脚本会构建二进制、运行全部 Go 测试（核心包带 `-race`），再通过真实 HTTP 接口
依次演示：三种分页首轮采集、新旧格式与 `g/m³ → mg/L` 统一、修订/撤回/改期同批、
幂等重跑、限流隔离、共享令牌单次刷新、游标过期头回读防复活、崩溃后续跑以及下游
按保存位置续读。依赖安装时间不计入。

只跑 Go 测试：

```bash
go test ./...            # 功能测试
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

- 每抓完一页先发布批次（临时文件 + `rename` 原子落盘，`fsync` 后返回），
  **再**保存下一页的读取位置；位置落盘同样是临时文件 + 原子替换。
- 进程重启后，记录索引通过**重放全部已发布批次**重建，序号计数器扫描批次文件
  恢复，所以读到一半崩溃只会从上一个已确认位置继续，已发布修订不会重复生成变更。
- charlie 的窗口读取位置是**按窗口记录下一行偏移**的映射，并且每抓完一页就把
  「窗口进度 + 已派生但未发布的变更（pending 日志）+ 预留批次号」原子写入
  `state/state.json`。中断重启后从**第一个缺页**继续而不是重头扫；日志里未发布的
  变更与恢复后新派生的变更**合为同一批次**发布，跨页、跨重启的改期都能从同一个
  批次读出。若崩溃发生在「批次已发布、日志未清」之间，重放索引会识别出这些变更
  已发布并在载入时丢弃，绝不重发。
- 下游保存的 `seq` 永远有效：读取时从首个批次文件开始扫描，按变更粒度过滤
  `seq > after_seq`，文件轮转不影响旧位置。
对应测试：`Test10_InterruptResume`、`Test14_CharlieCheckpointResume`、
`Test16_CharliePublishedPendingNotReemitted`、`Test6_DownstreamReadPosition`。

### 6. charlie 窗口补齐：补登靠前窗口、旧窗追加、快照轮次

charlie 每轮同步先取一次窗口目录（窗口名 + 行数）作为**本轮快照**，然后逐窗口只
请求「已消费偏移 … 快照行数」之间的新增范围：

- **补登排序靠前的窗口**：目录按窗口名排序，进度按窗口名记录（而非列表下标），
  所以靠后补登的早起窗口下一轮自然出现在待读集合里，从偏移 0 读起。
- **已读完的旧窗口被追加明细**：旧窗口的消费偏移小于快照行数，差值就是本轮要
  补的范围，明细请求只覆盖新增行。
- **抓取期间的追加留给下一轮**：窗口行数在抓取途中增长（模拟源用
  `arm-append` 注入）时，越过分快照边界的行即使已经在响应里被翻到也会被截断，
  留给下一轮按新偏移单独请求。
- **旧数据目录升级**：旧版状态文件（`window_index`/`window_offset`，无窗口映射）
  在升级后首轮同步时按当时目录迁移——下标之前的窗口按当前行数记为已消费、下标
  处窗口保留原窗内偏移，因此已发布变更不重发，之后按新格式续跑。
对应测试：`Test12_CharlieBackfillAndOldWindowAppend`、
`Test13_CharlieAppendDuringFetch`、`Test15_CharlieStateUpgrade`。

### 7. 限流隔离与令牌刷新

三个来源在同一轮同步中以独立 goroutine 运行；429 被包装成
`RateLimitedError`，只让该来源本轮标记为 `rate_limited`，其余两个继续完成。
共享令牌的 401 在客户端透明刷新一次后重试，刷新由 `auth.Manager` 单飞合并，
同一代令牌失效无论多少并发调用都只产生一次刷新请求。
对应测试：`Test8_TokenSingleRefresh`、`Test9_RateLimitIsolation`。

## 数据文件格式

`batches/batches.jsonl` 每行一个批次信封：

```json
{
  "seq": 1,
  "batch_id": "bravo-1795143282879085674-003",
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

`state/state.json` 保存每来源读取位置，charlie 额外带待发布变更日志：

```json
{
  "positions": {
    "alpha":   { "offset": 8 },
    "bravo":   { "cursor": "", "cursor_offset": 7 },
    "charlie": { "windows": { "2026-11-10T00:00:00Z": 3, "2026-11-11T00:00:00Z": 4 } }
  },
  "pending": {
    "charlie": { "batch_id": "charlie-…-000", "changes": [ … ] }
  }
}
```

`pending` 只在窗口抓取中途崩溃后存在，恢复运行发布成批后即清除；旧版状态文件
（`window_index`/`window_offset`，无 `windows` 映射与 `pending` 段）可直接打开，
首轮同步自动迁移。
