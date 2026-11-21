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
| charlie | 上传时间窗 + 窗内多页；同一窗可能整窗重传（日期与条数不变、版本号递增） | 共享短期令牌 | 新格式 |

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
internal/state       每来源读取位置，原子写盘，支持断点续传
internal/store       JSON Lines 批次存储：全局 seq、原子追加、按位置续读
internal/collector   采集引擎：修订判定、撤回防复活、改期同批、崩溃恢复
internal/service     HTTP 装配（/v1/sync、/v1/batches、/v1/records、/v1/status）
tests                端到端测试（16 个场景 + HTTP 层测试）
scripts/verify.sh    一条命令完成的可复现验证（约 2 秒）
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
curl -X POST http://127.0.0.1:18081/mock/control/reset      # 全部复位
# Charlie 整窗重传：窗日期与条数不变，版本号 +1，其中一条携带更高修订
curl -X POST "http://127.0.0.1:18081/mock/control/charlie/retransmit?window=2026-11-10T00:00:00Z"
# 预置重传：在该窗第 arm_page 个分页请求到来前自动触发，模拟读到两页之间赶上重传
curl -X POST "http://127.0.0.1:18081/mock/control/charlie/retransmit?window=2026-11-10T00:00:00Z&arm_page=2"
curl http://127.0.0.1:18081/mock/stats                      # 刷新次数/请求计数/重传次数等
```

## 一条命令的可复现验证（目标 80 秒内，实测约 2 秒）

```bash
scripts/verify.sh
```

脚本会构建二进制、运行全部 Go 测试（核心包带 `-race`），再通过真实 HTTP 接口
依次演示：三种分页首轮采集（含在 Charlie 两页之间触发的整窗重传：更高修订被
采到且不混版发布、已确认窗只查清单）、新旧格式与 `g/m³ → mg/L` 统一、
修订/撤回/改期同批、幂等重跑、限流隔离、共享令牌单次刷新、游标过期头回读防
复活、崩溃后续跑以及下游按保存位置续读。依赖安装时间不计入。

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
- Charlie 的检查点是**已确认窗清单**（窗日期 + 读到的窗版本），而非易失的窗
  下标：未读完的窗不进检查点，重启后整窗重读；重传窗靠版本号识别而不是位置。
- 下游保存的 `seq` 永远有效：读取时从首个批次文件开始扫描，按变更粒度过滤
  `seq > after_seq`，文件轮转不影响旧位置。
对应测试：`Test10_InterruptResume`、`Test14_CharlieRetransmitCrashResume`、
`Test6_DownstreamReadPosition`。

### 6. 限流隔离与令牌刷新

三个来源在同一轮同步中以独立 goroutine 运行；429 被包装成
`RateLimitedError`，只让该来源本轮标记为 `rate_limited`，其余两个继续完成。
共享令牌的 401 在客户端透明刷新一次后重试，刷新由 `auth.Manager` 单飞合并，
同一代令牌失效无论多少并发调用都只产生一次刷新请求。
对应测试：`Test8_TokenSingleRefresh`、`Test9_RateLimitIsolation`。

### 7. Charlie 整窗重传：采到更高修订但绝不混版

Charlie 偶尔会把**整个上传窗重传一遍**：窗日期（`window`）与条数都和原来
一样，只有部分记录携带更高修订号，重传甚至可能在采集器读到该窗两页之间
到达。模拟源为每个窗维护单调递增的 `version`（清单与每个分页都会回传）。

- **已确认且内容没变的窗只查清单**：状态里保存每个已完成窗读到的版本号；
  清单显示版本未变时直接跳过，一个分页请求都不发。
- **重传里的更高修订必须采到**：清单版本变大就整窗重读，修订号更大的记录
  再次产生变更；其余未变行因修订号未涨而被忽略。
- **不能把同一窗两版的分页混着发布**：读窗期间把版本固定在清单版本上；
  一旦某个分页回传的版本与固定版本不一致，立刻丢弃本次已读内容并从该窗
  第一页重读（报告中 `window_restart=true`）。每个窗的变更先在私有覆盖层
  推导，整窗按同一版本读完后才合并发布，所以一个批次里绝不会出现两版
  混排。
- **读到一半失败/重启**：只有完整读完的窗才写入检查点。中断的窗重启后
  从头重读；已发布批次仍是权威记录，回读到的旧修订被单调修订索引吸收，
  不会重复发布。
- 其他两家来源与 Charlie 重传互不影响；数据阶段推进（改期等）仍从同一批
  读取，改期两半仍在同一批次发布。
对应测试：`Test12_CharlieRetransmitImmediate`、
`Test13_CharlieRetransmitBetweenPages`、`Test14_CharlieRetransmitCrashResume`、
`Test15_CharlieRetransmitWithStageProgress`、`Test16_CharlieRetransmitStormFailsFast`。

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
