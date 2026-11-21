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
| charlie | 上传时间窗 + 窗内多页；窗口带版本，可补挂旧窗口、原地改版 | 共享短期令牌 | 新格式 |

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
internal/state       每来源读取位置，原子写盘，支持断点续传（含 charlie 窗口版本清单）
internal/journal     窗内分页的页级进度日志（原始记录 + 下一页偏移），供崩溃后续页
internal/store       JSON Lines 批次存储：全局 seq、原子追加、按位置续读
internal/collector   采集引擎：修订判定、撤回防复活、改期同批、窗口版本化、崩溃恢复
internal/service     HTTP 装配（/v1/sync、/v1/batches、/v1/records、/v1/status）
tests                端到端测试（14 个场景 + HTTP 层测试）
scripts/verify.sh    一条命令完成的可复现验证（约 5 秒）
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
curl http://127.0.0.1:18081/mock/stats                      # 刷新次数/请求计数等（含窗口清单与清单/取页计数）

# 以原始记录追加到 charlie 窗口（窗口版本 +1，等价于“旧窗口原地改版”）
curl -X POST http://127.0.0.1:18081/mock/control/charlie/append \
  -d '{"window":"2026-11-10T00:00:00Z","records":[{"institution":"charlie","sample_id":"C-300","rev":2,"sampled_on":"2026-11-10","site":"河口东","substance":"悬浮物","concentration":"9.8","unit":"mg/L","status":"active","published_at":"2026-11-13T01:00:00Z"}]}'
# 让某窗口的旧版本快照失效（模拟窗口改版后旧版本已不可续读）
curl -X POST http://127.0.0.1:18081/mock/control/charlie/expire \
  -d '{"window":"2026-11-10T00:00:00Z"}'
```

## 一条命令的可复现验证（目标 80 秒内，实测约 5 秒）

```bash
scripts/verify.sh
```

脚本会构建二进制、运行全部 Go 测试（核心包带 `-race`），再通过真实 HTTP 接口
依次演示：三种分页首轮采集、新旧格式与 `g/m³ → mg/L` 统一、修订/撤回/改期同批、
幂等重跑、限流隔离、共享令牌单次刷新、游标过期头回读防复活、Charlie 补挂旧窗口
与旧窗口原地改版（未变化窗口只查清单、不取页）、崩溃后续跑以及下游按保存位置
续读。依赖安装时间不计入。

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
对 charlie 而言，这个“一次性发布”的单位是一个**完整取完的窗口版本**：同一窗口
版本的撤除与补入必然在同一批次里，批次也绝不会混入另一个版本或只取了一半的窗口。
对应测试：`Test4_RescheduleAtomic`、`Test12_CharlieLateAndRevisedWindows`。

### 3.1 Charlie 窗口：补挂旧窗口、原地改版与只查清单

charlie 的窗口清单带版本号（`window` + `version` + `row_count`），采集器据此：

- **每次同步都重读清单**，因此新开窗口、以及上传时间早于已读窗口的**补挂旧窗口**
  （排序后落在已读位置之前）都会被完整取页；位置不再用窗口序号，而用窗口标识。
- 某窗口版本与已发布版本一致 → **只查清单、不取任何行**（未变化窗口零取页）。
- 版本变高（旧窗口原地追加了修订/改期/撤回）→ 该窗口**从头完整重取**，
  变更仍按修订号去重，未变化的行不会产生变更。
对应测试：`Test12_CharlieLateAndRevisedWindows`（含“未变化窗口零取页”断言）。

### 4. 新旧格式与单位统一

每条记录按字段名独立识别格式（新格式 `sample_id/rev/...`，旧格式
`sampleId/revisionId/...`，旧格式状态 `A/D`）。浓度统一用 `math/big.Rat`
精确换算为 mg/L（`1 g/m³ = 1 mg/L`），以十进制字符串保存避免浮点漂移。

### 5. 断点续传与崩溃恢复

- 每抓完一页先发布批次（临时文件 + `rename` 原子落盘，`fsync` 后返回），
  **再**保存下一页的读取位置；位置落盘同样是临时文件 + 原子替换。
- 进程重启后，记录索引通过**重放全部已发布批次**重建，序号计数器扫描批次文件
  恢复，所以读到一半崩溃只会从上一个已确认位置继续，已发布修订不会重复生成变更。
- 下游保存的 `seq` 永远有效：读取时从首个批次文件开始扫描，按变更粒度过滤
  `seq > after_seq`，文件轮转不影响旧位置。
- charlie 窗内分页例外地以整个窗口版本为发布单位：取页中途把已读原始记录与下一页
  偏移写入 `state/journal.json`。重启时若该版本上游仍可读，就**从下一页接着取**，
  已读页不会重取；若该旧版本已被上游收回，则从当前版本的首页重取，保证一个批次
  仍只包含同一版本的完整取页。发布批次后再记窗口版本，崩溃最多导致重取（修订号
  去重使变更不重复），不会丢变更。
对应测试：`Test10_InterruptResume`、`Test6_DownstreamReadPosition`、
`Test13_CharlieInterruptResumeSameVersion`、`Test14_CharlieVersionRetiredRestartsWindow`。

### 6. 限流隔离与令牌刷新

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
