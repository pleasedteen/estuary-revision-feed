#!/usr/bin/env bash
# verify.sh — deterministic end-to-end verification of the estuary collector
# against the bundled local mock institutions. No external network is used.
#
# It builds the two binaries, runs every Go test (including the race
# detector for the core packages) and drives the real HTTP surface through
# the headline scenarios: mixed formats/units, revision, withdrawal,
# atomic reschedule, idempotency, rate-limit isolation, cursor expiry,
# single-refresh tokens, crash/resume, downstream read positions, and the
# per-round run journal (query while a source is stuck, second-page rate
# limiting, stops at the publish/checkpoint persistence boundaries).
#
# Runtime target: well under 80 seconds on a modest machine (dependency
# installation excluded). Usage:  scripts/verify.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Locate Go: prefer PATH, then the local toolchain used by the project setup.
if ! command -v go >/dev/null 2>&1; then
  for cand in /tmp/go-install/go/bin/go /usr/local/go/bin/go; do
    [ -x "$cand" ] && PATH="$(dirname "$cand"):$PATH" && break
  done
fi
command -v go >/dev/null 2>&1 || { echo "go toolchain not found"; exit 1; }

PORT_MOCK="${PORT_MOCK:-18191}"
PORT_COL="${PORT_COL:-18190}"
WORK="$(mktemp -d)"
DATA="$WORK/data"
MOCK_LOG="$WORK/mock.log"
COL_LOG="$WORK/collector.log"
MOCK_BASE="http://127.0.0.1:$PORT_MOCK"
COL_BASE="http://127.0.0.1:$PORT_COL"

cleanup() {
  kill "${COL_PID:-}" "${MOCK_PID:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
pass() { printf '  \033[1;32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[1;31m✗ %s\033[0m\n' "$*"; exit 1; }

say "1/10 构建两个本地服务二进制"
go build -o "$WORK/mocksrv" ./cmd/mocksrv
go build -o "$WORK/collector" ./cmd/collector
pass "build ok"

say "2/10 运行 Go 测试（含 -race 核心包）"
go test ./... -timeout 60s >/dev/null
go test -race ./internal/... ./tests/ -timeout 90s >/dev/null
pass "all Go tests + race detector pass"

"$WORK/mocksrv" -addr "127.0.0.1:$PORT_MOCK" >"$MOCK_LOG" 2>&1 &
MOCK_PID=$!
"$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA" >"$COL_LOG" 2>&1 &
COL_PID=$!
for _ in $(seq 1 50); do
  curl -sf "$COL_BASE/healthz" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -sf "$COL_BASE/healthz" >/dev/null || fail "collector did not start"
pass "mock + collector are up"

sync() { curl -sf -X POST "$COL_BASE/v1/sync"; }
source_field() { jq -r --arg s "$1" --arg f "$2" '.sources[] | select(.source==$s) | .[$f]'; }

say "3/10 首轮同步：三种分页（偏移/游标/时间窗）一次性采集"
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status) pages=\(.pages_fetched) seen=\(.records_seen) changes=\(.changes)"'
for s in alpha bravo charlie; do
  [ "$(echo "$R" | source_field "$s" status)" = "ok" ] || fail "source $s not ok"
done
TOTAL="$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)"
[ "$TOTAL" = "22" ] || fail "expected 22 initial changes, got $TOTAL"
pass "22 initial records collected across all three pagination styles"

say "4/10 新旧格式与单位统一（g/m³ 与 mg/L 等价换算，字段统一）"
curl -sf "$COL_BASE/v1/records" | jq -r '
  (.records[] | select(.sample_id=="A-102" or .sample_id=="B-200" or .sample_id=="C-302"))
  | "  - \(.institution)/\(.sample_id): \(.mg_per_l) \(.unit) @ \(.location) [\(.material)]"'
BAD="$(curl -sf "$COL_BASE/v1/records" | jq '[.records[] | select(.unit!="mg/L")] | length')"
[ "$BAD" = "0" ] || fail "some records are not canonicalized to mg/L"
pass "all concentrations canonicalized to mg/L with unified field names"

say "5/10 进入下一数据阶段：修订 / 撤回 / 改期 / 新增"
curl -sf -X POST "$MOCK_BASE/mock/control/stage" >/dev/null
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): changes=\(.changes)"'
# 改期：旧日 withdraw 与新日 upsert 必须在同一个 JSONL 批次里
SAME_BATCH="$(jq -s --arg id A-101 '[.[] | select((.changes|map(select(.sample_id==$id and .op=="withdraw"))|length)>0
                          and (.changes|map(select(.sample_id==$id and .op=="upsert"))|length)>0)] | length' \
  "$DATA/batches/batches.jsonl")"
[ "$SAME_BATCH" -ge 1 ] || fail "reschedule halves not published atomically"
pass "A-101 reschedule published as withdraw+upsert in one batch"
curl -sf "$COL_BASE/v1/records" | jq -r '.records[] | select(.sample_id=="A-101") | "  - A-101 now archived on \(.sampled_on)"'

say "6/10 幂等：再同步一次不应产生任何新变更"
R="$(sync)"
EXTRA="$(echo "$R" | jq '[.sources[].changes] | add')"
[ "$EXTRA" = "0" ] || fail "rerun produced changes: $EXTRA"
SEQ_BEFORE="$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)"
pass "repeat sync is a no-op (max_seq stays $SEQ_BEFORE)"

say "7/10 限流隔离：限流 alpha 时 bravo 与 charlie 继续采集"
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit?source=alpha&on=true" >/dev/null
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status)"'
[ "$(echo "$R" | source_field alpha status)" = "rate_limited" ] || fail "alpha should be rate limited"
[ "$(echo "$R" | source_field bravo status)" = "ok" ] || fail "bravo blocked by alpha"
[ "$(echo "$R" | source_field charlie status)" = "ok" ] || fail "charlie blocked by alpha"
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit?source=alpha&on=false" >/dev/null
pass "rate-limited source isolated; other sources completed"

say "8/10 游标过期 + 撤回防复活 + 共享令牌只刷新一次"
# 让当前共享令牌失效（模拟一代令牌到期），随后采集器的一次刷新应被两家共用
REFRESH_BEFORE="$(curl -sf "$MOCK_BASE/mock/stats" | jq -r .refresh_count)"
curl -sf -X POST "$MOCK_BASE/auth/token" -H 'Content-Type: application/json' \
  -d '{"grant_type":"refresh_token","client_id":"estuary-collector","refresh_token":"refresh-token-v1"}' >/dev/null
# 清空采集位置以强制带旧令牌重读全部来源（已发布批次保证不重复产生变更）
python3 - "$DATA/state/state.json" <<'PY'
import json, sys
p = sys.argv[1]
with open(p, encoding="utf-8") as f:
    doc = json.load(f)
doc["positions"] = {k: {} for k in doc.get("positions", {})}
with open(p, "w", encoding="utf-8") as f:
    json.dump(doc, f)
PY
# 重启采集进程：内存里的令牌管理器回到初始态，磁盘位置与批次保留
kill "$COL_PID" >/dev/null 2>&1 || true
"$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA" >"$COL_LOG" 2>&1 &
COL_PID=$!
for _ in $(seq 1 50); do curl -sf "$COL_BASE/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status) changes=\(.changes)"'
REFRESH_AFTER="$(curl -sf "$MOCK_BASE/mock/stats" | jq -r .refresh_count)"
[ "$((REFRESH_AFTER-REFRESH_BEFORE))" = "2" ] || fail "expected manual + single collector refresh, delta=$((REFRESH_AFTER-REFRESH_BEFORE))"
pass "exactly one collector-side token refresh shared by alpha and charlie"

say "9/10 下游续读：保存 next_seq 后只读到新增批次"
FIRST="$(curl -sf "$COL_BASE/v1/batches?after_seq=0&limit=1")"
SAVED="$(echo "$FIRST" | jq -r .next_seq)"
PAGE="$(curl -sf "$COL_BASE/v1/batches?after_seq=$SAVED")"
echo "  saved position after_seq=$SAVED -> next read returns $(echo "$PAGE" | jq -r '.count') envelope(s), next_seq=$(echo "$PAGE" | jq -r .next_seq)"
# 推进两个阶段（含撤回后回读到的旧修订），验证撤回记录不会复活
curl -sf -X POST "$MOCK_BASE/mock/control/stage" >/dev/null
curl -sf -X POST "$MOCK_BASE/mock/control/stage" >/dev/null
sync >/dev/null
RESURRECTED="$(curl -sf "$COL_BASE/v1/records" | jq '[.records[] | select(.sample_id=="B-201")] | length')"
[ "$RESURRECTED" = "0" ] || fail "withdrawn B-201 was resurrected by a stale revision"
NEW_BATCHES="$(curl -sf "$COL_BASE/v1/batches?after_seq=$SAVED" | jq -r .count)"
[ "$NEW_BATCHES" -ge 1 ] || fail "downstream continuation saw no new batches"
pass "downstream resumed from saved position; withdrawn record stays gone"

say "10/10 运行可观测：按轮次查询、来源卡住可查、第二页限流、落盘边界停机"

# 查询助手：按轮次取某来源的日志字段（tostring：缺失字段输出 "null"）
run_field() { # ROUND SOURCE FIELD
  curl -sf "$COL_BASE/v1/sync/runs?round=$1" | jq -r --arg s "$2" --arg f "$3" '.run.sources[] | select(.source==$s) | .[$f] | tostring'
}
latest_field() { # SOURCE FIELD —— 最新一轮
  curl -sf "$COL_BASE/v1/sync/runs" | jq -r --arg s "$1" --arg f "$2" '.runs[0].sources[] | select(.source==$s) | .[$f] | tostring'
}
round_status() { curl -sf "$COL_BASE/v1/sync/runs?round=$1" | jq -r '.run.status'; }
current_round() { curl -sf "$COL_BASE/v1/sync/runs" | jq -r '.current_round'; }
wait_collector() {
  for _ in $(seq 1 50); do curl -sf "$COL_BASE/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
  curl -sf "$COL_BASE/healthz" >/dev/null || fail "collector did not start"
}

# --- 10a：某来源卡住时，其他来源仍能查 ---
curl -sf -X POST "$MOCK_BASE/mock/control/hang?source=alpha&on=true" >/dev/null
curl -sf -X POST "$COL_BASE/v1/sync" >/dev/null 2>&1 &
BG_SYNC=$!
for _ in $(seq 1 100); do
  if [ "$(latest_field bravo status)" = "ok" ] && [ "$(latest_field charlie status)" = "ok" ]; then break; fi
  sleep 0.05
done
[ "$(latest_field alpha status)" = "running" ] || fail "alpha should be running (stuck in mock)"
[ "$(latest_field alpha stage)" = "fetch" ] || fail "alpha should be stuck in the fetch stage"
[ "$(latest_field alpha pages_fetched)" = "0" ] || fail "stuck alpha should have fetched 0 pages"
[ "$(latest_field bravo status)" = "ok" ] || fail "bravo should finish while alpha is stuck"
[ "$(latest_field charlie status)" = "ok" ] || fail "charlie should finish while alpha is stuck"
[ "$(latest_field bravo position_saved)" = "true" ] || fail "bravo position should be saved"
curl -sf -X POST "$MOCK_BASE/mock/control/hang?source=alpha&on=false" >/dev/null
wait "$BG_SYNC" || true
[ "$(latest_field alpha status)" = "ok" ] || fail "alpha should recover once the hang is lifted"
pass "source stuck: run query keeps answering, other sources show done"

# --- 10b：第二页限流（失败阶段 + 已抓页数可见） ---
# 让 alpha 从头重读（已发布批次保证不重复产生变更），从第二页开始 429
python3 - "$DATA/state/state.json" <<'PY'
import json, sys
p = sys.argv[1]
with open(p, encoding="utf-8") as f:
    doc = json.load(f)
doc["positions"]["alpha"] = {}
with open(p, "w", encoding="utf-8") as f:
    json.dump(doc, f)
PY
# 位置文件只在启动时读取，重启采集进程让清空的 alpha 位置生效
kill "$COL_PID" >/dev/null 2>&1 || true
"$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA" >"$COL_LOG" 2>&1 &
COL_PID=$!
wait_collector
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=alpha&after=1" >/dev/null
CODE="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$COL_BASE/v1/sync")"
[ "$CODE" = "206" ] || fail "expected 206 with throttled alpha, got $CODE"
ROUND="$(current_round)"
[ "$(run_field "$ROUND" alpha status)" = "rate_limited" ] || fail "alpha should be rate_limited"
[ "$(run_field "$ROUND" alpha stage)" = "fetch" ] || fail "alpha failure stage should be fetch"
[ "$(run_field "$ROUND" alpha pages_fetched)" = "1" ] || fail "alpha should have fetched exactly 1 page"
[ "$(run_field "$ROUND" alpha batch_id)" = "null" ] || fail "alpha must not publish"
[ "$(run_field "$ROUND" alpha position_saved)" = "false" ] || fail "alpha position must not be saved"
[ "$(run_field "$ROUND" bravo status)" = "ok" ] || fail "bravo blocked by alpha throttle"
[ "$(run_field "$ROUND" charlie status)" = "ok" ] || fail "charlie blocked by alpha throttle"
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=alpha&after=-1" >/dev/null
sync >/dev/null
[ "$(latest_field alpha status)" = "ok" ] || fail "alpha should recover after quota lifted"
pass "second-page rate limit: failure stage + fetched pages visible, others unaffected"

# --- 10c：落盘边界停机 —— 已发布但续采位置没保存 ---
# 全新数据目录 + 模拟源复位；限流 alpha/charlie 使 bravo 成为唯一发布者（确定性停机）
curl -sf -X POST "$MOCK_BASE/mock/control/reset" >/dev/null
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=alpha&after=0" >/dev/null
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=charlie&after=0" >/dev/null
kill "$COL_PID" >/dev/null 2>&1 || true
DATA2="$WORK/data2"
ESTUARY_TEST_CRASH_AFTER=publish:bravo "$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA2" >"$COL_LOG" 2>&1 &
COL_PID=$!
wait_collector
curl -sf -X POST "$COL_BASE/v1/sync" >/dev/null 2>&1 || true # 进程在发布边界退出
sleep 0.3
if curl -sf "$COL_BASE/healthz" >/dev/null 2>&1; then fail "collector should have exited at the publish boundary"; fi
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=alpha&after=-1" >/dev/null
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=charlie&after=-1" >/dev/null
"$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA2" >"$COL_LOG" 2>&1 &
COL_PID=$!
wait_collector
[ "$(round_status 1)" = "interrupted" ] || fail "round 1 should be interrupted, got $(round_status 1)"
[ "$(run_field 1 bravo stage)" = "publish" ] || fail "bravo should stop at the publish boundary"
BID="$(run_field 1 bravo batch_id)"
[ "$BID" != "null" ] || fail "bravo batch should be journaled"
[ "$(run_field 1 bravo batch_changes)" = "7" ] || fail "bravo batch should carry 7 changes"
[ "$(run_field 1 bravo position_saved)" = "false" ] || fail "bravo position must not be saved"
# 磁盘上：批次已发布、下游可续读，但 bravo 的续采位置从未保存
if [ -f "$DATA2/state/state.json" ]; then
  jq -e '.positions.bravo == null' "$DATA2/state/state.json" >/dev/null || fail "bravo position should be absent on disk"
fi
jq -se --arg bid "$BID" 'map(select(.batch_id==$bid)) | length == 1' "$DATA2/batches/batches.jsonl" >/dev/null || fail "journaled batch must be on disk"
[ "$(curl -sf "$COL_BASE/v1/batches?after_seq=0" | jq -r .count)" = "1" ] || fail "downstream should read the published batch"
# 下一轮：bravo 从头重读，已发布修订被吸收，不重复产生变更
R="$(sync)"
[ "$(echo "$R" | source_field bravo changes)" = "0" ] || fail "bravo re-run must not re-emit changes"
[ "$(echo "$R" | source_field alpha changes)" = "8" ] || fail "alpha should publish its 8 records"
pass "stopped after publish: batch durable + journaled, position unsaved, no duplicates on resume"

# --- 10d：落盘边界停机 —— 位置已保存；空轮重启后仍能判断 ---
curl -sf -X POST "$MOCK_BASE/mock/control/stage" >/dev/null # bravo 新增 4 条
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=alpha&after=0" >/dev/null
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=charlie&after=0" >/dev/null
kill "$COL_PID" >/dev/null 2>&1 || true
ESTUARY_TEST_CRASH_AFTER=checkpoint:bravo "$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA2" >"$COL_LOG" 2>&1 &
COL_PID=$!
wait_collector
curl -sf -X POST "$COL_BASE/v1/sync" >/dev/null 2>&1 || true # 进程在位置保存后退出
sleep 0.3
if curl -sf "$COL_BASE/healthz" >/dev/null 2>&1; then fail "collector should have exited at the checkpoint boundary"; fi
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=alpha&after=-1" >/dev/null
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit_page?source=charlie&after=-1" >/dev/null
"$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA2" >"$COL_LOG" 2>&1 &
COL_PID=$!
wait_collector
[ "$(round_status 3)" = "interrupted" ] || fail "round 3 should be interrupted, got $(round_status 3)"
[ "$(run_field 3 bravo stage)" = "checkpoint" ] || fail "bravo should stop at the checkpoint boundary"
[ "$(run_field 3 bravo batch_changes)" = "4" ] || fail "bravo should have published 4 stage-1 changes"
[ "$(run_field 3 bravo position_saved)" = "true" ] || fail "bravo position should be saved"
R="$(sync)"
[ "$(echo "$R" | source_field bravo changes)" = "0" ] || fail "bravo must not re-emit after saved checkpoint"
pass "stopped after checkpoint: position saved, resume emits nothing twice"

# 空轮：没有新批次、续采位置没变化，重启后仍能看出这轮已正常完成
R="$(sync)"
[ "$(echo "$R" | jq '[.sources[].changes] | add')" = "0" ] || fail "expected a change-free round"
EMPTY_ROUND="$(current_round)"
kill "$COL_PID" >/dev/null 2>&1 || true
"$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA2" >"$COL_LOG" 2>&1 &
COL_PID=$!
wait_collector
[ "$(current_round)" = "$EMPTY_ROUND" ] || fail "round numbering must survive restarts"
[ "$(round_status "$EMPTY_ROUND")" = "done" ] || fail "empty round should be done"
for s in alpha bravo charlie; do
  [ "$(run_field "$EMPTY_ROUND" "$s" position_saved)" = "true" ] || fail "$s position should be saved in the empty round"
  [ "$(run_field "$EMPTY_ROUND" "$s" batch_id)" = "null" ] || fail "$s should have no batch in the empty round"
done
pass "change-free round still shows done + position saved after restart (round $EMPTY_ROUND)"

say "全部检查通过。批次文件："
ls -1 "$DATA/batches" | sed 's/^/  /'
echo
echo "可读批次示例（第一行 JSONL）："
head -1 "$DATA/batches/batches.jsonl" | jq '{seq, batch_id, changes: (.changes|length), first_change: .changes[0]}'
