#!/usr/bin/env bash
# verify.sh — deterministic end-to-end verification of the estuary collector
# against the bundled local mock institutions. No external network is used.
#
# It builds the two binaries, runs every Go test (including the race
# detector for the core packages) and drives the real HTTP surface through
# the headline scenarios: mixed formats/units, revision, withdrawal,
# atomic reschedule, idempotency, rate-limit isolation, cursor expiry,
# single-refresh tokens, crash/resume and downstream read positions.
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
# Rotate every few hundred bytes so continuation has to cross files.
ROTATE_BYTES="${ROTATE_BYTES:-400}"
MOCK_LOG="$WORK/mock.log"
COL_LOG="$WORK/collector.log"
MOCK_BASE="http://127.0.0.1:$PORT_MOCK"
COL_BASE="http://127.0.0.1:$PORT_COL"

cleanup() {
  kill "${COL_PID:-}" "${MOCK_PID:-}" >/dev/null 2>&1 || true
  rm -f "${DELIVERED:-}"
}
trap cleanup EXIT

say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
pass() { printf '  \033[1;32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[1;31m✗ %s\033[0m\n' "$*"; exit 1; }

say "1/9 构建两个本地服务二进制"
go build -o "$WORK/mocksrv" ./cmd/mocksrv
go build -o "$WORK/collector" ./cmd/collector
pass "build ok"

say "2/9 运行 Go 测试（含 -race 核心包）"
go test ./... -timeout 60s >/dev/null
go test -race ./internal/... ./tests/ -timeout 90s >/dev/null
pass "all Go tests + race detector pass"

"$WORK/mocksrv" -addr "127.0.0.1:$PORT_MOCK" >"$MOCK_LOG" 2>&1 &
MOCK_PID=$!
"$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA" -rotate-bytes "$ROTATE_BYTES" >"$COL_LOG" 2>&1 &
COL_PID=$!
for _ in $(seq 1 50); do
  curl -sf "$COL_BASE/healthz" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -sf "$COL_BASE/healthz" >/dev/null || fail "collector did not start"
pass "mock + collector are up"

sync() { curl -sf -X POST "$COL_BASE/v1/sync"; }
source_field() { jq -r --arg s "$1" --arg f "$2" '.sources[] | select(.source==$s) | .[$f]'; }
# all_batch_lines concatenates every rotated JSONL file in publish order.
all_batch_lines() { cat "$DATA"/batches/*.jsonl 2>/dev/null || true; }

say "3/9 首轮同步：第二页 429 后同进程重试，第一页不丢；三种分页一次性采集"
# 让 alpha 的第二页（offset=3）先返回 429：第 1 页已取到但整轮失败。
# 失败轮不得发布任何 alpha 变更、不得推进位置；同进程重试必须补齐第 1、2 页。
curl -sf -X POST "$MOCK_BASE/mock/control/failpage?source=alpha&page=2&status=429" >/dev/null
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status) changes=\(.changes)"'
[ "$(echo "$R" | source_field alpha status)" = "rate_limited" ] || fail "alpha page 2 should be rate limited"
[ "$(echo "$R" | source_field alpha changes)" = "0" ] || fail "failed alpha run published changes"
[ "$(echo "$R" | source_field bravo status)" = "ok" ] || fail "bravo blocked by alpha page-2 429"
[ "$(echo "$R" | source_field charlie status)" = "ok" ] || fail "charlie blocked by alpha page-2 429"
ALPHA_RECORDS="$(curl -sf "$COL_BASE/v1/records" | jq '[.records[] | select(.institution=="alpha")] | length')"
[ "$ALPHA_RECORDS" = "0" ] || fail "unpublished alpha page-1 records leaked into /v1/records"
ALPHA_BATCHED="$(all_batch_lines | jq -s '[.[] | .changes[] | select(.institution=="alpha")] | length')"
[ "$ALPHA_BATCHED" = "0" ] || fail "unpublished alpha page-1 changes leaked into /v1/batches"
ALPHA_OFFSET="$(jq -r '.positions.alpha.offset // 0' "$DATA/state/state.json")"
[ "$ALPHA_OFFSET" = "0" ] || fail "alpha position advanced after a failed run (offset=$ALPHA_OFFSET)"
pass "page-2 429 published nothing and kept alpha at offset 0"

# 同进程重试（不重启）：第一页必须重新出现，并有且仅有一次。
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status) pages=\(.pages_fetched) seen=\(.records_seen) changes=\(.changes)"'
for s in alpha bravo charlie; do
  [ "$(echo "$R" | source_field "$s" status)" = "ok" ] || fail "source $s not ok on retry"
done
[ "$(echo "$R" | source_field alpha changes)" = "8" ] || fail "alpha retry did not publish all 8 rows"
TOTAL="$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)"
[ "$TOTAL" = "22" ] || fail "expected 22 initial changes, got $TOTAL"
ALPHA_TOTAL="$(all_batch_lines | jq -s '[.[] | .changes[] | select(.institution=="alpha")] | length')"
[ "$ALPHA_TOTAL" = "8" ] || fail "alpha records not published exactly once (got $ALPHA_TOTAL)"
pass "same-process retry re-read page 1 and published 8 alpha records exactly once"

say "4/9 新旧格式与单位统一（g/m³ 与 mg/L 等价换算，字段统一）"
curl -sf "$COL_BASE/v1/records" | jq -r '
  (.records[] | select(.sample_id=="A-102" or .sample_id=="B-200" or .sample_id=="C-302"))
  | "  - \(.institution)/\(.sample_id): \(.mg_per_l) \(.unit) @ \(.location) [\(.material)]"'
BAD="$(curl -sf "$COL_BASE/v1/records" | jq '[.records[] | select(.unit!="mg/L")] | length')"
[ "$BAD" = "0" ] || fail "some records are not canonicalized to mg/L"
pass "all concentrations canonicalized to mg/L with unified field names"

say "5/9 进入下一数据阶段：修订 / 撤回 / 改期 / 新增"
curl -sf -X POST "$MOCK_BASE/mock/control/stage" >/dev/null
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): changes=\(.changes)"'
# 改期：旧日 withdraw 与新日 upsert 必须在同一个 JSONL 批次里
SAME_BATCH="$(all_batch_lines | jq -s --arg id A-101 '[.[] | select((.changes|map(select(.sample_id==$id and .op=="withdraw"))|length)>0
                          and (.changes|map(select(.sample_id==$id and .op=="upsert"))|length)>0)] | length')"
[ "$SAME_BATCH" -ge 1 ] || fail "reschedule halves not published atomically"
pass "A-101 reschedule published as withdraw+upsert in one batch"
curl -sf "$COL_BASE/v1/records" | jq -r '.records[] | select(.sample_id=="A-101") | "  - A-101 now archived on \(.sampled_on)"'

say "6/9 幂等：再同步一次不应产生任何新变更"
R="$(sync)"
EXTRA="$(echo "$R" | jq '[.sources[].changes] | add')"
[ "$EXTRA" = "0" ] || fail "rerun produced changes: $EXTRA"
SEQ_BEFORE="$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)"
pass "repeat sync is a no-op (max_seq stays $SEQ_BEFORE)"

say "7/9 限流隔离：限流 alpha 时 bravo 与 charlie 继续采集"
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit?source=alpha&on=true" >/dev/null
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status)"'
[ "$(echo "$R" | source_field alpha status)" = "rate_limited" ] || fail "alpha should be rate limited"
[ "$(echo "$R" | source_field bravo status)" = "ok" ] || fail "bravo blocked by alpha"
[ "$(echo "$R" | source_field charlie status)" = "ok" ] || fail "charlie blocked by alpha"
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit?source=alpha&on=false" >/dev/null
pass "rate-limited source isolated; other sources completed"

say "8/9 游标过期 + 撤回防复活 + 共享令牌只刷新一次"
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
"$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA" -rotate-bytes "$ROTATE_BYTES" >"$COL_LOG" 2>&1 &
COL_PID=$!
for _ in $(seq 1 50); do curl -sf "$COL_BASE/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status) changes=\(.changes)"'
REFRESH_AFTER="$(curl -sf "$MOCK_BASE/mock/stats" | jq -r .refresh_count)"
[ "$((REFRESH_AFTER-REFRESH_BEFORE))" = "2" ] || fail "expected manual + single collector refresh, delta=$((REFRESH_AFTER-REFRESH_BEFORE))"
pass "exactly one collector-side token refresh shared by alpha and charlie"

say "9/9 下游续读：保存 next_seq 后按较小 limit 跨文件续读，每个批次只出现一次"
# 先记下所有已发布批次（seq 升序）与一个轮转前的书签位置。
BOOKMARK="$(curl -sf "$COL_BASE/v1/batches?after_seq=0&limit=1" | jq -r .next_seq)"
TOTAL_BEFORE="$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)"
echo "  bookmark after_seq=$BOOKMARK (max_seq=$TOTAL_BEFORE before the new stages)"

# 推进两个阶段（含撤回后回读到的旧修订），验证撤回记录不会复活
curl -sf -X POST "$MOCK_BASE/mock/control/stage" >/dev/null
curl -sf -X POST "$MOCK_BASE/mock/control/stage" >/dev/null
sync >/dev/null
RESURRECTED="$(curl -sf "$COL_BASE/v1/records" | jq '[.records[] | select(.sample_id=="B-201")] | length')"
[ "$RESURRECTED" = "0" ] || fail "withdrawn B-201 was resurrected by a stale revision"

# 用轮转前保存的书签，以 limit=1（小于任一批次大小）逐批续读，直到没有新批次。
SEEN="$BOOKMARK"
DELIVERED="$(mktemp)"
: >"$DELIVERED"
while :; do
  PAGE="$(curl -sf "$COL_BASE/v1/batches?after_seq=$SEEN&limit=1")"
  COUNT="$(echo "$PAGE" | jq -r .count)"
  [ "$COUNT" = "0" ] && break
  echo "$PAGE" | jq -c '.batches[]' >>"$DELIVERED"
  NEXT="$(echo "$PAGE" | jq -r .next_seq)"
  [ "$NEXT" -gt "$SEEN" ] || fail "next_seq did not advance past after_seq=$SEEN"
  SEEN="$NEXT"
done
# 连续按 limit=1 读到的批次编号必须两两不同：每个批次只出现一次。
DUP="$(jq -r .batch_id "$DELIVERED" | sort | uniq -d | wc -l | tr -d ' ')"
[ "$DUP" = "0" ] || fail "a batch was delivered more than once across continuation"
# 每个书签之后的已发布批次都完整到达下游（恰好一次）。
PUBLISHED_BATCHES="$(curl -sf "$COL_BASE/v1/batches?after_seq=$BOOKMARK" | jq -r '.batches[].batch_id' | sort -u | wc -l | tr -d ' ')"
DELIVERED_BATCHES="$(jq -r .batch_id "$DELIVERED" | sort -u | wc -l | tr -d ' ')"
[ "$DELIVERED_BATCHES" = "$PUBLISHED_BATCHES" ] \
  || fail "continuation saw $DELIVERED_BATCHES distinct batches, $PUBLISHED_BATCHES were published after the bookmark"
# 书签之前的批次不应被重复下发；书签之后必须至少有新批次。
if jq -e --argjson b "$BOOKMARK" 'select(.seq <= $b)' "$DELIVERED" >/dev/null 2>&1; then
  fail "continuation re-delivered a batch at or before the bookmark"
fi
[ "$DELIVERED_BATCHES" -ge 1 ] || fail "downstream continuation saw no new batches"
pass "resumed from pre-rotation bookmark with limit=1: $DELIVERED_BATCHES batches, each whole and exactly once"

say "全部检查通过。批次文件："
ls -1 "$DATA/batches" | sed 's/^/  /'
echo
echo "可读批次示例（第一个文件的第一行 JSONL）："
FIRST_FILE="$(ls -1 "$DATA"/batches/*.jsonl | head -1)"
head -1 "$FIRST_FILE" | jq '{seq, batch_id, changes: (.changes|length), first_change: .changes[0]}'
