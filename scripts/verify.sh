#!/usr/bin/env bash
# verify.sh — deterministic end-to-end verification of the estuary collector
# against the bundled local mock institutions. No external network is used.
#
# It builds the two binaries, runs every Go test (including the race
# detector for the core packages) and drives the real HTTP surface through
# the headline scenarios: mixed formats/units, revision, withdrawal,
# atomic reschedule, idempotency, bravo tail-page resume, rate-limit
# isolation, bounded bravo cursor-expiry head readback (at most six detail
# requests per round, tail-only publication, no resurrection), single-
# refresh tokens, crash/resume and downstream read positions.
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

say "3/9 首轮同步：三种分页（偏移/游标/时间窗）一次性采集"
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status) pages=\(.pages_fetched) seen=\(.records_seen) changes=\(.changes)"'
for s in alpha bravo charlie; do
  [ "$(echo "$R" | source_field "$s" status)" = "ok" ] || fail "source $s not ok"
done
TOTAL="$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)"
[ "$TOTAL" = "22" ] || fail "expected 22 initial changes, got $TOTAL"
pass "22 initial records collected across all three pagination styles"

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
SAME_BATCH="$(jq -s --arg id A-101 '[.[] | select((.changes|map(select(.sample_id==$id and .op=="withdraw"))|length)>0
                          and (.changes|map(select(.sample_id==$id and .op=="upsert"))|length)>0)] | length' \
  "$DATA/batches/batches.jsonl")"
[ "$SAME_BATCH" -ge 1 ] || fail "reschedule halves not published atomically"
pass "A-101 reschedule published as withdraw+upsert in one batch"
curl -sf "$COL_BASE/v1/records" | jq -r '.records[] | select(.sample_id=="A-101") | "  - A-101 now archived on \(.sampled_on)"'

say "6/9 幂等 + bravo 末页续读：再同步只重读上次末页（1 次明细请求）"
R="$(sync)"
EXTRA="$(echo "$R" | jq '[.sources[].changes] | add')"
[ "$EXTRA" = "0" ] || fail "rerun produced changes: $EXTRA"
SEQ_BEFORE="$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)"
BRAVO_PAGES="$(echo "$R" | source_field bravo pages_fetched)"
[ "$BRAVO_PAGES" = "1" ] || fail "bravo catch-up must re-read only the previous final page, got $BRAVO_PAGES pages"
# 末页书签：后继游标为空，续读游标落盘，跨轮/重启都可续读
jq -e '(.positions.bravo.cursor // "") == "" and (.positions.bravo.resume_cursor | length > 0)' \
  "$DATA/state/state.json" >/dev/null || fail "bravo tail bookmark missing persisted resume_cursor"
echo "  bravo 书签: $(jq -c .positions.bravo "$DATA/state/state.json")"
pass "repeat sync is a no-op (max_seq stays $SEQ_BEFORE); bravo used 1 detail request via resume cursor"

say "7/9 限流隔离：限流 alpha 时 bravo 与 charlie 继续采集"
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit?source=alpha&on=true" >/dev/null
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status)"'
[ "$(echo "$R" | source_field alpha status)" = "rate_limited" ] || fail "alpha should be rate limited"
[ "$(echo "$R" | source_field bravo status)" = "ok" ] || fail "bravo blocked by alpha"
[ "$(echo "$R" | source_field charlie status)" = "ok" ] || fail "charlie blocked by alpha"
curl -sf -X POST "$MOCK_BASE/mock/control/ratelimit?source=alpha&on=false" >/dev/null
pass "rate-limited source isolated; other sources completed"

say "8/9 游标过期：头回读有 6 次请求上限，到尾才发布且撤回不复活；令牌只刷新一次"
# 让当前共享令牌失效（模拟一代令牌到期），随后采集器的一次刷新应被两家共用
REFRESH_BEFORE="$(curl -sf "$MOCK_BASE/mock/stats" | jq -r .refresh_count)"
curl -sf -X POST "$MOCK_BASE/auth/token" -H 'Content-Type: application/json' \
  -d '{"grant_type":"refresh_token","client_id":"estuary-collector","refresh_token":"refresh-token-v1"}' >/dev/null
# 清空采集位置以强制带旧令牌重读全部来源（已发布批次保证不重复产生变更）。
# bravo 额外写入一个已过期的中段游标，重启后应触发 410 -> 从头回读。
python3 - "$DATA/state/state.json" <<'PY'
import json, sys, base64
p = sys.argv[1]
with open(p, encoding="utf-8") as f:
    doc = json.load(f)
doc["positions"] = {k: {} for k in doc.get("positions", {})}
# 指向日志第 3 行的不透明游标（与 mockserver 编码一致），稍后标记为过期
mid = base64.urlsafe_b64encode(b"bravo-cursor:3").rstrip(b"=").decode()
doc["positions"]["bravo"] = {"cursor": mid, "cursor_offset": 3}
with open(p, "w", encoding="utf-8") as f:
    json.dump(doc, f)
PY
EXPIRED_CURSOR="$(python3 -c 'import base64;print(base64.urlsafe_b64encode(b"bravo-cursor:3").rstrip(b"=").decode())')"
curl -sf -X POST "$MOCK_BASE/mock/control/cursor/expire" \
  -H 'Content-Type: application/json' -d "{\"cursor\":\"$EXPIRED_CURSOR\"}" >/dev/null
# 重启采集进程：内存里的令牌管理器回到初始态，磁盘位置与批次保留
kill "$COL_PID" >/dev/null 2>&1 || true
"$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA" >"$COL_LOG" 2>&1 &
COL_PID=$!
for _ in $(seq 1 50); do curl -sf "$COL_BASE/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
SEQ_BEFORE_EXPIRY="$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)"
R="$(sync)"
echo "$R" | jq -r '.sources[] | "  - \(.source): status=\(.status) changes=\(.changes) pages=\(.pages_fetched) restart=\(.cursor_restart // false)"'
BRAVO="$(echo "$R" | source_field bravo status)"
[ "$BRAVO" = "ok" ] || fail "bravo head readback after cursor expiry failed"
[ "$(echo "$R" | source_field bravo cursor_restart)" = "true" ] || fail "bravo should report a cursor restart"
BP="$(echo "$R" | source_field bravo pages_fetched)"
[ "$BP" -le 6 ] || fail "bravo round exceeded the 6 detail-request budget: $BP"
# 已发布旧记录全部被吸收，bravo 本轮不产生任何新变更（到尾才发布，无新修订）
[ "$(echo "$R" | source_field bravo changes)" = "0" ] || fail "head readback re-emitted already-published bravo revisions"
[ "$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)" = "$SEQ_BEFORE_EXPIRY" ] || fail "bravo published before reaching a stable tail"
# 回读后书签回到末页形态：后继游标为空 + 续读游标持久化
jq -e '(.positions.bravo.cursor // "") == "" and (.positions.bravo.resume_cursor | length > 0)' \
  "$DATA/state/state.json" >/dev/null || fail "bravo did not persist a tail resume cursor after readback"
REFRESH_AFTER="$(curl -sf "$MOCK_BASE/mock/stats" | jq -r .refresh_count)"
[ "$((REFRESH_AFTER-REFRESH_BEFORE))" = "2" ] || fail "expected manual + single collector refresh, delta=$((REFRESH_AFTER-REFRESH_BEFORE))"
pass "cursor expiry: bounded head readback, tail-only publish, resume cursor persisted; one shared token refresh"

say "9/9 下游续读：保存 next_seq 后只读到新增批次"
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

say "全部检查通过。批次文件："
ls -1 "$DATA/batches" | sed 's/^/  /'
echo
echo "可读批次示例（第一行 JSONL）："
head -1 "$DATA/batches/batches.jsonl" | jq '{seq, batch_id, changes: (.changes|length), first_change: .changes[0]}'
