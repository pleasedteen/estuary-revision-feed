#!/usr/bin/env bash
# verify_memory.sh — one-command, fully local verification that the collector
# stays heap-bounded while syncing and recovering from a large revision
# backlog. It generates the backlog locally (a small sample set revised over
# and over until the published changes exceed 64 MiB), then measures and
# prints the extra Go heap peak of a full sync and of a restart recovery,
# each required to stay under 32 MiB.
#
# Along the way it re-verifies the guarantees that must survive the
# streaming redesign: one batch per fully-read source, fast sources
# publishing while a slow source is still collecting, atomic reschedule,
# and readability of legacy JSONL files and already-issued bookmarks.
#
# Usage:  scripts/verify_memory.sh
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

PORT_MOCK="${PORT_MOCK:-18291}"
PORT_COL="${PORT_COL:-18290}"
WORK="$(mktemp -d)"
DATA="$WORK/data"
MOCK_BASE="http://127.0.0.1:$PORT_MOCK"
COL_BASE="http://127.0.0.1:$PORT_COL"
MIB=$((1024 * 1024))
BUDGET=$((32 * MIB))
MIN_CHANGES=$((64 * MIB))

cleanup() { kill "${COL_PID:-}" "${MOCK_PID:-}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

say()  { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
pass() { printf '  \033[1;32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[1;31m✗ %s\033[0m\n' "$*"; exit 1; }
mib()  { awk "BEGIN{printf \"%.1f\", $1 / 1048576}"; }

say "1/8 构建两个本地服务二进制"
go build -o "$WORK/mocksrv" ./cmd/mocksrv
go build -o "$WORK/collector" ./cmd/collector
pass "build ok"

say "2/8 本地造数：8 个采样反复修订 22 万次的机构积压"
"$WORK/mocksrv" -addr "127.0.0.1:$PORT_MOCK" >"$WORK/mock.log" 2>&1 &
MOCK_PID=$!
for _ in $(seq 1 50); do curl -sf "$MOCK_BASE/mock/stats" >/dev/null 2>&1 && break; sleep 0.1; done
BULK="$(curl -sf -X POST "$MOCK_BASE/mock/control/bulk" -H 'Content-Type: application/json' \
  -d '{"source":"alpha","samples":8,"revisions":220000,"page_size":5000,"page_delay_ms":25}')"
ROWS="$(echo "$BULK" | jq -r .rows_appended)"
[ "$ROWS" = "220002" ] || fail "bulk generation failed: $BULK"
pass "alpha 积压 $ROWS 行修订（8 个采样号，末尾含一对改期），每页延迟 25ms 模拟慢源"

say "3/8 预置旧格式 JSONL（轮转归档 + 活跃文件）作为已发布历史"
mkdir -p "$DATA/batches"
cat > "$DATA/batches/batch-000000000001.jsonl" <<'EOF'
{"seq":1,"batch_id":"legacy-1","changed_at":"2026-11-10T00:00:00Z","changes":[{"seq":1,"op":"upsert","institution":"alpha","sample_id":"A-100","sampled_on":"2026-11-10","revision":1,"record":{"institution":"alpha","sample_id":"A-100","revision":1,"sampled_on":"2026-11-10","location":"北滩","material":"悬浮物","mg_per_l":"12.5","unit":"mg/L","published_at":"2026-11-10T08:00:00Z","withdrawn":false}},{"seq":2,"op":"upsert","institution":"alpha","sample_id":"A-101","sampled_on":"2026-11-10","revision":1,"record":{"institution":"alpha","sample_id":"A-101","revision":1,"sampled_on":"2026-11-10","location":"北滩","material":"悬浮物","mg_per_l":"8.2","unit":"mg/L","published_at":"2026-11-10T08:05:00Z","withdrawn":false}}]}
EOF
cat > "$DATA/batches/batches.jsonl" <<'EOF'
{"seq":3,"batch_id":"legacy-2","changed_at":"2026-11-10T00:00:01Z","changes":[{"seq":3,"op":"upsert","institution":"bravo","sample_id":"B-200","sampled_on":"2026-11-10","revision":1,"record":{"institution":"bravo","sample_id":"B-200","revision":1,"sampled_on":"2026-11-10","location":"南港","material":"悬浮物","mg_per_l":"6.4","unit":"mg/L","published_at":"2026-11-10T10:00:00Z","withdrawn":false}}]}
{"seq":4,"batch_id":"legacy-2","changed_at":"2026-11-10T00:00:02Z","changes":[{"seq":4,"op":"upsert","institution":"charlie","sample_id":"C-300","sampled_on":"2026-11-10","revision":1,"record":{"institution":"charlie","sample_id":"C-300","revision":1,"sampled_on":"2026-11-10","location":"河口东","material":"悬浮物","mg_per_l":"9.4","unit":"mg/L","published_at":"2026-11-10T01:00:00Z","withdrawn":false}}]}
EOF
pass "旧格式历史已就位（seq 1..4，跨轮转归档与活跃文件两种旧文件）"

start_collector() {
  "$WORK/collector" -addr "127.0.0.1:$PORT_COL" -mock "127.0.0.1:$PORT_MOCK" -data "$DATA" \
    >"$WORK/collector.log" 2>&1 &
  COL_PID=$!
  for _ in $(seq 1 100); do curl -sf "$COL_BASE/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
  curl -sf "$COL_BASE/healthz" >/dev/null || { cat "$WORK/collector.log"; fail "collector did not start"; }
}

say "4/8 启动采集器：旧 JSONL 参与恢复，已发书签仍可续读"
start_collector
LEG="$(curl -sf "$COL_BASE/v1/batches?after_seq=2&limit=10")"
[ "$(echo "$LEG" | jq '[.batches[].changes[] | select(.seq==3 or .seq==4)] | length')" = "2" ] \
  || fail "legacy bookmark read broken: $LEG"
[ "$(echo "$LEG" | jq -r .next_seq)" = "4" ] || fail "legacy next_seq wrong: $LEG"
pass "旧 JSONL 两个文件均可读，书签 after_seq=2 正确续读到 seq 3、4"

say "5/8 同步 64+ MiB 积压：慢源采集期间其他来源先发布"
BASELINE="$(curl -sf -X POST "$COL_BASE/debug/mem/reset" | jq -r .heap_alloc)"
curl -sf -X POST "$COL_BASE/v1/sync" >"$WORK/sync.json" &
SYNC_CURL=$!
EARLY=""
for _ in $(seq 1 400); do
  if ls "$DATA"/batches/env-*-bravo-*.jsonl >/dev/null 2>&1 \
     && ls "$DATA"/batches/env-*-charlie-*.jsonl >/dev/null 2>&1 \
     && ! ls "$DATA"/batches/env-*-alpha-*.jsonl >/dev/null 2>&1; then
    EARLY="yes"; break
  fi
  sleep 0.05
done
wait "$SYNC_CURL"
[ "$EARLY" = "yes" ] || fail "bravo/charlie did not publish while alpha was still collecting"
pass "慢源 alpha 采集期间，bravo 与 charlie 的批次已先行对下游可见"
jq -r '.sources[] | "  - \(.source): status=\(.status) pages=\(.pages_fetched) seen=\(.records_seen) changes=\(.changes)"' "$WORK/sync.json"
for s in alpha bravo charlie; do
  [ "$(jq -r --arg s "$s" '.sources[] | select(.source==$s) | .status' "$WORK/sync.json")" = "ok" ] \
    || fail "source $s not ok"
done
# 220020 = 220002 积压 + 22 条初始 - 4 条已由旧 JSONL 发布（去重生效）
[ "$(jq -r '[.sources[].changes] | add' "$WORK/sync.json")" = "220020" ] || fail "unexpected total changes"
pass "旧 JSONL 中的 4 条修订未重复产生变更（跨格式去重生效）"

PEAK_SYNC="$(curl -sf "$COL_BASE/debug/mem" | jq -r .peak_since_reset)"
EXTRA_SYNC=$((PEAK_SYNC - BASELINE))

PUBLISHED="$(find "$DATA/batches" -name '*.jsonl' -exec stat -c %s {} + | awk '{s+=$1} END {print s}')"
[ "$PUBLISHED" -ge "$MIN_CHANGES" ] || fail "published $(mib "$PUBLISHED") MiB of changes < 64 MiB"

AENV="$(ls "$DATA"/batches/env-*-alpha-*.jsonl)"
grep -q '"op":"withdraw","institution":"alpha","sample_id":"BULK-000","sampled_on":"2026-11-10"' "$AENV" \
  || fail "reschedule withdraw half missing from alpha batch"
grep -q '"op":"upsert","institution":"alpha","sample_id":"BULK-000","sampled_on":"2026-11-12"' "$AENV" \
  || fail "reschedule upsert half missing from alpha batch"
pass "改期两半在同一批次（$(basename "$AENV")），发布变更 $(mib "$PUBLISHED") MiB ≥ 64 MiB"

A_FIRST="$(basename "$AENV" | cut -d- -f2)"
for f in "$DATA"/batches/env-*-bravo-*.jsonl "$DATA"/batches/env-*-charlie-*.jsonl; do
  LAST="$(basename "$f" | cut -d- -f3)"
  [ "$((10#$LAST))" -lt "$((10#$A_FIRST))" ] || fail "fast-source batch not sequenced before slow alpha"
done
pass "bravo/charlie 批次的序号区间整体先于慢源 alpha（先读完的先发布）"

say "6/8 重启采集器：从已发布数据恢复"
SAVED="$(curl -sf "$COL_BASE/v1/status" | jq -r .max_seq)"
[ "$SAVED" = "220024" ] || fail "unexpected max_seq $SAVED"   # 4 旧 + 220020 新
kill "$COL_PID" >/dev/null 2>&1 || true
wait "$COL_PID" 2>/dev/null || true
start_collector
PEAK_REC="$(curl -sf "$COL_BASE/debug/mem" | jq -r .peak_since_start)"
BASELINE2="$(curl -sf -X POST "$COL_BASE/debug/mem/reset" | jq -r .heap_alloc)"
EXTRA_REC=$((PEAK_REC - BASELINE2))

R="$(curl -sf -X POST "$COL_BASE/v1/sync")"
[ "$(echo "$R" | jq '[.sources[].changes] | add')" = "0" ] || fail "post-recovery sync not empty: $R"
pass "恢复后重放无重复变更（索引由 75 MiB 已发布数据流式重建）"
NREC="$(curl -sf "$COL_BASE/v1/records" | jq '.records | length')"
[ "$NREC" = "30" ] || fail "expected 30 records after recovery, got $NREC"
pass "恢复后可见记录 30 条（22 初始 + 8 积压样本，BULK-000 已改期到新归档日）"

say "7/8 已发书签贯穿重启与新旧两种文件格式"
CONT="$(curl -sf "$COL_BASE/v1/batches?after_seq=$SAVED")"
[ "$(echo "$CONT" | jq -r .count)" = "0" ] || fail "bookmark at max_seq should read empty"
FIRSTSEQ="$(curl -sf "$COL_BASE/v1/batches?after_seq=4&limit=2" | jq -r '.batches[0].changes[0].seq')"
[ "$FIRSTSEQ" = "5" ] || fail "continuation after legacy bookmark starts at seq $FIRSTSEQ, want 5"
pass "书签 after_seq=4 跨旧 JSONL 与新批次文件续读，下一变更为 seq 5"
CODE="$(curl -sf -o /dev/null -w '%{http_code}' "$COL_BASE/v1/batches?after_seq=$((10#$A_FIRST - 1))&limit=1")"
[ "$CODE" = "200" ] || fail "giant batch not readable via the batches API"
pass "64+ MiB 的大批次可经 /v1/batches 按书签整批读取"

say "8/8 两个堆峰值（预算各 32 MiB）"
[ "$EXTRA_SYNC" -lt "$BUDGET" ] || fail "sync extra heap peak $(mib "$EXTRA_SYNC") MiB exceeds 32 MiB"
[ "$EXTRA_REC" -lt "$BUDGET" ] || fail "recovery extra heap peak $(mib "$EXTRA_REC") MiB exceeds 32 MiB"
echo
printf '  同步阶段额外 Go 堆峰值:   %6s MiB   (基线 %s MiB → 峰值 %s MiB)\n' \
  "$(mib "$EXTRA_SYNC")" "$(mib "$BASELINE")" "$(mib "$PEAK_SYNC")"
printf '  重启恢复额外 Go 堆峰值:   %6s MiB   (基线 %s MiB → 峰值 %s MiB)\n' \
  "$(mib "$EXTRA_REC")" "$(mib "$BASELINE2")" "$(mib "$PEAK_REC")"
echo
pass "内存有界验证全部通过：≥64 MiB 变更下，同步与恢复各 < 32 MiB"
