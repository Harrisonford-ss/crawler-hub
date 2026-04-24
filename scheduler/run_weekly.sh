#!/bin/bash
# 111.229.73.65 (国内 always-on) 每周一 08:00 运行。
# 先合并 43.165 的海外 DB，再 collect 国内 + analyze + report + push + publish。

set -eu
export TZ=Asia/Shanghai

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_DIR"

mkdir -p logs
TS=$(date +%Y%m%d_%H%M%S)
LOG="logs/weekly_${TS}.log"

echo "=== crawler-hub weekly run @ $(date '+%F %T %Z') ===" | tee -a "$LOG"

# 0a. 合并 43.165 scp 过来的海外 DB
OVERSEAS_DB=/tmp/overseas.db
if [ -f "$OVERSEAS_DB" ]; then
  echo "--- merging overseas db ---" | tee -a "$LOG"
  ./.venv/bin/python scheduler/merge_db.py "$OVERSEAS_DB" 2>&1 | tee -a "$LOG"
  mv "$OVERSEAS_DB" "${OVERSEAS_DB}.consumed.$(date +%s)" 2>/dev/null || true
else
  echo "--- no overseas db to merge ---" | tee -a "$LOG"
fi

# 0b. 合并家用机 scp 过来的小红书 JSON
XHS_JSON=/home/ubuntu/incoming/xhs.json
if [ -f "$XHS_JSON" ]; then
  echo "--- importing xhs json ---" | tee -a "$LOG"
  ./.venv/bin/python scheduler/import_xhs.py --file "$XHS_JSON" 2>&1 | tee -a "$LOG"
else
  echo "--- no xhs json to import (家用机可能还没跑) ---" | tee -a "$LOG"
fi

run_stage() {
  local stage=$1
  echo "--- stage: $stage ---" | tee -a "$LOG"
  if ! ./.venv/bin/python cli.py "$stage" 2>&1 | tee -a "$LOG"; then
    echo "!!! stage $stage failed, continuing" | tee -a "$LOG"
  fi
}

run_stage collect
run_stage analyze
run_stage report
run_stage push
run_stage publish

echo "=== done @ $(date '+%F %T %Z') ===" | tee -a "$LOG"

find logs/ -name "weekly_*.log" -mtime +60 -delete 2>/dev/null || true
find /tmp/ -maxdepth 1 -name "overseas.db.consumed.*" -mtime +14 -delete 2>/dev/null || true
