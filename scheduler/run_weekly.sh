#!/bin/bash
# 每周一 08:00 (Asia/Shanghai) 由 cron 触发。
# 把整个 pipeline 串起来跑完：collect → analyze → report → push → publish
# 所有输出写到 logs/weekly_<timestamp>.log，cron 本身只管触发。

set -eu
export TZ=Asia/Shanghai

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_DIR"

mkdir -p logs
TS=$(date +%Y%m%d_%H%M%S)
LOG="logs/weekly_${TS}.log"

echo "=== crawler-hub weekly run @ $(date '+%Y-%m-%d %H:%M:%S %Z') ===" | tee -a "$LOG"

run_stage() {
  local stage=$1
  echo "--- stage: $stage ---" | tee -a "$LOG"
  if ! ./.venv/bin/python cli.py "$stage" 2>&1 | tee -a "$LOG"; then
    echo "!!! stage $stage failed, continuing anyway" | tee -a "$LOG"
  fi
}

run_stage collect
run_stage analyze
run_stage report
run_stage push
run_stage publish

echo "=== done @ $(date '+%Y-%m-%d %H:%M:%S %Z') ===" | tee -a "$LOG"

# 保留最近 8 周的日志，其余清理
find logs/ -name "weekly_*.log" -mtime +60 -delete 2>/dev/null || true
