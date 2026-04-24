#!/bin/bash
# 43.165.176.100 (海外可达) 每周一 07:00 运行。
# 收集 HF + YouTube → 分析 → scp 到 111.229.73.65 供合并。

set -eu
export TZ=Asia/Shanghai

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_DIR"

mkdir -p logs
TS=$(date +%Y%m%d_%H%M%S)
LOG="logs/overseas_${TS}.log"

echo "=== overseas run @ $(date '+%F %T %Z') ===" | tee -a "$LOG"

# 1. collect + analyze on 43.165
./.venv/bin/python cli.py collect 2>&1 | tee -a "$LOG"
./.venv/bin/python cli.py analyze 2>&1 | tee -a "$LOG"

# 2. scp DB to 111.229.73.65
echo "--- scp db to 111.229 ---" | tee -a "$LOG"
if scp -i /home/ubuntu/.ssh/id_ed25519_crawler -o ConnectTimeout=15 \
     data/crawler.db ubuntu@111.229.73.65:/tmp/overseas.db 2>&1 | tee -a "$LOG"; then
  echo "=== overseas done ===" | tee -a "$LOG"
else
  echo "!!! scp failed, but data is still on 43.165" | tee -a "$LOG"
fi

# 保留 60 天日志
find logs/ -name "overseas_*.log" -mtime +60 -delete 2>/dev/null || true
