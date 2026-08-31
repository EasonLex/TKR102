#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

fail() {
  curl -s -H "Title: compact failed" \
    -d "壓實失敗 $(TZ=Asia/Taipei date '+%F %H:%M')" \
    "${NTFY_URL:-}" || true
  exit 1
}
trap fail ERR

# 不帶參數 = 昨天、兩個城市；compact.py 內部有 sys.exit(1)
.venv/bin/python -m trafficproject.compact
EOF