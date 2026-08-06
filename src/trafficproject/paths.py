from pathlib import Path

# src/trafficproject/paths.py → 上溯三層才是專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR     = PROJECT_ROOT / "raw"
OUTPUT_DIR  = PROJECT_ROOT / "output"
PARQUET_DIR = OUTPUT_DIR / "parquet"
REPORT_DIR  = OUTPUT_DIR / "reports"
LOG_DIR     = OUTPUT_DIR / "logs"

for d in (RAW_DIR, PARQUET_DIR, REPORT_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)