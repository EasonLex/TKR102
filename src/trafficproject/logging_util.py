# src/trafficproject/logging_util.py
from datetime import datetime, timezone, timedelta
from trafficproject.paths import LOG_DIR

TPE = timezone(timedelta(hours=8))

def make_logger(name):
    path = LOG_DIR / f"{name}.log"
    def log(msg):
        line = f"[{datetime.now(TPE).isoformat(timespec='seconds')}] {msg}"
        print(line, flush=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    return log