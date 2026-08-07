import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from trafficproject.convert_to_parquet import convert_to_parquet
from trafficproject.inspect_day import load_day, health_check, plot
from trafficproject.logging_util import make_logger

log = make_logger("daily_batch")

def run_city(city, date):
    """回傳是否成功。"""
    try:
        convert_to_parquet(city, date)
    except Exception as e:
        log(f"{city} 轉檔失敗，跳過體檢: {type(e).__name__}: {e}")
        return False

    try:
        df = load_day(city, date)
        dedup, snap = health_check(df, city, date)
        plot(dedup, snap, city, date)
    except Exception as e:
        log(f"{city} 體檢失敗: {type(e).__name__}: {e}")
        return False

    log(f"{city} 完成")
    return True


def main():
    today = datetime.now(ZoneInfo("Asia/Taipei"))
    # 
    # Default city: Taipei
    # Default date: yesterday
    # 
    cities = ["Taipei", "NewTaipei"]
    date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    if len(sys.argv) > 2:
        cities = [sys.argv[1]]
        date = sys.argv[2]
    elif len(sys.argv) > 1:
        cities = [sys.argv[1]]

    log(f"=== batch start: {date} {cities} ===")

    results = [run_city(c, date) for c in cities]
    ok = sum(results)
    failed = len(results) - ok
    
    log(f"=== batch done: {ok} ok, {failed} failed ===")

    if failed:
        sys.exit(1)

if __name__ == "__main__":
    main()