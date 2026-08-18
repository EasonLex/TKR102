import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from trafficproject.convert_to_parquet import convert_to_parquet
from trafficproject.inspect_day import load_day, health_check, plot
from trafficproject.logging_util import make_logger
from trafficproject.extra_event import load_all_stops
from trafficproject.extra_event import extract_day
from trafficproject.transfer_stats import build_transfers
from trafficproject.build_marts import build_marts

log = make_logger("daily_batch")

def run_city(city, date, all_stops):
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

    try:
        extract_day(city, date, all_stops)
    except Exception as e:
        log(f"{city} extract_day 失敗: {type(e).__name__}: {e}")
        return False

    try:
        build_transfers(city, date)
    except Exception as e:
        log(f"{city} build_transfers 失敗: {type(e).__name__}: {e}")
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
    all_stops = load_all_stops()        # 只載一次，兩個城市共用

    if len(sys.argv) > 2:
        cities = [sys.argv[1]]
        date = sys.argv[2]
    elif len(sys.argv) > 1:
        cities = [sys.argv[1]]

    log(f"=== batch start: {date} {cities} ===")

    results = [run_city(c, date, all_stops) for c in cities]
    ok = sum(results)
    failed = len(results) - ok

    if ok:
        try:
            for c in cities:
                build_marts(c)
        except Exception as e:
            log(f"marts 失敗: {type(e).__name__}: {e}")
            failed += 1

    log(f"=== batch done: {ok} ok, {failed} failed ===")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()