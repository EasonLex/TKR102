import sys
import pandas as pd

from trafficproject.logging_util import make_logger
from trafficproject.extra_event import load_all_stops
from trafficproject.build_marts import build_marts
from trafficproject.daily_batch import run_city

log = make_logger("backfill")


def main():

    if len(sys.argv) < 3:
        print("請輸入兩個日期")
        sys.exit(1)

    cities = ["Taipei", "NewTaipei"]
    dt_range = pd.date_range(start=sys.argv[1], end=sys.argv[2])    
    all_stops = load_all_stops()        # 只載一次，兩個城市共用

    all_ok = 0
    all_failed = 0
    for d in dt_range:
        date = d.strftime("%Y-%m-%d")
        log(f"=== batch start: {date} {cities} ===")

        results = [run_city(c, date, all_stops) for c in cities]
        ok = sum(results)
        failed = len(results) - ok
        all_ok += ok
        all_failed += failed
        log(f"=== batch done: {ok} ok, {failed} failed ===")

    if all_ok:
        try:
            for c in cities:
                build_marts(c)
        except Exception as e:
            log(f"marts 失敗: {type(e).__name__}: {e}")
            all_failed += 1
    if all_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()