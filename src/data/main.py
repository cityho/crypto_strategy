"""for historical data"""

import os
from datetime import datetime
from binance_historical_data import BinanceDataDumper


MARKET = "um"
FILE_PATH = "data" # run cmd on project root
assert FILE_PATH and os.path.exists(FILE_PATH), \
    "Please check the path where u run the cmd, should be proj root."


def run(start: datetime.date, end:datetime.date, freq: str):
    assert freq in ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h"],\
        "check assertion condition for freq"
    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=FILE_PATH,
        asset_class="um",  # spot, um, cm
        data_type="klines",  # aggTrades, klines, trades
        data_frequency=freq,
    )
    try:
        data_dumper.dump_data(
            tickers=None,
            date_start=start,
            date_end=end
        ) # 최신 데이터는 다시 실행시 진행 가능(자동 업뎃)
    except KeyError:
        pass


if __name__ == "__main__":
    import sys
    import pandas as pd

    start = datetime.strptime(sys.argv[1], "%Y%m%d").date()
    end = datetime.strptime(sys.argv[2], "%Y%m%d").date()

    try:
        freq = sys.argv[3]
    except IndexError:
        freq = "12h"
    run(start=start, end=end, freq=freq)