"""
end datetime을 trimming 처리하고, freq 칼럼을 추가함 (timeframe 칼럼용)
"""

import sys
from datetime import datetime

import pandas as pd

from src.helper.data_loader import DuckParquetStore

def _add_freq(df: pd.DataFrame) -> pd.DataFrame:
    td = df["close_time"] - df["open_time"]
    seconds = td.dt.total_seconds().astype(int)

    def _fmt(sec: int) -> str:
        if sec % 86400 == 0:
            return f"{sec // 86400}d"
        if sec % 3600 == 0:
            return f"{sec // 3600}h"
        if sec % 60 == 0:
            return f"{sec // 60}m"
        return f"{sec}s"

    df["freq"] = seconds.map(_fmt)
    return df


def _round_close_time(df: pd.DataFrame) -> pd.DataFrame:
    """00:59:59.999 -> 01:00:00"""
    df["close_time"] = df["close_time"].dt.ceil("s")
    df["half_open_interval"] = "right-open"
    return df


def run(start, end, item_list):
    dt_list: list[str] = [
        d.strftime("%Y-%m-%d") for d in pd.date_range(start, end)
    ]
    for it in item_list:
        paths = DuckParquetStore.paths_for(dt_list, it)
        for p in paths:
            df = pd.read_parquet(p)
            df = _round_close_time(df)
            df = _add_freq(df)
            df.to_parquet(p)


if __name__ == "__main__":
    # 원래는 이렇게 하면 안되는디
    start = datetime.strptime(sys.argv[1], "%Y%m%d").date()
    end = datetime.strptime(sys.argv[2], "%Y%m%d").date()

    items = [
        'close', 'count', 'high', 'low', 'open',
        'quote_volume', 'taker_buy_quote_volume', 'taker_buy_volume', 'volume'
    ]

    run(start, end, items)
