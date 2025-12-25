import sys
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path


from util import search_file_list, ENV
import pandas as pd

# file명 조합 : partition 날짜, 개별 항목별로 조합
# file 구조: 칼럼: 종목, 파

TIME_COL = ['open_time', 'close_time']
FILE_COL = [ #개별로 file로 쪼개지는 항목들,
    'open', 'high', 'low', 'close',
    'volume', 'quote_volume', 'count',
    'taker_buy_volume', 'taker_buy_quote_volume'
]


def path_by_partition(d, freq):
    d = d.strftime("%Y-%m-%d")
    path_list = search_file_list(
        f"{ENV.DATA_PATH.value}/futures/um/daily/klines",
        f"**/{freq}/*{d}.csv"
    )
    print(f"{d}      : {len(path_list)}")
    return path_list


def _create_pq_by_partition(df: pd.DataFrame, d: str, c: str) -> None:
    f_name = f"{ENV.DATA_PATH.value}/pq/{d}/{c.lower()}.parquet"
    if Path(f_name).exists():
        base = pd.read_parquet(f_name)
    else:
        base = pd.DataFrame()

    for c in TIME_COL:
        df[c] = df[c].apply(
            lambda x: datetime.fromtimestamp(x / 1000, tz=timezone.utc)
        )
    data = pd.concat([base, df]).drop_duplicates()

    f_name = Path(f_name)
    f_name.parent.mkdir(parents=True, exist_ok=True)
    data.to_parquet(f_name)


def pq_by_partition(d, file_list):
    d = d.strftime("%Y-%m-%d")

    data_holder = defaultdict(pd.DataFrame)
    for f in file_list:
        df = pd.read_csv(f)

        symbol = f.parents[1].name
        for c in FILE_COL:
            if data_holder[c].empty:
                data_holder[c] = df[TIME_COL + [c]] \
                    .rename(columns={c:symbol}) \
                    .copy()
            else:
                data_holder[c] = data_holder[c].merge(
                    df[TIME_COL + [c]].rename(columns={c:symbol}),
                    on=TIME_COL
                )

    for k, v in data_holder.items():
        _create_pq_by_partition(v, d, k)


def run(start, end, freq):
    date_iter = pd.date_range(start, end)
    # import pdb; pdb.set_trace()

    for d in date_iter:
        file_list: list = path_by_partition(d, freq)
        pq_by_partition(d, file_list)


if __name__ == "__main__":
    start = datetime.strptime(sys.argv[1], "%Y%m%d")
    end = datetime.strptime(sys.argv[2], "%Y%m%d")
    try:
        freq = sys.argv[3]
    except IndexError:
        freq = "12h"
    run(start, end, freq)

# export PYTHONPATH=$(pwd) && python src/data/csv_to_pq.py 20240101 20251231