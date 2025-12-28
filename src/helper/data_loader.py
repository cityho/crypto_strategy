from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Sequence, Optional

import duckdb
import pandas as pd

from util import ENV, get_logger

LOGGER = get_logger("DATA_LOADER")

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _date_range(start: str, end: str) -> list[str]:
    s = _parse_date(start)
    e = _parse_date(end)
    if e < s:
        raise ValueError("end_date must be >= start_date")
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


@dataclass(frozen=True)
class DuckParquetStore:
    base_dir: Path = Path(ENV.PQ_DATA_PATH.value)
    join_keys: tuple[str, ...] = ("open_time", "close_time")

    @classmethod
    def paths_for(cls, dates: Sequence[str], field: str) -> list[str]:
        paths: list[str] = []
        for d in dates:
            p = cls.base_dir / d / f"{field}.parquet"
            if p.exists():
                paths.append(str(p))

        return paths

    def query_field(
        self,
        field: str,
        start_date: str,
        end_date: str,
        columns: Optional[Sequence[str]] = None,
        where_sql: Optional[str] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> pd.DataFrame:
        """
        특정 field parquet들을 날짜 구간으로 모아 DuckDB에서 스캔.
        where_sql은 duckdb SQL 조건절 문자열 (예: "symbol='BTCUSDT' AND ts BETWEEN ...")
        """
        dates = _date_range(start_date, end_date)
        paths = DuckParquetStore.paths_for(dates, field)
        if not paths:
            raise FileNotFoundError(
                f"No parquet files found for field='{field}' in {start_date}~{end_date}"
            )

        con = conn or duckdb.connect(database=":memory:")
        cols = ", ".join(columns) if columns else "*"
        where = f"WHERE {where_sql}" if where_sql else ""

        sql = f"""
        SELECT {cols}
        FROM parquet_scan({paths})
        {where}
        """
        return con.execute(sql).fetchdf()

    def load_ts_data(
        self,
        start_date: str,
        end_date: str,
        field: str,
        columns: Optional[Sequence[str]] = None,
        where_sql: Optional[str] = None,
        order_by_keys: bool = True,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> pd.DataFrame:
        """
        단일 field를 날짜 구간(start_date~end_date)에서만 s스캔해서 가져온다.
        - data/pq/YYYY-MM-DD/{field}.parquet 파일들을 parquet_scan(list)로 읽음
        - where_sql로 필터링 가능
        - columns로 필요한 컬럼만 선택 가능 (속도/메모리 절약)
        - order_by_keys=True면 join_keys로 정렬 (존재하는 컬럼일 때만 의미 있음)
        """
        con = conn or duckdb.connect(database=":memory:")

        dates = _date_range(start_date, end_date)
        paths = DuckParquetStore.paths_for(dates, field)
        if not paths:
            raise FileNotFoundError(
                f"No parquet files found for field='{field}' in {start_date}~{end_date}"
            )

        cols = ", ".join(columns) if columns else "*"
        where = f"WHERE {where_sql}" if where_sql else ""
        order = f"ORDER BY {', '.join(self.join_keys)}" if order_by_keys and self.join_keys else ""

        sql = f"""
        SELECT {cols}
        FROM parquet_scan({paths})
        {where}
        {order}
        """
        df = con.execute(sql).fetchdf()
        LOGGER.info(
            f"LOAD DATA: {start_date}~{end_date}, shape={df.shape} field={field}, where_sql={where_sql}"
        )
        return df

    def list_all_items(self) -> list[str]:
        if not self.base_dir.exists():
            return []
        return sorted({p.stem for p in self.base_dir.glob("*/*.parquet")})


def set_index_of(df, freq):
    df["freq"] = freq
    df["open_time"] = pd.to_datetime(df["open_time"])

    for c in ["freq", "close_time"]:
        del df[c]

    df = df.set_index("open_time")
    return df


if __name__ == "__main__":
    store = DuckParquetStore()

    df: pd.DataFrame = store.load_ts_data(
        start_date="2025-01-01",
        end_date="2025-02-01",
        field="close",
    )

    items = store.list_all_items()
    LOGGER.info(f"AVAILAVLE ITEMS: {items}")
