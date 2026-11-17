"""
Reusable PySpark cleaning helpers for the Silver layer.
Used by pipelines/silver_pipeline.py and unit-tested in tests/test_transforms.py.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

TS_FMT = "yyyy-MM-dd HH:mm:ss"


def to_ts(col_name: str, fmt: str = TS_FMT) -> Column:
    """Text -> timestamp. Returns NULL instead of failing on bad values (safe under ANSI mode)."""
    return F.expr(f"try_to_timestamp({col_name}, '{fmt}')")


def to_num(col_name: str, dtype: str = "double") -> Column:
    """Text -> number. Handles '40' and '40.0'. Returns NULL instead of failing on bad values."""
    return F.expr(f"try_cast({col_name} AS DOUBLE)").cast(dtype)


def clean_str(col_name: str) -> Column:
    """Trim leading/trailing whitespace."""
    return F.trim(F.col(col_name))


def add_event_ts(df: DataFrame, ts_cols: list, out_col: str = "_event_ts") -> DataFrame:
    """
    Add the latest real event time across several timestamp columns (NULLs ignored).
    Used as the sequence column so late-arriving stale records never overwrite newer ones.
    """
    if len(ts_cols) < 2:
        raise ValueError("add_event_ts needs at least 2 timestamp columns")
    return df.withColumn(out_col, F.greatest(*[F.col(c) for c in ts_cols]))