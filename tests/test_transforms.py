from datetime import datetime

from retail_lakehouse.transforms import add_event_ts, clean_str, to_num, to_ts


def test_to_ts_parses_valid_timestamp(spark):
    df = spark.createDataFrame([("2017-10-02 10:56:33",)], ["raw"])
    result = df.select(to_ts("raw").alias("ts")).first().ts
    assert result == datetime(2017, 10, 2, 10, 56, 33)


def test_to_ts_returns_null_for_bad_value_instead_of_failing(spark):
    df = spark.createDataFrame([("not a date",)], ["raw"])
    assert df.select(to_ts("raw").alias("ts")).first().ts is None


def test_to_num_handles_decimal_text_as_int(spark):
    df = spark.createDataFrame([("40.0",)], ["raw"])
    assert df.select(to_num("raw", "int").alias("n")).first().n == 40


def test_to_num_returns_null_for_bad_value_instead_of_failing(spark):
    df = spark.createDataFrame([("abc",)], ["raw"])
    assert df.select(to_num("raw").alias("n")).first().n is None


def test_clean_str_trims_whitespace(spark):
    df = spark.createDataFrame([("  sao paulo  ",)], ["raw"])
    assert df.select(clean_str("raw").alias("s")).first().s == "sao paulo"


def test_add_event_ts_picks_latest_and_ignores_nulls(spark):
    df = spark.createDataFrame(
        [(datetime(2024, 1, 1), datetime(2024, 1, 5), None)],
        "a TIMESTAMP, b TIMESTAMP, c TIMESTAMP",
    )
    result = add_event_ts(df, ["a", "b", "c"]).first()._event_ts
    assert result == datetime(2024, 1, 5)


def test_late_arriving_stale_record_has_older_event_ts(spark):
    """The Silver pipeline sorts by _event_ts, so a stale 'created' record must rank below 'delivered'."""
    purchase = datetime(2024, 1, 1, 9, 0)
    df = spark.createDataFrame(
        [
            ("delivered", purchase, datetime(2024, 1, 1, 9, 10), datetime(2024, 1, 4)),
            ("created",   purchase, None,                         None),
        ],
        "status STRING, purchased TIMESTAMP, approved TIMESTAMP, delivered TIMESTAMP",
    )
    rows = {r.status: r._event_ts for r in add_event_ts(df, ["purchased", "approved", "delivered"]).collect()}
    assert rows["delivered"] > rows["created"]


def test_add_event_ts_rejects_single_column(spark):
    import pytest
    df = spark.createDataFrame([(datetime(2024, 1, 1),)], "a TIMESTAMP")
    with pytest.raises(ValueError):
        add_event_ts(df, ["a"])