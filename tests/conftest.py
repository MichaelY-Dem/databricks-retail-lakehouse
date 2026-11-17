"""Shared pytest fixtures: a small local Spark session that behaves like Databricks serverless."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("retail-lakehouse-unit-tests")
        .config("spark.sql.ansi.enabled", "true")       # serverless runs in ANSI mode
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()