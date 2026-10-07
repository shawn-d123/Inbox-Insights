import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession shared by the whole test run, since start-up is slow."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("inbox-insights-tests")
        # Keep shuffles tiny; the default of 200 partitions makes small tests crawl.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
