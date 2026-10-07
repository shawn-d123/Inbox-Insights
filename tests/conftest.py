import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession shared by the whole test run, since start-up is slow."""
    # Without this, Spark launches workers with whichever "python" is first on PATH,
    # which can be a different version from the test runner and fails to start.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

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
