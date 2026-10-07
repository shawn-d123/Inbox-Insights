from datetime import datetime

import pytest
from pyspark.sql import functions as F

from inbox_insights.rules import is_after_hours, is_internal, is_meeting


@pytest.mark.parametrize(
    ("subject", "body", "expected"),
    [
        ("Systems Meeting 7/18", "", True),
        ("", "Can we meet on Tuesday to go over the curves?", True),
        ("Re: budget", "Conference call moved to 3pm.", True),
        ("Offsite", "The offsite has been postponed until March.", True),
        ("Gas prices", "Call me when you get a chance.", False),
        ("Re: Wade", "Thanks for the update on the meetinghouse lease.", False),
        (None, None, False),
    ],
)
def test_is_meeting(spark, subject, body, expected):
    df = spark.createDataFrame([(subject, body)], "subject string, body string")
    assert df.select(is_meeting(F.col("subject"), F.col("body"))).first()[0] is expected


@pytest.mark.parametrize(
    ("local_time", "expected"),
    [
        (datetime(2001, 5, 14, 7, 59), True),  # Monday before 8am
        (datetime(2001, 5, 14, 8, 0), False),  # Monday 8am
        (datetime(2001, 5, 14, 18, 59), False),
        (datetime(2001, 5, 14, 19, 0), True),  # Monday 7pm
        (datetime(2001, 5, 19, 12, 0), True),  # Saturday midday
        (datetime(2001, 5, 20, 12, 0), True),  # Sunday midday
    ],
)
def test_is_after_hours(spark, local_time, expected):
    df = spark.createDataFrame([(local_time,)], "t timestamp_ntz")
    assert df.select(is_after_hours(F.col("t"))).first()[0] is expected


@pytest.mark.parametrize(
    ("sender", "recipients", "expected"),
    [
        ("a@enron.com", ["b@enron.com", "c@enron.com"], True),
        ("a@enron.com", ["b@enron.com", "c@aol.com"], False),
        ("x@aol.com", ["b@enron.com"], False),
        ("a@enron.com", [], False),
    ],
)
def test_is_internal(spark, sender, recipients, expected):
    df = spark.createDataFrame([(sender, recipients)], "s string, r array<string>")
    assert df.select(is_internal(F.col("s"), F.col("r"))).first()[0] is expected
