from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from inbox_insights.cleaning import is_reply, normalise_subject
from inbox_insights.gold import (
    add_flags,
    email_categories,
    match_replies,
    person_activity,
    weekly_volume,
)

MONDAY_10AM = datetime(2001, 5, 14, 10, 0)


def make_emails(spark, rows):
    """
    Build silver-shaped emails from (id, sender, to, cc, sent, subject, body) tuples.

    The same wall-clock value is used for sent_at and sent_at_houston, which is all
    these tests need: they only compare times with each other.
    """
    schema = (
        "message_id string, sender string, to_addresses array<string>, "
        "cc_addresses array<string>, sent_at timestamp, subject string, body_full string"
    )
    return (
        spark.createDataFrame(rows, schema)
        .withColumn("sent_at_houston", F.col("sent_at").cast("timestamp_ntz"))
        .withColumn("subject_normalised", normalise_subject(F.col("subject")))
        .withColumn("is_reply", is_reply(F.col("subject")))
    )


def email(message_id, sender, to, sent, subject="Budget", cc=(), body=""):
    return (message_id, sender, list(to), list(cc), sent, subject, body)


@pytest.fixture
def reply_scenario(spark):
    a, b, c = "a@enron.com", "b@enron.com", "c@enron.com"
    rows = [
        email("o1", a, [b], MONDAY_10AM),
        # A follow-up from A an hour later: B's reply should match this, not o1.
        email("o2", a, [b, c], MONDAY_10AM + timedelta(hours=1)),
        email("r1", b, [a], MONDAY_10AM + timedelta(hours=1, minutes=30), "RE: Budget"),
        # C replies three days later: 3 days minus 1 hour after o2.
        email("r2", c, [a], MONDAY_10AM + timedelta(days=3), "Re: budget"),
        # Reply with no matching original inside 14 days.
        email("o3", a, [b], MONDAY_10AM, "Old topic"),
        email("r3", b, [a], MONDAY_10AM + timedelta(days=20), "RE: Old topic"),
        # Reply to someone who never wrote to the replier.
        email("r4", c, [b], MONDAY_10AM + timedelta(hours=2), "RE: Budget"),
        # Replying to your own email does not count as a response.
        email("r5", a, [a], MONDAY_10AM + timedelta(hours=2), "RE: Budget"),
    ]
    return make_emails(spark, rows)


def test_match_replies_picks_latest_earlier_email(reply_scenario):
    pairs = {r.reply_id: r for r in match_replies(reply_scenario).collect()}

    assert set(pairs) == {"r1", "r2"}
    assert pairs["r1"].original_id == "o2"
    assert pairs["r1"].response_minutes == 30
    assert pairs["r2"].original_id == "o2"
    assert pairs["r2"].response_minutes == (3 * 24 - 1) * 60


def test_person_activity(spark, reply_scenario):
    flagged = add_flags(reply_scenario)
    recipients = reply_scenario.select(
        "message_id", F.explode(F.concat("to_addresses", "cc_addresses")).alias("recipient")
    )
    people = {
        r.person: r
        for r in person_activity(flagged, recipients, match_replies(reply_scenario)).collect()
    }

    assert people["a@enron.com"].sent_count == 4
    assert people["a@enron.com"].received_count == 4  # r1, r2, r3 and r5
    assert people["a@enron.com"].replies_matched == 0
    assert people["a@enron.com"].median_response_minutes is None
    assert people["b@enron.com"].median_response_minutes == 30
    assert people["c@enron.com"].replies_matched == 1


def test_weekly_volume_splits_internal_and_external(spark):
    rows = [
        email("1", "a@enron.com", ["b@enron.com"], MONDAY_10AM),
        email("2", "a@enron.com", ["x@aol.com"], MONDAY_10AM + timedelta(days=6)),  # Sunday
        email("3", "x@aol.com", ["a@enron.com"], MONDAY_10AM + timedelta(days=7)),
    ]
    weeks = weekly_volume(add_flags(make_emails(spark, rows))).collect()
    summary = [
        (str(w.week_start), w.total_emails, w.internal_emails, w.external_emails) for w in weeks
    ]

    assert summary == [
        ("2001-05-14", 2, 1, 1),
        ("2001-05-21", 1, 0, 1),
    ]


def test_email_categories_shares(spark):
    rows = [
        email("1", "a@enron.com", ["b@enron.com"], MONDAY_10AM, "Staff meeting"),
        email("2", "a@enron.com", ["b@enron.com"], MONDAY_10AM.replace(hour=21), "Prices"),
        email("3", "a@enron.com", ["b@enron.com"], datetime(2001, 5, 19, 11, 0), "Prices"),
        email("4", "a@enron.com", ["b@enron.com"], MONDAY_10AM, "Prices", body="Can we meet?"),
    ]
    month = email_categories(add_flags(make_emails(spark, rows))).first()

    assert month.total_emails == 4
    assert month.meeting_emails == 2
    assert month.meeting_share == 0.5
    assert month.after_hours_emails == 2  # 9pm Monday and Saturday
    assert month.weekend_emails == 1
