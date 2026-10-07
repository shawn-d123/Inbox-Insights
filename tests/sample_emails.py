"""Small hand-written silver-shaped emails shared by the feature and governance tests."""

from datetime import datetime, timedelta

from pyspark.sql import functions as F

from inbox_insights.cleaning import is_forward, is_reply, normalise_subject

A, B = "a@enron.com", "b@enron.com"
DAY_ONE = datetime(2001, 5, 14, 10, 0)  # a Monday

BASE_ROWS = [
    ("e1", "allen-p", A, [B], DAY_ONE, "Budget", "Can you review the budget? It is urgent."),
    # b replies after 30 minutes: e1 gets label 1.
    ("e2", "beck-s", B, [A], DAY_ONE + timedelta(minutes=30), "RE: Budget", "Looks fine."),
    # Next day a writes again; b takes five hours this time: label 0.
    ("e3", "allen-p", A, [B], DAY_ONE + timedelta(days=1), "Budget", "One more change."),
    ("e4", "beck-s", B, [A], DAY_ONE + timedelta(days=1, hours=5), "RE: Budget", "Done."),
    # Between two people whose mailboxes we do not hold: not eligible.
    ("e5", "allen-p", "x@aol.com", ["y@aol.com"], DAY_ONE, "Hello", "Hi there"),
]


def make_emails(spark, rows):
    """Silver-shaped emails from (id, mailbox, sender, to, sent, subject, body) tuples."""
    schema = (
        "message_id string, mailbox string, sender string, to_addresses array<string>, "
        "sent_at timestamp, subject string, body_clean string"
    )
    return (
        spark.createDataFrame(rows, schema)
        .withColumn("cc_addresses", F.array().cast("array<string>"))
        .withColumn("body_full", F.col("body_clean"))
        .withColumn("sent_at_houston", F.col("sent_at").cast("timestamp_ntz"))
        .withColumn("subject_normalised", normalise_subject(F.col("subject")))
        .withColumn("is_reply", is_reply(F.col("subject")))
        .withColumn("is_forward", is_forward(F.col("subject")))
    )
