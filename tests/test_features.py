from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from inbox_insights.cleaning import is_forward, is_reply, normalise_subject
from inbox_insights.features import (
    FEATURE_COLUMNS,
    LABEL_COLUMN,
    assert_no_leakage,
    build_message_features,
    mailbox_owners,
)
from inbox_insights.gold import match_replies
from inbox_insights.silver import build_recipients

A, B = "a@enron.com", "b@enron.com"
DAY_ONE = datetime(2001, 5, 14, 10, 0)  # a Monday


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


def features_for(spark, rows):
    emails = make_emails(spark, rows)
    table = build_message_features(emails, build_recipients(emails), match_replies(emails))
    return {r.message_id: r for r in table.collect()}


@pytest.fixture
def features(spark):
    return features_for(spark, BASE_ROWS)


def test_only_emails_to_mailbox_owners_are_kept(features):
    assert set(features) == {"e1", "e2", "e3", "e4"}


def test_label_is_reply_within_two_hours(features):
    assert features["e1"][LABEL_COLUMN] == 1
    assert features["e3"][LABEL_COLUMN] == 0
    assert features["e2"][LABEL_COLUMN] == 0  # never replied to


def test_history_features_only_count_earlier_activity(features):
    first, second = features["e1"], features["e3"]

    assert (first.sender_prior_emails, second.sender_prior_emails) == (0, 1)
    assert (first.prior_emails_to_recipients, second.prior_emails_to_recipients) == (0, 1)
    # By the time of e3, b had replied to a once (e2).
    assert (first.prior_replies_from_recipients, second.prior_replies_from_recipients) == (0, 1)
    assert [features[m].thread_position for m in ("e1", "e2", "e3")] == [0, 1, 2]


def test_content_and_text_flags(features):
    first = features["e1"]

    assert first.has_question == 1
    assert first.has_urgent == 1
    assert first.word_count == 8
    assert first.recipient_count == 1
    assert first.is_reply == 0 and features["e2"].is_reply == 1
    assert first.text == "Budget Can you review the budget? It is urgent."


def test_future_emails_do_not_change_earlier_features(spark, features):
    later = DAY_ONE + timedelta(days=30)
    future_rows = BASE_ROWS + [
        ("e6", "allen-p", A, [B], later, "Budget", "Final version."),
        ("e7", "beck-s", B, [A], later + timedelta(minutes=5), "RE: Budget", "Thanks."),
    ]
    with_future = features_for(spark, future_rows)

    for message_id in ("e1", "e2", "e3", "e4"):
        before = {c: features[message_id][c] for c in FEATURE_COLUMNS}
        after = {c: with_future[message_id][c] for c in FEATURE_COLUMNS}
        assert before == after, message_id


def test_no_feature_columns_are_null(features):
    for row in features.values():
        assert all(row[c] is not None for c in FEATURE_COLUMNS)


def test_leakage_guard():
    assert_no_leakage(FEATURE_COLUMNS)
    with pytest.raises(ValueError, match="response_minutes"):
        assert_no_leakage([*FEATURE_COLUMNS, "response_minutes"])


def test_mailbox_owner_is_most_frequent_sender(spark):
    owners = {r.mailbox: r.owner for r in mailbox_owners(make_emails(spark, BASE_ROWS)).collect()}
    assert owners == {"allen-p": A, "beck-s": B}
