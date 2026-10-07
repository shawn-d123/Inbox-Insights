"""Gold analytics tables: weekly volume, email categories, reply matching and people."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from inbox_insights.frames import keep_first
from inbox_insights.rules import is_after_hours, is_internal, is_meeting, is_weekend

# A reply arriving more than two weeks after the email it answers is more likely a
# reused subject line than a real response, so matches are capped at 14 days.
MAX_REPLY_WINDOW_DAYS = 14


def add_flags(emails: DataFrame) -> DataFrame:
    """Add the internal, meeting, after-hours and weekend flags to silver emails."""
    recipients = F.concat("to_addresses", "cc_addresses")
    return emails.withColumns(
        {
            "is_internal": is_internal(F.col("sender"), recipients),
            "is_meeting": is_meeting(F.col("subject"), F.col("body_full")),
            "is_after_hours": is_after_hours(F.col("sent_at_houston")),
            "is_weekend": is_weekend(F.col("sent_at_houston")),
        }
    )


def weekly_volume(flagged: DataFrame) -> DataFrame:
    """Emails per week (weeks start on Monday, Houston time), split internal/external."""
    return (
        flagged.groupBy(F.date_trunc("week", "sent_at_houston").cast("date").alias("week_start"))
        .agg(
            F.count("*").alias("total_emails"),
            F.sum(F.col("is_internal").cast("int")).alias("internal_emails"),
            F.sum((~F.col("is_internal")).cast("int")).alias("external_emails"),
        )
        .orderBy("week_start")
    )


def email_categories(flagged: DataFrame) -> DataFrame:
    """Monthly counts and shares of meeting, after-hours and weekend email."""
    total = F.count("*")
    meeting = F.sum(F.col("is_meeting").cast("int"))
    after_hours = F.sum(F.col("is_after_hours").cast("int"))
    weekend = F.sum(F.col("is_weekend").cast("int"))

    return (
        flagged.groupBy(F.date_trunc("month", "sent_at_houston").cast("date").alias("month"))
        .agg(
            total.alias("total_emails"),
            meeting.alias("meeting_emails"),
            F.round(meeting / total, 4).alias("meeting_share"),
            after_hours.alias("after_hours_emails"),
            F.round(after_hours / total, 4).alias("after_hours_share"),
            weekend.alias("weekend_emails"),
        )
        .orderBy("month")
    )


def match_replies(emails: DataFrame) -> DataFrame:
    """
    Pair each reply with the email it most likely answers.

    A reply R from person B matches an earlier email O when O was sent by someone R
    is addressed to, O was addressed to B, both share the same normalised subject,
    and O is no more than 14 days older. When several emails qualify, the most
    recent one is taken, since that is what B was actually responding to.

    Joining on (subject, sender) instead of a full self-join keeps this to one
    equi-join that Spark can shuffle efficiently.
    """
    replies = emails.filter(F.col("is_reply") & (F.col("subject_normalised") != "")).select(
        F.col("message_id").alias("reply_id"),
        F.col("sender").alias("replier"),
        F.col("sent_at").alias("reply_sent_at"),
        "subject_normalised",
        F.explode(F.array_distinct(F.concat("to_addresses", "cc_addresses"))).alias(
            "original_sender"
        ),
    )
    originals = emails.select(
        F.col("message_id").alias("original_id"),
        F.col("sender").alias("original_sender"),
        F.col("sent_at").alias("original_sent_at"),
        "subject_normalised",
        F.concat("to_addresses", "cc_addresses").alias("original_recipients"),
    )

    window_start = F.col("reply_sent_at") - F.expr(f"INTERVAL {MAX_REPLY_WINDOW_DAYS} DAYS")
    candidates = replies.join(originals, ["subject_normalised", "original_sender"]).filter(
        (F.col("original_sent_at") < F.col("reply_sent_at"))
        & (F.col("original_sent_at") >= window_start)
        & F.array_contains("original_recipients", F.col("replier"))
        & (F.col("replier") != F.col("original_sender"))
    )

    latest = keep_first(candidates, ["reply_id"], F.col("original_sent_at").desc(), "original_id")
    response_seconds = F.unix_timestamp("reply_sent_at") - F.unix_timestamp("original_sent_at")
    return latest.select(
        "reply_id",
        "original_id",
        "replier",
        "original_sender",
        "subject_normalised",
        "original_sent_at",
        "reply_sent_at",
        (response_seconds / 60).alias("response_minutes"),
    )


def person_activity(flagged: DataFrame, recipients: DataFrame, reply_pairs: DataFrame) -> DataFrame:
    """
    One row per email address: sent and received counts, after-hours share of
    sent email, and median response time over their matched replies.
    """
    sent = flagged.groupBy(F.col("sender").alias("person")).agg(
        F.count("*").alias("sent_count"),
        F.round(F.avg(F.col("is_after_hours").cast("double")), 4).alias("after_hours_sent_share"),
    )
    received = recipients.groupBy(F.col("recipient").alias("person")).agg(
        F.count("*").alias("received_count")
    )
    responses = reply_pairs.groupBy(F.col("replier").alias("person")).agg(
        F.count("*").alias("replies_matched"),
        F.round(F.median("response_minutes"), 1).alias("median_response_minutes"),
    )

    return (
        sent.join(received, "person", "full_outer")
        .join(responses, "person", "left")
        .fillna(0, subset=["sent_count", "received_count", "replies_matched"])
        .withColumn("is_enron", F.col("person").endswith("@enron.com"))
    )
