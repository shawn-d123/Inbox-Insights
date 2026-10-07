"""Build gold.message_features: one row per email, features plus a reply label.

Two rules keep the model honest:
  * Nothing used to build the label (this email's own reply times) is a feature.
  * Every behavioural feature only looks at activity *before* the email was sent,
    so the model never sees the future.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from inbox_insights.frames import keep_first
from inbox_insights.rules import INTERNAL_DOMAIN, is_after_hours, is_meeting

REPLY_WINDOW_MINUTES = 120
LABEL_COLUMN = "label_replied_2h"

# Columns derived from the replies to an email. They define the label, so they
# must never appear among the features.
LABEL_SOURCES = {"response_minutes", "reply_sent_at", "first_reply_minutes", LABEL_COLUMN}

URGENCY_PATTERNS = {
    "has_urgent": r"(?i)\burgent\b",
    "has_asap": r"(?i)\basap\b|as soon as possible",
    "has_deadline": r"(?i)\bdeadline\b|\bby (today|tomorrow|eod|end of (the )?day)\b",
    "has_action_request": r"(?i)\bplease (respond|reply|confirm|review|advise|call|let me know)\b",
    "has_important": r"(?i)\bimportant\b",
}

FEATURE_COLUMNS = [
    # content
    "body_length",
    "word_count",
    "to_count",
    "cc_count",
    "recipient_count",
    "is_reply",
    "is_forward",
    "thread_position",
    "has_question",
    # behaviour (all point-in-time)
    "sender_is_internal",
    "sender_prior_emails",
    "prior_emails_to_recipients",
    "prior_replies_from_recipients",
    # time
    "hour_sent",
    "weekday_sent",
    "is_after_hours",
    # text flags
    "is_meeting",
    *URGENCY_PATTERNS,
]


def mailbox_owners(emails: DataFrame) -> DataFrame:
    """
    Guess each mailbox owner's address as the most frequent sender in that mailbox.

    The corpus only holds sent mail for these ~150 people, so they are the only
    recipients for whom "no reply found" really means "did not reply".
    """
    counts = emails.groupBy("mailbox", "sender").count()
    owners = keep_first(counts, ["mailbox"], F.col("count").desc(), "sender")
    return owners.select("mailbox", F.col("sender").alias("owner"))


def reply_labels(reply_pairs: DataFrame) -> DataFrame:
    """1 if the email's fastest matched reply came within the reply window, else 0."""
    fastest = reply_pairs.groupBy(F.col("original_id").alias("message_id")).agg(
        F.min("response_minutes").alias("first_reply_minutes")
    )
    return fastest.select(
        "message_id",
        (F.col("first_reply_minutes") <= REPLY_WINDOW_MINUTES).cast("int").alias(LABEL_COLUMN),
    )


def _bool_int(column: Column) -> Column:
    return F.coalesce(column, F.lit(False)).cast("int")


def content_features(emails: DataFrame) -> DataFrame:
    """Features that come from the email itself."""
    body = F.coalesce(F.col("body_clean"), F.lit(""))
    words = F.size(F.filter(F.split(body, r"\s+"), lambda w: w != ""))
    to_count = F.size("to_addresses")
    cc_count = F.size("cc_addresses")
    flag_text = F.concat_ws(" ", F.coalesce("subject", F.lit("")), body)

    # Position in the thread: how many earlier emails share the normalised subject.
    in_thread = Window.partitionBy("subject_normalised").orderBy("sent_at", "message_id")
    thread_position = F.when(F.col("subject_normalised") == "", F.lit(0)).otherwise(
        F.row_number().over(in_thread) - 1
    )

    return emails.select(
        "message_id",
        F.length(body).alias("body_length"),
        words.alias("word_count"),
        to_count.alias("to_count"),
        cc_count.alias("cc_count"),
        (to_count + cc_count).alias("recipient_count"),
        _bool_int(F.col("is_reply")).alias("is_reply"),
        _bool_int(F.col("is_forward")).alias("is_forward"),
        thread_position.alias("thread_position"),
        _bool_int(body.contains("?")).alias("has_question"),
        _bool_int(F.col("sender").endswith(INTERNAL_DOMAIN)).alias("sender_is_internal"),
        F.hour("sent_at_houston").alias("hour_sent"),
        F.dayofweek("sent_at_houston").alias("weekday_sent"),
        _bool_int(is_after_hours(F.col("sent_at_houston"))).alias("is_after_hours"),
        _bool_int(is_meeting(F.col("subject"), F.col("body_full"))).alias("is_meeting"),
        *[_bool_int(flag_text.rlike(p)).alias(name) for name, p in URGENCY_PATTERNS.items()],
        F.concat_ws(" ", F.coalesce("subject", F.lit("")), body).alias("text"),
    )


def sender_history(emails: DataFrame) -> DataFrame:
    """How many emails the sender had sent before this one."""
    earlier = Window.partitionBy("sender").orderBy("sent_at", "message_id")
    return emails.select(
        "message_id", (F.row_number().over(earlier) - 1).alias("sender_prior_emails")
    )


def contact_history(emails: DataFrame, recipients: DataFrame) -> DataFrame:
    """
    Per email, the most earlier emails the sender had sent to any one recipient.

    A proxy for "is the sender a regular contact of the recipient", computed only
    from emails that came before.
    """
    pairs = recipients.join(emails.select("message_id", "sender", "sent_at"), "message_id")
    earlier = Window.partitionBy("sender", "recipient").orderBy("sent_at", "message_id")
    return (
        pairs.withColumn("prior", F.row_number().over(earlier) - 1)
        .groupBy("message_id")
        .agg(F.max("prior").alias("prior_emails_to_recipients"))
    )


def reply_history(emails: DataFrame, recipients: DataFrame, reply_pairs: DataFrame) -> DataFrame:
    """
    Per email, how many times its recipients had already replied to this sender.

    Uses replies to *other, earlier* emails only. Emails and past replies for the
    same (recipient, sender) pair are put in one time-ordered stream and a running
    count of replies is read off at each email, which avoids a large range join.
    """
    email_events = recipients.join(emails.select("message_id", "sender", "sent_at"), "message_id")
    email_events = email_events.select(
        F.col("recipient").alias("replier"),
        F.col("sender").alias("original_sender"),
        F.col("sent_at").alias("event_at"),
        "message_id",
        F.lit(0).alias("is_past_reply"),
    )
    reply_events = reply_pairs.select(
        "replier",
        "original_sender",
        F.col("reply_sent_at").alias("event_at"),
        F.lit(None).cast("string").alias("message_id"),
        F.lit(1).alias("is_past_reply"),
    )

    # At equal timestamps emails sort before replies, so a reply sent in the same
    # second as an email is not counted as history for it.
    stream = (
        Window.partitionBy("replier", "original_sender")
        .orderBy("event_at", "is_past_reply")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        email_events.unionByName(reply_events)
        .withColumn("replies_so_far", F.sum("is_past_reply").over(stream))
        .filter(F.col("is_past_reply") == 0)
        .groupBy("message_id")
        .agg(F.sum("replies_so_far").alias("prior_replies_from_recipients"))
    )


def eligible_emails(emails: DataFrame, recipients: DataFrame) -> DataFrame:
    """Emails sent To at least one mailbox owner other than the sender."""
    owners = mailbox_owners(emails).select(F.col("owner").alias("recipient")).distinct()
    to_owner = recipients.filter(F.col("recipient_type") == "to").join(owners, "recipient")
    return (
        to_owner.join(emails.select("message_id", "sender"), "message_id")
        .filter(F.col("recipient") != F.col("sender"))
        .select("message_id")
        .distinct()
    )


def build_message_features(
    emails: DataFrame, recipients: DataFrame, reply_pairs: DataFrame
) -> DataFrame:
    """
    Assemble the feature table for emails whose replies we could observe.

    History features are computed over all emails, then the table is narrowed to
    eligible emails, so an email's history still counts activity with anyone.
    """
    assert_no_leakage(FEATURE_COLUMNS)

    email_recipient_pairs = recipients.select("message_id", "recipient")
    features = (
        content_features(emails)
        .join(sender_history(emails), "message_id")
        .join(contact_history(emails, email_recipient_pairs), "message_id", "left")
        .join(reply_history(emails, email_recipient_pairs, reply_pairs), "message_id", "left")
        .join(eligible_emails(emails, recipients), "message_id")
        .join(reply_labels(reply_pairs), "message_id", "left")
        .join(emails.select("message_id", "sender", "sent_at"), "message_id")
        .fillna(0, subset=["prior_emails_to_recipients", "prior_replies_from_recipients"])
        .fillna(0, subset=[LABEL_COLUMN])
    )
    return features.select(
        "message_id", "sender", "sent_at", *FEATURE_COLUMNS, "text", LABEL_COLUMN
    )


def assert_no_leakage(feature_columns: list[str]) -> None:
    """Fail fast if a label input has been added to the feature list."""
    leaked = LABEL_SOURCES.intersection(feature_columns)
    if leaked:
        raise ValueError(f"Label inputs used as features: {sorted(leaked)}")
