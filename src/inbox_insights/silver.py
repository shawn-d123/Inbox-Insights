"""Build the silver tables: cleaned and de-duplicated emails, and recipients."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from inbox_insights.cleaning import (
    clean_body,
    is_forward,
    is_reply,
    normalise_subject,
    to_houston_time,
)

# The same email often sits in several folders of one mailbox (inbox, all_documents,
# discussion_threads) and in the mailboxes of everyone it was sent to. These copies
# get different Message-IDs, so the second pass matches on content instead.
CONTENT_KEY = ["sender", "sent_at", "subject", "body_hash"]


def add_clean_columns(emails: DataFrame) -> DataFrame:
    """Add the cleaned body, threading helpers and Houston time to parsed emails."""
    return emails.select(
        "message_id",
        "file",
        "mailbox",
        "x_folder",
        "sent_at",
        to_houston_time(F.col("sent_at")).alias("sent_at_houston"),
        "sender",
        "to_addresses",
        "cc_addresses",
        "subject",
        normalise_subject(F.col("subject")).alias("subject_normalised"),
        is_reply(F.col("subject")).alias("is_reply"),
        is_forward(F.col("subject")).alias("is_forward"),
        clean_body(F.col("body"), F.col("transfer_encoding")).alias("body_clean"),
        # Hash the raw body so exact copies match even if cleaning rules change later.
        F.sha2(F.coalesce(F.col("body"), F.lit("")), 256).alias("body_hash"),
    )


def keep_first(df: DataFrame, keys: list[str], order_by: str = "file") -> DataFrame:
    """
    Keep one row per key, choosing the first by `order_by` so reruns are repeatable.

    Plain dropDuplicates keeps an arbitrary row, which would make the chosen file
    path (and so the mailbox) change between runs.
    """
    window = Window.partitionBy(*keys).orderBy(order_by)
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def deduplicate(emails: DataFrame) -> tuple[DataFrame, dict[str, DataFrame]]:
    """
    Remove duplicates by Message-ID, then by sender + sent time + subject + body.

    Returns the de-duplicated frame and the frame after each pass, so the caller
    can record row counts without this function triggering any Spark actions.
    """
    by_id = keep_first(emails, ["message_id"])
    by_content = keep_first(by_id, CONTENT_KEY)
    return by_content, {"after_message_id": by_id, "after_content": by_content}


def build_recipients(emails: DataFrame) -> DataFrame:
    """
    One row per (email, recipient), with recipient_type "to" or "cc".

    Bcc is left out on purpose: in this corpus the Bcc header is always a copy of
    Cc, so including it would double count every Cc recipient. If an address is
    in both To and Cc it is kept once, as "to".
    """
    def tagged(column: str, recipient_type: str):
        return F.transform(
            column,
            lambda a: F.struct(a.alias("recipient"), F.lit(recipient_type).alias("recipient_type")),
        )

    exploded = emails.select(
        "message_id",
        F.explode(F.concat(tagged("to_addresses", "to"), tagged("cc_addresses", "cc"))).alias("r"),
    ).select("message_id", "r.recipient", "r.recipient_type")

    # "to" > "cc" alphabetically, so descending order ranks the "to" row first.
    window = Window.partitionBy("message_id", "recipient").orderBy(F.col("recipient_type").desc())
    return (
        exploded.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
