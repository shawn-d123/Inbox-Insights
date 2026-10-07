"""Build the silver tables: cleaned and de-duplicated emails, and recipients."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from inbox_insights.cleaning import (
    clean_body,
    correct_sent_at,
    full_body,
    is_forward,
    is_reply,
    normalise_subject,
    to_houston_time,
)
from inbox_insights.frames import keep_first

# The same email often sits in several folders of one mailbox (inbox, all_documents,
# discussion_threads) and in the mailboxes of everyone it was sent to. These copies
# get different Message-IDs, so the second pass matches on content instead.
CONTENT_KEY = ["sender", "sent_at", "subject", "body_hash"]


def add_clean_columns(emails: DataFrame) -> DataFrame:
    """Add the cleaned body, threading helpers and corrected times to parsed emails."""
    sent_at = correct_sent_at(F.col("sent_at"), F.col("source_system"))
    return emails.select(
        "message_id",
        "file",
        "mailbox",
        "x_folder",
        "source_system",
        F.col("sent_at").alias("sent_at_header"),
        sent_at.alias("sent_at"),
        to_houston_time(sent_at).alias("sent_at_houston"),
        "sender",
        "to_addresses",
        "cc_addresses",
        "subject",
        normalise_subject(F.col("subject")).alias("subject_normalised"),
        is_reply(F.col("subject")).alias("is_reply"),
        is_forward(F.col("subject")).alias("is_forward"),
        clean_body(F.col("body"), F.col("transfer_encoding")).alias("body_clean"),
        full_body(F.col("body"), F.col("transfer_encoding")).alias("body_full"),
        # Hash the raw body so exact copies match even if cleaning rules change later.
        F.sha2(F.coalesce(F.col("body"), F.lit("")), 256).alias("body_hash"),
    )


def deduplicate(emails: DataFrame) -> tuple[DataFrame, dict[str, DataFrame]]:
    """
    Remove duplicates by Message-ID, then by sender + sent time + subject + body.

    The copy with the alphabetically first file path is kept, so the mailbox a
    row is attributed to stays stable between runs. Returns the de-duplicated
    frame and the frame after each pass, so the caller can record row counts
    without this function triggering any Spark actions.
    """
    by_id = keep_first(emails, ["message_id"], "file")
    by_content = keep_first(by_id, CONTENT_KEY, "file")
    return by_content, {"after_message_id": by_id, "after_content": by_content}


def _tag_addresses(column: str, recipient_type: str) -> Column:
    """Turn an array of addresses into an array of (recipient, recipient_type) structs."""
    return F.transform(
        column,
        lambda address: F.struct(
            address.alias("recipient"), F.lit(recipient_type).alias("recipient_type")
        ),
    )


def build_recipients(emails: DataFrame) -> DataFrame:
    """
    One row per (email, recipient), with recipient_type "to" or "cc".

    Bcc is left out on purpose: in this corpus the Bcc header is always a copy of
    Cc, so including it would double count every Cc recipient. If an address is
    in both To and Cc it is kept once, as "to".
    """
    tagged = F.concat(_tag_addresses("to_addresses", "to"), _tag_addresses("cc_addresses", "cc"))
    exploded = emails.select("message_id", F.explode(tagged).alias("r")).select(
        "message_id", "r.recipient", "r.recipient_type"
    )

    # "to" > "cc" alphabetically, so descending order keeps the "to" row.
    return keep_first(exploded, ["message_id", "recipient"], F.col("recipient_type").desc())
