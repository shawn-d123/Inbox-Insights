"""Parse raw Enron messages into typed columns using built-in Spark functions.

Each raw message is an RFC 822 style block: a set of headers, a blank line, then
the body. Everything here is done with Spark SQL functions rather than Python
UDFs so it scales to the full corpus without serialisation overhead.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Enron dates look like "Mon, 14 May 2001 16:39:00 -0700 (PDT)". After the day
# name and the "(PDT)" suffix are stripped, one of these formats should match.
_DATE_FORMATS_WITH_OFFSET = ["d MMM yyyy HH:mm:ss Z", "d MMM yyyy HH:mm Z"]
_DATE_FORMATS_LOCAL = ["d MMM yyyy HH:mm:ss", "d MMM yyyy HH:mm"]


def split_headers_and_body(message: Column) -> tuple[Column, Column]:
    """
    Split a raw message on the first blank line into (headers, body).

    Line endings are normalised to \\n first so CRLF messages behave the same.
    Folded header lines (continuations starting with whitespace) are joined
    back onto the line above, so a long To: list reads as one line.
    """
    normalised = F.regexp_replace(message, r"\r\n?", "\n")

    headers = F.regexp_extract(normalised, r"(?s)^(.*?)\n\n", 1)
    # A message with no blank line is all headers and no body.
    headers = F.when(headers == "", normalised).otherwise(headers)
    headers = F.regexp_replace(headers, r"\n[ \t]+", " ")

    body = F.regexp_extract(normalised, r"(?s)^.*?\n\n(.*)$", 1)
    return headers, body


def extract_header(headers: Column, name: str) -> Column:
    """
    Return the value of one header, or null if it is missing or blank.

    The match is anchored to the start of a line, so "To:" will not pick up
    "X-To:", and only the header block is searched, so "To:" lines inside a
    forwarded message in the body are ignored.
    """
    value = F.trim(F.regexp_extract(headers, rf"(?im)^{name}:[ \t]*(.*)$", 1))
    return F.when(value != "", value)


def parse_address_list(raw: Column) -> Column:
    """Turn a comma separated header value into a lower-cased array of addresses."""
    tokens = F.transform(F.split(F.coalesce(raw, F.lit("")), ","), lambda t: F.lower(F.trim(t)))
    return F.filter(tokens, lambda t: t != "")


def parse_email_date(raw: Column) -> tuple[Column, Column]:
    """
    Parse an Enron Date header into (sent_at, sent_at_local).

    sent_at is the true instant (offset applied). sent_at_local is the wall-clock
    time as written in the header, which is what hour-of-day analysis needs.
    Unparseable dates come back as null rather than failing the job, so they can
    be quarantined downstream.
    """
    cleaned = F.regexp_replace(raw, r"\s*\([^)]*\)\s*$", "")  # drop "(PDT)"
    cleaned = F.regexp_replace(cleaned, r"^[A-Za-z]{3},\s*", "")  # drop "Mon, "
    cleaned = F.trim(F.regexp_replace(cleaned, r"\s+", " "))

    without_offset = F.regexp_replace(cleaned, r"\s*[+-]\d{4}$", "")

    sent_at = F.coalesce(
        *[F.try_to_timestamp(cleaned, F.lit(fmt)) for fmt in _DATE_FORMATS_WITH_OFFSET]
    )
    # Stored without a time zone so the wall-clock value never shifts with the
    # session or machine time zone.
    sent_at_local = F.coalesce(
        *[F.try_to_timestamp(without_offset, F.lit(fmt)) for fmt in _DATE_FORMATS_LOCAL]
    ).cast("timestamp_ntz")
    return sent_at, sent_at_local


def parse_emails(raw: DataFrame) -> DataFrame:
    """
    Parse the bronze table (columns `file`, `message`) into one row per email.

    Output columns: file, mailbox, message_id, date_raw, sent_at, sent_at_local,
    sender, to_addresses, cc_addresses, bcc_addresses, subject, body, x_folder.
    Nothing is filtered here; bad rows are kept with nulls so the quality step
    can quarantine them with a reason.
    """
    headers, body = split_headers_and_body(F.col("message"))
    with_parts = raw.select("file", headers.alias("_headers"), body.alias("body"))

    date_raw = extract_header(F.col("_headers"), "Date")
    sent_at, sent_at_local = parse_email_date(date_raw)

    return with_parts.select(
        "file",
        # The Kaggle path starts with the mailbox owner, e.g. "allen-p/sent_mail/1."
        F.split("file", "/").getItem(0).alias("mailbox"),
        F.regexp_replace(extract_header(F.col("_headers"), "Message-ID"), r"^<|>$", "").alias(
            "message_id"
        ),
        date_raw.alias("date_raw"),
        sent_at.alias("sent_at"),
        sent_at_local.alias("sent_at_local"),
        F.lower(extract_header(F.col("_headers"), "From")).alias("sender"),
        parse_address_list(extract_header(F.col("_headers"), "To")).alias("to_addresses"),
        parse_address_list(extract_header(F.col("_headers"), "Cc")).alias("cc_addresses"),
        parse_address_list(extract_header(F.col("_headers"), "Bcc")).alias("bcc_addresses"),
        extract_header(F.col("_headers"), "Subject").alias("subject"),
        "body",
        extract_header(F.col("_headers"), "X-Folder").alias("x_folder"),
    )
