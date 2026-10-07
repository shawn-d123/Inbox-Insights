from datetime import UTC, datetime

import pytest
from pyspark.sql import functions as F

from inbox_insights.cleaning import (
    clean_body,
    correct_sent_at,
    is_forward,
    is_reply,
    normalise_subject,
    to_houston_time,
)


def apply(spark, expr, value, schema="value string"):
    """Run one column expression over a single value and return the result."""
    return spark.createDataFrame([(value,)], schema).select(expr.alias("out")).first().out


def clean(spark, body, encoding="7bit"):
    df = spark.createDataFrame([(body, encoding)], "body string, enc string")
    return df.select(clean_body(F.col("body"), F.col("enc")).alias("out")).first().out


def test_plain_body_is_only_tidied(spark):
    assert clean(spark, "  Thanks,\n\n  see you at 3pm.  ") == "Thanks, see you at 3pm."


def test_original_message_block_is_removed(spark):
    body = (
        "Sounds good to me.\n\n -----Original Message-----\n"
        "From: Smith, Jo\nSent: Monday\n\nOld text"
    )
    assert clean(spark, body) == "Sounds good to me."


def test_lotus_forward_is_removed(spark):
    body = (
        "can you take care of this...\n"
        "---------------------- Forwarded by John Arnold/HOU/ECT on 04/30/2001 08:26 PM ---\n"
        "\nInvoice attached."
    )
    assert clean(spark, body) == "can you take care of this..."


def test_lotus_header_on_separate_lines_is_removed(spark):
    body = (
        "I need to go to DC next Tuesday.\n\nKay\n\n\n"
        "Mark Bernstein@ECT\n01/04/2001 05:22 PM\nTo: Kay Mann/Corp/Enron@Enron\n"
        "cc:\nSubject: Coop City\n\nOlder message"
    )
    assert clean(spark, body) == "I need to go to DC next Tuesday. Kay"


def test_lotus_header_on_one_line_is_removed(spark):
    body = (
        "See below.\n\n"
        '"Mahon, Laurie" <MahonL@pbworld.com> on 01/04/2001 01:46:39 PM\n'
        "To: mark.bernstein@enron.com\nSubject: Coop City\n\nOlder message"
    )
    assert clean(spark, body) == "See below."


def test_angle_bracket_quotes_and_wrote_line_are_removed(spark):
    body = "Agreed.\n\nOn Mon, 14 May 2001, Tim wrote:\n> are we on for Friday?"
    assert clean(spark, body) == "Agreed."


def test_lotus_from_line_with_to_is_removed(spark):
    body = (
        "Contact list attached. --Sally\n\n"
        "From: Sally Beck 07/05/2000 06:23 PM\nTo: Patti Thompson/HOU/ECT@ECT\n\nOld"
    )
    assert clean(spark, body) == "Contact list attached. --Sally"


def test_from_line_without_header_block_is_kept(spark):
    body = "From: Governor's Office of Emergency Services\n\nState's mutual aid system works."
    assert clean(spark, body).startswith("From: Governor's Office")


def test_signature_after_delimiter_is_removed(spark):
    assert clean(spark, "See attached.\n-- \nJeff Dasovich\nDirector") == "See attached."


def test_pure_forward_leaves_empty_body(spark):
    body = "----- Forwarded by Jeff Dasovich/NA/Enron on 04/05/2001 02:44 PM -----\n\nText"
    assert clean(spark, body) == ""


def test_quoted_printable_is_decoded_only_when_declared(spark):
    body = "Solar power has long been used as rene=\nwable energy.=09It=92s cheap."

    decoded = clean(spark, body, "quoted-printable")
    assert decoded == "Solar power has long been used as renewable energy. It's cheap."
    assert "=09" in clean(spark, body, "7bit")


@pytest.mark.parametrize(
    ("subject", "expected"),
    [
        ("RE: Fw: Gas Deal", "gas deal"),
        ("re:re:  Gas   deal ", "gas deal"),
        ("FWD: Systems Meeting 7/18", "systems meeting 7/18"),
        ("Re[2]: Budget", "budget"),
        ("Regarding the budget", "regarding the budget"),
        (None, ""),
    ],
)
def test_normalise_subject(spark, subject, expected):
    assert apply(spark, normalise_subject(F.col("value")), subject) == expected


@pytest.mark.parametrize(
    ("subject", "reply", "forward"),
    [
        ("RE: budget", True, False),
        ("Fwd: budget", False, True),
        ("Re-org plan", False, False),
        (None, False, False),
    ],
)
def test_reply_and_forward_flags(spark, subject, reply, forward):
    assert apply(spark, is_reply(F.col("value")), subject) is reply
    assert apply(spark, is_forward(F.col("value")), subject) is forward


@pytest.mark.parametrize("session_tz", ["UTC", "Asia/Tokyo"])
def test_lotus_notes_times_are_corrected(spark, session_tz):
    # Header said "Tue, 11 Jul 2000 09:24:00 -0700"; the Lotus forward stamp in the
    # same email said 04:24 PM Houston time.
    header_instant = datetime(2000, 7, 11, 16, 24, tzinfo=UTC)
    df = spark.createDataFrame(
        [("lotus_notes", header_instant), ("outlook", header_instant)],
        "source string, sent_at timestamp",
    )
    corrected = correct_sent_at(F.col("sent_at"), F.col("source"))

    original_tz = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        rows = df.select("source", to_houston_time(corrected).alias("houston")).collect()
    finally:
        spark.conf.set("spark.sql.session.timeZone", original_tz)

    houston = {r.source: r.houston for r in rows}
    assert houston["lotus_notes"] == datetime(2000, 7, 11, 16, 24)
    assert houston["outlook"] == datetime(2000, 7, 11, 11, 24)


def test_houston_time_handles_daylight_saving(spark):
    summer = datetime(2001, 5, 14, 23, 39, tzinfo=UTC)  # CDT, UTC-5
    winter = datetime(2001, 1, 15, 15, 0, tzinfo=UTC)  # CST, UTC-6

    houston = to_houston_time(F.col("value"))
    assert apply(spark, houston, summer, "value timestamp") == datetime(2001, 5, 14, 18, 39)
    assert apply(spark, houston, winter, "value timestamp") == datetime(2001, 1, 15, 9, 0)
