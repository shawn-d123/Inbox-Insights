from datetime import UTC, datetime

import pytest

from inbox_insights.parsing import parse_emails


def make_message(headers: list[str], body: str = "Hello.", newline: str = "\n") -> str:
    """Build a raw message in the same shape as the Kaggle `message` column."""
    return newline.join(headers) + newline + newline + body


STANDARD_HEADERS = [
    "Message-ID: <18782981.1075855378110.JavaMail.evans@thyme>",
    "Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)",
    "From: Phillip.Allen@enron.com",
    "To: tim.belden@enron.com",
    "Subject: Forecast",
    "Mime-Version: 1.0",
    "X-From: Phillip K Allen",
    "X-To: Tim Belden <Tim Belden/Enron@EnronXGate>",
    "X-Folder: \\Phillip_Allen_Jan2002_1\\Allen, Phillip K.\\'Sent Mail",
]


def parse_one(spark, message: str, file: str = "allen-p/_sent_mail/1."):
    df = spark.createDataFrame([(file, message)], "file string, message string")
    return parse_emails(df).collect()[0]


def test_standard_email(spark):
    row = parse_one(spark, make_message(STANDARD_HEADERS, "Here is our forecast"))

    assert row.mailbox == "allen-p"
    assert row.message_id == "18782981.1075855378110.JavaMail.evans@thyme"
    assert row.sender == "phillip.allen@enron.com"
    assert row.to_addresses == ["tim.belden@enron.com"]
    assert row.cc_addresses == []
    assert row.subject == "Forecast"
    assert row.body == "Here is our forecast"
    assert row.x_folder.endswith("'Sent Mail")


def test_date_keeps_both_instant_and_wall_clock(spark):
    row = parse_one(spark, make_message(STANDARD_HEADERS))

    # PySpark hands back sent_at in the machine's local zone, so compare instants.
    assert row.sent_at.timestamp() == datetime(2001, 5, 14, 23, 39, tzinfo=UTC).timestamp()
    assert row.sent_at_local == datetime(2001, 5, 14, 16, 39, 0)


def test_missing_subject_is_null(spark):
    headers = [h if not h.startswith("Subject") else "Subject: " for h in STANDARD_HEADERS]
    assert parse_one(spark, make_message(headers)).subject is None


def test_folded_recipient_list_is_unfolded(spark):
    headers = STANDARD_HEADERS[:3] + [
        "To: Tim.Belden@enron.com, john.lavorato@enron.com, ",
        "\tsally.beck@enron.com,",
        "\tkevin.presto@enron.com",
        "Subject: Systems meeting",
    ]
    row = parse_one(spark, make_message(headers))

    assert row.to_addresses == [
        "tim.belden@enron.com",
        "john.lavorato@enron.com",
        "sally.beck@enron.com",
        "kevin.presto@enron.com",
    ]
    assert row.subject == "Systems meeting"


def test_cc_and_bcc(spark):
    headers = STANDARD_HEADERS + ["Cc: a@enron.com, b@enron.com", "Bcc: a@enron.com, b@enron.com"]
    row = parse_one(spark, make_message(headers))

    assert row.cc_addresses == ["a@enron.com", "b@enron.com"]
    assert row.bcc_addresses == ["a@enron.com", "b@enron.com"]


def test_x_to_header_is_not_read_as_to(spark):
    headers = [h for h in STANDARD_HEADERS if not h.startswith("To:")]
    assert parse_one(spark, make_message(headers)).to_addresses == []


def test_headers_inside_forwarded_body_are_ignored(spark):
    body = (
        "---------------------- Forwarded by Phillip K Allen/HOU/ECT on 07/11/2000 ----\n"
        "\n"
        "From: Kimberly Hillis 07/11/2000 01:16 PM\n"
        "To: someone.else@enron.com\n"
        "Subject: Systems Meeting 7/18\n"
        "\n"
        "Please note the meeting has moved."
    )
    row = parse_one(spark, make_message(STANDARD_HEADERS, body))

    assert row.to_addresses == ["tim.belden@enron.com"]
    assert row.subject == "Forecast"
    assert row.body.startswith("---------------------- Forwarded by")
    assert row.body.endswith("meeting has moved.")


def test_crlf_line_endings(spark):
    row = parse_one(spark, make_message(STANDARD_HEADERS, "Line one\r\nLine two", newline="\r\n"))

    assert row.subject == "Forecast"
    assert row.body == "Line one\nLine two"


@pytest.mark.parametrize(
    ("date_header", "expected_local"),
    [
        ("Date: Fri, 1 Dec 2000 08:05:00 -0800 (PST)", datetime(2000, 12, 1, 8, 5)),
        ("Date: 1 Dec 2000 08:05:00 -0800", datetime(2000, 12, 1, 8, 5)),
        ("Date: Fri,  1 Dec 2000 08:05 -0800 (PST)", datetime(2000, 12, 1, 8, 5)),
    ],
)
def test_odd_but_valid_dates(spark, date_header, expected_local):
    headers = [date_header if h.startswith("Date") else h for h in STANDARD_HEADERS]
    row = parse_one(spark, make_message(headers))

    assert row.sent_at_local == expected_local
    assert row.sent_at is not None


@pytest.mark.parametrize("date_header", ["Date: sometime last week", "Date: "])
def test_unparseable_date_becomes_null(spark, date_header):
    headers = [date_header if h.startswith("Date") else h for h in STANDARD_HEADERS]
    row = parse_one(spark, make_message(headers))

    assert row.sent_at is None
    assert row.sent_at_local is None


def test_missing_message_id_and_sender_are_null(spark):
    headers = [h for h in STANDARD_HEADERS if not h.startswith(("Message-ID", "From"))]
    row = parse_one(spark, make_message(headers))

    assert row.message_id is None
    assert row.sender is None


def test_message_with_no_body(spark):
    row = parse_one(spark, "\n".join(STANDARD_HEADERS))

    assert row.subject == "Forecast"
    assert row.body == ""
