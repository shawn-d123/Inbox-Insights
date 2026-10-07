from datetime import datetime

from inbox_insights.silver import build_recipients, deduplicate

EMAIL_SCHEMA = (
    "message_id string, file string, sender string, sent_at timestamp, subject string, "
    "body_hash string"
)
SENT = datetime(2001, 5, 14, 16, 39)


def test_deduplicate_by_message_id_then_content(spark):
    rows = [
        # Same Message-ID stored twice: keep the first file alphabetically.
        ("id-1", "allen-p/inbox/2.", "a@enron.com", SENT, "Forecast", "h1"),
        ("id-1", "allen-p/all_documents/9.", "a@enron.com", SENT, "Forecast", "h1"),
        # Same email copied into another mailbox under a new Message-ID.
        ("id-2", "belden-t/inbox/5.", "a@enron.com", SENT, "Forecast", "h1"),
        # Same sender and body but sent at a different time: a genuinely new email.
        ("id-3", "allen-p/sent/3.", "a@enron.com", datetime(2001, 5, 15, 9, 0), "Forecast", "h1"),
    ]
    deduped, stages = deduplicate(spark.createDataFrame(rows, EMAIL_SCHEMA))

    assert stages["after_message_id"].count() == 3
    assert sorted((r.message_id, r.file) for r in deduped.collect()) == [
        ("id-1", "allen-p/all_documents/9."),
        ("id-3", "allen-p/sent/3."),
    ]


def test_null_subjects_still_deduplicate(spark):
    rows = [
        ("id-1", "a/1.", "a@enron.com", SENT, None, "h1"),
        ("id-2", "b/1.", "a@enron.com", SENT, None, "h1"),
    ]
    deduped, _ = deduplicate(spark.createDataFrame(rows, EMAIL_SCHEMA))
    assert deduped.count() == 1


def test_recipients_explode_to_and_cc_and_prefer_to(spark):
    emails = spark.createDataFrame(
        [
            ("id-1", ["b@enron.com", "c@enron.com"], ["d@enron.com", "b@enron.com"]),
            ("id-2", [], []),
        ],
        "message_id string, to_addresses array<string>, cc_addresses array<string>",
    )
    recipients = build_recipients(emails).collect()

    assert {(r.message_id, r.recipient, r.recipient_type) for r in recipients} == {
        ("id-1", "b@enron.com", "to"),
        ("id-1", "c@enron.com", "to"),
        ("id-1", "d@enron.com", "cc"),
    }
