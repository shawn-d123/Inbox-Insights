from pathlib import Path

import pytest
from sample_emails import BASE_ROWS, make_emails

from inbox_insights.features import build_message_features
from inbox_insights.gold import (
    add_flags,
    email_categories,
    match_replies,
    person_activity,
    weekly_volume,
)
from inbox_insights.governance import TABLES, governance_statements, render_data_dictionary
from inbox_insights.silver import build_recipients


@pytest.fixture(scope="module")
def built_tables(spark):
    """Run the real gold builders on tiny data so their output columns can be checked."""
    emails = make_emails(spark, BASE_ROWS)
    recipients = build_recipients(emails)
    flagged = add_flags(emails)
    pairs = match_replies(emails)
    return {
        "silver.recipients": recipients,
        "gold.weekly_volume": weekly_volume(flagged),
        "gold.email_categories": email_categories(flagged),
        "gold.reply_pairs": pairs,
        "gold.person_activity": person_activity(flagged, recipients, pairs),
        "gold.message_features": build_message_features(emails, recipients, pairs),
    }


def test_every_built_column_is_documented(built_tables):
    for name, df in built_tables.items():
        assert set(df.columns) == set(TABLES[name].columns), name


def test_pii_columns_are_real_columns():
    for name, doc in TABLES.items():
        if name == "silver.dq_quarantine":
            continue  # quarantine keeps all parsed columns but only documents its own
        assert set(doc.pii_columns) <= set(doc.columns), name


def test_statements_escape_quotes_and_tag_pii():
    statements = governance_statements("inbox_insights", "someone@example.com")

    assert "COMMENT ON TABLE inbox_insights.bronze.raw_emails IS" in statements[0]
    # Quotes inside a description must be escaped or the SQL string ends early.
    assert (
        "ALTER TABLE inbox_insights.gold.message_features ALTER COLUMN has_urgent "
        "COMMENT '1 if the subject or body contains \\'urgent\\'.'"
    ) in statements
    assert (
        "ALTER TABLE inbox_insights.gold.person_activity ALTER COLUMN person "
        "SET TAGS ('pii' = 'true')"
    ) in statements
    assert "ALTER TABLE inbox_insights.gold.weekly_volume OWNER TO `someone@example.com`" in (
        statements
    )


def test_committed_data_dictionary_is_up_to_date():
    committed = Path("docs/data_dictionary.md").read_text(encoding="utf-8")
    assert committed == render_data_dictionary(), "Run: python scripts/write_data_dictionary.py"
