"""Table documentation, owners and PII tags for Unity Catalog.

This module is the single source for what every table and column means. The same
definitions are applied to Unity Catalog (comments and tags) and rendered into
docs/data_dictionary.md, so the catalogue and the docs cannot drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from inbox_insights.features import FEATURE_COLUMNS


@dataclass(frozen=True)
class TableDoc:
    comment: str
    columns: dict[str, str] = field(default_factory=dict)
    pii_columns: tuple[str, ...] = ()


FEATURE_DOCS = {
    "body_length": "Characters in the cleaned body (quoted text removed).",
    "word_count": "Words in the cleaned body.",
    "to_count": "Number of To recipients.",
    "cc_count": "Number of Cc recipients.",
    "recipient_count": "To plus Cc recipients.",
    "is_reply": "1 if the subject starts with Re:.",
    "is_forward": "1 if the subject starts with Fw: or Fwd:.",
    "thread_position": "Earlier emails with the same normalised subject (0 = first in thread).",
    "has_question": "1 if the cleaned body contains a question mark.",
    "sender_is_internal": "1 if the sender has an @enron.com address.",
    "sender_prior_emails": "Emails the sender had sent before this one.",
    "prior_emails_to_recipients": "Most earlier emails from this sender to any one recipient.",
    "prior_replies_from_recipients": "Times these recipients had already replied to this sender.",
    "hour_sent": "Hour sent, Houston time (0-23).",
    "weekday_sent": "Day sent, Houston time (1 = Sunday ... 7 = Saturday).",
    "is_after_hours": "1 if sent before 8am, from 7pm or at the weekend (Houston time).",
    "is_meeting": "1 if the subject or full body matches the meeting keyword rule.",
    "has_urgent": "1 if the subject or body contains 'urgent'.",
    "has_asap": "1 if the subject or body contains 'asap' or 'as soon as possible'.",
    "has_deadline": "1 if it mentions a deadline or 'by today/tomorrow/end of day'.",
    "has_action_request": "1 if it asks the reader to respond, confirm, review, call etc.",
    "has_important": "1 if the subject or body contains 'important'.",
}

TABLES: dict[str, TableDoc] = {
    "bronze.raw_emails": TableDoc(
        "Kaggle Enron emails.csv loaded unchanged, one row per mailbox file.",
        {
            "file": "Source path in the original maildir, e.g. allen-p/sent_mail/1.",
            "message": "Raw email: headers, blank line, body.",
            "source_path": "File in the Volume this row was read from.",
            "ingested_at": "When the row was loaded.",
        },
        pii_columns=("message",),
    ),
    "silver.emails": TableDoc(
        "Parsed, cleaned and de-duplicated emails. One row per unique email.",
        {
            "message_id": "Message-ID header, without angle brackets. Primary key.",
            "file": "Source file of the copy that was kept after de-duplication.",
            "mailbox": "Mailbox owner folder, e.g. allen-p.",
            "x_folder": "Original mail client folder path.",
            "source_system": "lotus_notes, outlook or unknown, from X-FileName.",
            "sent_at_header": "Date header as written. Shifted for Lotus Notes exports.",
            "sent_at": "Corrected send time (UTC instant).",
            "sent_at_houston": "Corrected send time as Houston wall-clock time.",
            "sender": "From address, lower case.",
            "to_addresses": "To addresses, lower case.",
            "cc_addresses": "Cc addresses, lower case. Bcc is dropped: it always copies Cc.",
            "subject": "Subject line.",
            "subject_normalised": "Subject lower-cased with Re:/Fw: prefixes removed.",
            "is_reply": "Subject starts with Re:.",
            "is_forward": "Subject starts with Fw: or Fwd:.",
            "body_clean": "Body with quoted replies, forwards and signatures removed.",
            "body_full": "Decoded body including forwarded text; used for topic flags.",
            "body_hash": "SHA-256 of the raw body; part of the de-duplication key.",
        },
        pii_columns=("sender", "to_addresses", "cc_addresses", "body_clean", "body_full"),
    ),
    "silver.recipients": TableDoc(
        "One row per email and To/Cc recipient.",
        {
            "message_id": "Email this recipient belongs to.",
            "recipient": "Recipient address, lower case.",
            "recipient_type": "to or cc. An address in both is kept once, as to.",
        },
        pii_columns=("recipient",),
    ),
    "silver.dq_quarantine": TableDoc(
        "Parsed rows that failed a quality rule, kept with the reason instead of dropped.",
        {
            "reason": "Rules failed, '; ' separated: missing_message_id, unparseable_date, "
            "date_out_of_range, missing_sender.",
            "quarantined_at": "When the row was quarantined.",
        },
        pii_columns=("sender", "to_addresses", "cc_addresses", "bcc_addresses", "body"),
    ),
    "silver.run_metrics": TableDoc(
        "Row counts at each pipeline stage, appended on every run.",
        {
            "metric": "Stage name, e.g. bronze_rows or after_content_dedup.",
            "value": "Row count.",
            "run_at": "When the run recorded the count.",
        },
    ),
    "gold.weekly_volume": TableDoc(
        "Emails per week, split by whether they stayed inside Enron.",
        {
            "week_start": "Monday of the week, Houston time.",
            "total_emails": "Unique emails sent that week.",
            "internal_emails": "Sent by an Enron address to Enron addresses only.",
            "external_emails": "Any other email, including Bcc-only ones.",
        },
    ),
    "gold.email_categories": TableDoc(
        "Monthly share of meeting, after-hours and weekend email.",
        {
            "month": "First day of the month, Houston time.",
            "total_emails": "Unique emails sent that month.",
            "meeting_emails": "Emails matching the meeting keyword rule.",
            "meeting_share": "meeting_emails / total_emails.",
            "after_hours_emails": "Sent before 8am, from 7pm or at the weekend.",
            "after_hours_share": "after_hours_emails / total_emails.",
            "weekend_emails": "Sent on Saturday or Sunday.",
        },
    ),
    "gold.reply_pairs": TableDoc(
        "Each reply matched to the email it answers (same subject, reversed sender and "
        "recipient, within 14 days, latest candidate).",
        {
            "reply_id": "Message-ID of the reply.",
            "original_id": "Message-ID of the email being replied to.",
            "replier": "Who sent the reply.",
            "original_sender": "Who sent the original email.",
            "subject_normalised": "Shared normalised subject.",
            "original_sent_at": "When the original was sent (UTC).",
            "reply_sent_at": "When the reply was sent (UTC).",
            "response_minutes": "Minutes between the original and the reply.",
        },
        pii_columns=("replier", "original_sender"),
    ),
    "gold.person_activity": TableDoc(
        "One row per email address: volume, after-hours load and reply speed.",
        {
            "person": "Email address.",
            "sent_count": "Unique emails sent.",
            "after_hours_sent_share": "Share of sent email that was after hours.",
            "received_count": "Times the address appears as a To or Cc recipient.",
            "replies_matched": "Replies by this person matched in gold.reply_pairs.",
            "median_response_minutes": "Median minutes this person took to reply.",
            "is_enron": "Address is @enron.com.",
        },
        pii_columns=("person",),
    ),
    "gold.message_features": TableDoc(
        "ML feature table: one row per email sent To a mailbox owner, with point-in-time "
        "features and the reply-within-2-hours label.",
        {
            "message_id": "Email this row describes.",
            "sender": "Sender address (kept for analysis, not used as a feature).",
            "sent_at": "Send time (UTC); used for the time-based train/test split.",
            **FEATURE_DOCS,
            "text": "Subject plus cleaned body; TF-IDF is fitted on this at training time.",
            "label_replied_2h": "Label: 1 if any recipient replied within 120 minutes.",
        },
        pii_columns=("sender", "text"),
    ),
    "gold.model_metrics": TableDoc(
        "Test-set results of each training run, for the model and the baselines.",
        {
            "run_id": "MLflow run ID.",
            "test_from": "Start of the test period.",
            "model": "model, model_at_0.5 or a baseline name.",
            "threshold": "Probability cut-off used.",
            "precision": "Precision for the replied-within-2h class.",
            "recall": "Recall for the replied-within-2h class.",
            "f1": "F1 for the replied-within-2h class.",
            "positives_predicted": "Emails flagged as likely to get a fast reply.",
            "test_pr_auc": "Area under the precision-recall curve (model rows only).",
            "test_roc_auc": "Area under the ROC curve (model rows only).",
            "test_rows": "Emails in the test period.",
            "test_positive_rate": "Share of test emails replied to within 2 hours.",
            "trained_at": "When the run finished.",
        },
    ),
}

assert set(FEATURE_DOCS) == set(FEATURE_COLUMNS), "Every feature needs a description"


def _quote(text: str) -> str:
    """Escape text for use inside a single-quoted SQL string literal."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def governance_statements(catalog: str, owner: str) -> list[str]:
    """
    SQL that applies comments, owners, layer tags and PII tags to every table.

    Rerun after each pipeline run, because overwriting a table with a new schema
    can reset column comments.
    """
    statements = []
    for name, doc in TABLES.items():
        table = f"{catalog}.{name}"
        layer = name.split(".")[0]
        statements += [
            f"COMMENT ON TABLE {table} IS {_quote(doc.comment)}",
            f"ALTER TABLE {table} OWNER TO `{owner}`",
            f"ALTER TABLE {table} SET TAGS ('layer' = '{layer}', 'project' = 'inbox_insights')",
        ]
        statements += [
            f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT {_quote(text)}"
            for column, text in doc.columns.items()
        ]
        statements += [
            f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ('pii' = 'true')"
            for column in doc.pii_columns
        ]
    return statements


def render_data_dictionary() -> str:
    """Markdown data dictionary built from the same definitions as the catalogue."""
    lines = [
        "# Data dictionary",
        "",
        "Generated from `src/inbox_insights/governance.py`, which is also what writes the",
        "Unity Catalog comments and tags. Columns marked PII are tagged `pii = true`.",
    ]
    for name, doc in TABLES.items():
        lines += ["", f"## `{name}`", "", doc.comment, "", "| Column | Description | PII |"]
        lines += ["|---|---|---|"]
        for column, text in doc.columns.items():
            pii = "yes" if column in doc.pii_columns else ""
            lines.append(f"| `{column}` | {text} | {pii} |")
        undocumented_pii = [c for c in doc.pii_columns if c not in doc.columns]
        if undocumented_pii:
            lines += ["", "Also tagged PII: " + ", ".join(f"`{c}`" for c in undocumented_pii)]
    return "\n".join(lines) + "\n"
