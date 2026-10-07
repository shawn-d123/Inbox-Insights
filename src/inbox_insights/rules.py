"""Rule-based flags shared by the gold analytics and the feature table."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

INTERNAL_DOMAIN = "@enron.com"

# Ported from the keyword rules in my earlier calendar extraction project
# (meeting / appointment / cancellation categories), plus the phrases Enron staff
# actually used for meetings. Word boundaries stop "meetings" matching inside
# unrelated words and "call" on its own is left out because it is mostly "call me".
MEETING_PATTERN = (
    r"(?i)\b("
    r"meetings?|appointments?|conference calls?|conf\.? calls?|"
    r"rescheduled?|postponed|cancell?ed|agenda|"
    r"(?:let'?s|can we|could we|to) meet"
    r")\b"
)

# Working day in Houston: 08:00 to 19:00, Monday to Friday.
WORKDAY_START_HOUR = 8
WORKDAY_END_HOUR = 19


def is_meeting(subject: Column, body: Column) -> Column:
    """
    True if the subject or body talks about arranging a meeting.

    Pass the full body (forwarded text included) so forwarded meeting notices count.
    """
    text = F.concat_ws(" ", F.coalesce(subject, F.lit("")), F.coalesce(body, F.lit("")))
    return text.rlike(MEETING_PATTERN)


def is_weekend(local_time: Column) -> Column:
    # dayofweek: 1 = Sunday ... 7 = Saturday
    return F.dayofweek(local_time).isin(1, 7)


def is_after_hours(local_time: Column) -> Column:
    """True for emails sent before 8am, from 7pm, or at the weekend (Houston time)."""
    hour = F.hour(local_time)
    return (hour < WORKDAY_START_HOUR) | (hour >= WORKDAY_END_HOUR) | is_weekend(local_time)


def is_internal(sender: Column, recipients: Column) -> Column:
    """
    True when an Enron address sent the email only to Enron addresses.

    Emails with no visible recipients (Bcc only) count as external, since there
    is no evidence they stayed inside the company.
    """
    enron_sender = sender.endswith(INTERNAL_DOMAIN)
    all_enron = F.forall(recipients, lambda r: r.endswith(INTERNAL_DOMAIN))
    return enron_sender & (F.size(recipients) > 0) & all_enron
