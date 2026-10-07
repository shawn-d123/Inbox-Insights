"""Column-level cleaning for the silver layer: bodies, subjects and time zones."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Enron staff were mostly in Houston, but every Date header in the corpus is
# written in US Pacific time. Converting to Houston time keeps "9am" meaning 9am
# at the desk, which matters for the after-hours analysis.
HOUSTON_TZ = "America/Chicago"

# A Lotus Notes style timestamp, e.g. "01/04/2001 05:22 PM" or "01:46:39 PM".
_LOTUS_DATE = r"\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2}(?::\d{2})?\s*[AP]M"

# Each pattern marks the start of quoted or forwarded text. The body is cut at
# whichever one appears first, so only what the sender actually wrote is kept.
_QUOTE_MARKERS = [
    r"-{2,}\s*Original Message\s*-{2,}",
    r"-{5,}\s*Forwarded by",
    # A "From:" line only counts as a quoted header if To/Sent/Date follows within
    # a few lines; otherwise press releases starting "From: Governor's Office" vanish.
    r"From:[ \t][^\n]*\n(?:[^\n]*\n){0,3}?[ \t]*(?:To|Sent|Date):",
    r"To:[ \t]",
    r"On [^\n]{0,100}wrote:",
    r">",
    # Lotus Notes header with the sender name and timestamp on separate lines:
    #   Mark Bernstein@ECT
    #   01/04/2001 05:22 PM
    #   To: ...
    rf"[^\n]*\n[ \t]*{_LOTUS_DATE}[ \t]*\n(?:[ \t]*\n)*[ \t]*To:",
    # ...and with both on one line: '"Mahon, Laurie" <x@y.com> on 01/04/2001 01:46:39 PM'
    rf"[^\n]*\S[ \t]+(?:on[ \t]+)?{_LOTUS_DATE}[ \t]*\n(?:[ \t]*\n)*[ \t]*To:",
    # Conventional "-- " signature delimiter on its own line.
    r"--[ \t]*$",
]
_QUOTE_START = r"(?ms)^[ \t]*(?:" + "|".join(_QUOTE_MARKERS) + r").*\z"

# Quoted-printable escapes worth turning back into real characters. The 0x91-0x97
# codes are Windows smart quotes and dashes, which Enron mail is full of.
_QP_REPLACEMENTS = {
    "=09": "\t",
    "=20": " ",
    "=3D": "=",
    "=91": "'",
    "=92": "'",
    "=93": '"',
    "=94": '"',
    "=96": "-",
    "=97": "-",
}


def decode_quoted_printable(body: Column, encoding: Column) -> Column:
    """
    Undo the quoted-printable encoding used by some Enron messages.

    Only applied when the Content-Transfer-Encoding header says so, because "="
    followed by two hex digits can legitimately appear in plain text.
    """
    decoded = F.regexp_replace(body, r"=\n", "")  # soft line break: word continues
    for code, char in _QP_REPLACEMENTS.items():
        decoded = F.regexp_replace(decoded, code, char)
    decoded = F.regexp_replace(decoded, r"=[0-9A-F]{2}", "")
    return F.when(encoding == "quoted-printable", decoded).otherwise(body)


def strip_quoted_text(body: Column) -> Column:
    """Drop forwarded messages, quoted replies and signatures from a body."""
    return F.regexp_replace(body, _QUOTE_START, "")


def normalise_whitespace(text: Column) -> Column:
    """Collapse runs of whitespace into single spaces and trim the ends."""
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def clean_body(body: Column, encoding: Column) -> Column:
    """Decode, strip quoted text and tidy whitespace, in that order."""
    return normalise_whitespace(strip_quoted_text(decode_quoted_printable(body, encoding)))


def normalise_subject(subject: Column) -> Column:
    """
    Lower-case a subject and strip any stack of Re:/Fw:/Fwd: prefixes.

    "RE: Fw: Gas deal" and "gas deal" both become "gas deal", which is what lets
    a reply be matched to the email it answers.
    """
    lowered = F.lower(F.coalesce(subject, F.lit("")))
    stripped = F.regexp_replace(lowered, r"^(\s*(re|fw|fwd)\s*(\[\d+\])?\s*:\s*)+", "")
    return normalise_whitespace(stripped)


def is_reply(subject: Column) -> Column:
    return F.coalesce(F.lower(subject).rlike(r"^\s*re\s*:"), F.lit(False))


def is_forward(subject: Column) -> Column:
    return F.coalesce(F.lower(subject).rlike(r"^\s*(fw|fwd)\s*:"), F.lit(False))


def to_houston_time(sent_at: Column) -> Column:
    """
    Convert a UTC instant into Houston wall-clock time (timestamp without zone).

    convert_timezone reads the instant in the session time zone and converts from
    there, so the result is the same whatever the session is set to.
    """
    return F.convert_timezone(None, F.lit(HOUSTON_TZ), sent_at)
