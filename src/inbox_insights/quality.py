"""Data quality rules. Failing rows are quarantined with a reason, never dropped silently."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# The corpus covers Enron's last few years. Anything outside this window is a
# broken Date header (the raw data has emails dated 0001 and 2044).
MIN_YEAR = 1998
MAX_YEAR = 2002


def quality_rules() -> dict[str, Column]:
    """Map each quarantine reason to a condition that is true when a row fails."""
    year = F.year("sent_at")
    return {
        "missing_message_id": F.col("message_id").isNull(),
        "unparseable_date": F.col("sent_at").isNull(),
        "date_out_of_range": (year < MIN_YEAR) | (year > MAX_YEAR),
        "missing_sender": F.col("sender").isNull(),
    }


def failed_reasons(rules: dict[str, Column]) -> Column:
    """
    Build a "; " separated string of every rule a row fails, or null if it passes.

    Listing all reasons rather than the first one makes the quarantine counts
    easier to read when a row is broken in more than one way.
    """
    # A null condition (e.g. year of a null date) counts as passing that rule;
    # the dedicated null check reports it instead.
    flags = [F.when(F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    reasons = F.concat_ws("; ", *flags)
    return F.when(reasons != "", reasons)


def split_by_quality(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split parsed emails into (passed, quarantined).

    The quarantined frame keeps every original column plus `reason` and
    `quarantined_at`, so a bad row can be traced back to its source file.
    """
    checked = df.withColumn("reason", failed_reasons(quality_rules()))

    passed = checked.filter(F.col("reason").isNull()).drop("reason")
    quarantined = checked.filter(F.col("reason").isNotNull()).withColumn(
        "quarantined_at", F.current_timestamp()
    )
    return passed, quarantined
