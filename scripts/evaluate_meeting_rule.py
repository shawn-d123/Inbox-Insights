"""Score the Spark meeting rule against the 20 hand-labelled Enron emails.

The labels come from my earlier calendar extraction project, where the same 20
emails were scored with a pandas rule-based baseline. Usage:

    python scripts/evaluate_meeting_rule.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from inbox_insights.parsing import parse_emails
from inbox_insights.rules import is_meeting
from inbox_insights.silver import add_clean_columns

BENCHMARK = Path("benchmarks") / "enron_meeting_labels.csv"


def main() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    labelled = spark.read.csv(str(BENCHMARK), header=True, multiLine=True, escape='"')
    cleaned = add_clean_columns(parse_emails(labelled.select("file", "message")))
    scored = cleaned.join(labelled.select("file", "is_meeting"), "file").select(
        "subject",
        (F.col("is_meeting") == "true").alias("actual"),
        is_meeting(F.col("subject"), F.col("body_full")).alias("predicted"),
    )

    rows = scored.collect()
    tp = sum(r.actual and r.predicted for r in rows)
    fp = sum(not r.actual and r.predicted for r in rows)
    fn = sum(r.actual and not r.predicted for r in rows)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * tp / (2 * tp + fp + fn) if tp else 0.0

    for r in rows:
        if r.actual != r.predicted:
            print(f"  {'missed' if r.actual else 'false alarm':>11}: {r.subject}")
    print(f"rows={len(rows)} tp={tp} fp={fp} fn={fn}")
    print(f"precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}")
    spark.stop()


if __name__ == "__main__":
    main()
