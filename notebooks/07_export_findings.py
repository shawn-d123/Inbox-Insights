# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Export findings
# MAGIC Builds the README charts from the gold tables and writes them, with a
# MAGIC `findings.json` of headline numbers, to the `gold.reports` Volume.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F

from inbox_insights.charts import (
    bucket_response_times,
    monthly_share_chart,
    response_time_chart,
    top_senders_chart,
    volume_chart,
)

dbutils.widgets.text("catalog", "inbox_insights")
catalog = dbutils.widgets.get("catalog")
gold = f"{catalog}.gold"
output_dir = f"/Volumes/{catalog}/gold/reports"

# The corpus thins out sharply at both ends, so trend charts use the period with
# meaningful volume. Months under this many emails are left out of share charts.
CHART_START, CHART_END = "1999-07-01", "2002-03-01"
MIN_MONTHLY_EMAILS = 1000

# Accounts that send machine-generated mail; excluded from "busiest senders".
AUTOMATED_SENDERS = ["pete.davis@enron.com"]

# COMMAND ----------

weekly = (
    spark.table(f"{gold}.weekly_volume")
    .filter(F.col("week_start").between(CHART_START, CHART_END))
    .toPandas()
)
monthly = (
    spark.table(f"{gold}.email_categories")
    .filter(F.col("month").between(CHART_START, CHART_END))
    .filter(F.col("total_emails") >= MIN_MONTHLY_EMAILS)
    .orderBy("month")
    .toPandas()
)
top_senders = (
    spark.table(f"{gold}.person_activity")
    .filter("is_enron")
    .filter(~F.col("person").isin(AUTOMATED_SENDERS))
    .orderBy(F.desc("sent_count"))
    .limit(10)
    .toPandas()
)
reply_minutes = spark.table(f"{gold}.reply_pairs").select("response_minutes").toPandas()
median_minutes = float(reply_minutes["response_minutes"].median())

# COMMAND ----------

charts = {
    "weekly_volume.png": volume_chart(weekly),
    "top_senders.png": top_senders_chart(top_senders),
    "response_times.png": response_time_chart(
        bucket_response_times(reply_minutes["response_minutes"]), median_minutes
    ),
    "meeting_share.png": monthly_share_chart(
        monthly, "meeting_share", "Email about meetings", "Share of emails each month"
    ),
    "after_hours_share.png": monthly_share_chart(
        monthly,
        "after_hours_share",
        "Email sent after hours",
        "Share sent before 8am, from 7pm or at weekends (Houston time)",
    ),
}
for name, figure in charts.items():
    figure.savefig(f"{output_dir}/{name}", facecolor=figure.get_facecolor())
print(f"Wrote {len(charts)} charts to {output_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Headline numbers for the README

# COMMAND ----------

run_metrics = {
    r.metric: r.value
    for r in spark.sql(
        f"""
        SELECT metric, value FROM {catalog}.silver.run_metrics
        WHERE run_at = (SELECT max(run_at) FROM {catalog}.silver.run_metrics)
        """
    ).collect()
}
totals = (
    spark.table(f"{gold}.email_categories")
    .agg(
        F.sum("total_emails").alias("emails"),
        (F.sum("meeting_emails") / F.sum("total_emails")).alias("meeting_share"),
        (F.sum("after_hours_emails") / F.sum("total_emails")).alias("after_hours_share"),
        (F.sum("weekend_emails") / F.sum("total_emails")).alias("weekend_share"),
    )
    .first()
)
volume = (
    spark.table(f"{gold}.weekly_volume")
    .agg(
        (F.sum("internal_emails") / F.sum("total_emails")).alias("internal_share"),
        F.max_by("week_start", "total_emails").alias("busiest_week"),
        F.max("total_emails").alias("busiest_week_emails"),
    )
    .first()
)
replies = (
    spark.table(f"{gold}.reply_pairs")
    .agg(
        F.count("*").alias("matched_replies"),
        F.median("response_minutes").alias("median_minutes"),
        F.avg((F.col("response_minutes") <= 120).cast("int")).alias("share_within_2h"),
    )
    .first()
)
model_rows = spark.sql(
    f"""
    SELECT * FROM {gold}.model_metrics
    WHERE trained_at = (SELECT max(trained_at) FROM {gold}.model_metrics)
    """
).collect()

findings = {
    "pipeline": run_metrics,
    "emails": totals.emails,
    "meeting_share": totals.meeting_share,
    "after_hours_share": totals.after_hours_share,
    "weekend_share": totals.weekend_share,
    "internal_share": volume.internal_share,
    "busiest_week": str(volume.busiest_week),
    "busiest_week_emails": volume.busiest_week_emails,
    "matched_replies": replies.matched_replies,
    "median_reply_minutes": replies.median_minutes,
    "share_replied_within_2h": replies.share_within_2h,
    "top_senders": top_senders[["person", "sent_count", "median_response_minutes"]].to_dict(
        "records"
    ),
    "model": {
        r.model: {
            k: r[k]
            for k in ("threshold", "precision", "recall", "f1", "test_pr_auc", "test_roc_auc")
        }
        | {"run_id": r.run_id, "test_from": r.test_from, "test_rows": r.test_rows}
        | {"test_positive_rate": r.test_positive_rate}
        for r in model_rows
    },
}
with open(f"{output_dir}/findings.json", "w") as f:
    json.dump(findings, f, indent=2, default=str)
print(json.dumps(findings, indent=2, default=str))
