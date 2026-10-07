# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver clean
# MAGIC Parses bronze into typed emails, quarantines rows that fail quality checks,
# MAGIC removes duplicates and builds the recipients table. All logic lives in
# MAGIC `src/inbox_insights`, where it is unit tested.

# COMMAND ----------

import os
import sys

# The bundle deploys src/ next to notebooks/, so make the package importable.
sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F

from inbox_insights.parsing import parse_emails
from inbox_insights.quality import split_by_quality
from inbox_insights.silver import add_clean_columns, build_recipients, deduplicate

dbutils.widgets.text("catalog", "inbox_insights")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.raw_emails")
parsed = parse_emails(bronze)
passed, quarantined = split_by_quality(parsed)
emails, stages = deduplicate(add_clean_columns(passed))


def save(df, table):
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{catalog}.{table}")


save(quarantined, "silver.dq_quarantine")
save(emails, "silver.emails")
save(build_recipients(spark.table(f"{catalog}.silver.emails")), "silver.recipients")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts at each stage
# MAGIC Stored in `silver.run_metrics` so the README numbers can be traced to a run.

# COMMAND ----------

counts = {
    "bronze_rows": bronze.count(),
    "quarantined": spark.table(f"{catalog}.silver.dq_quarantine").count(),
    "after_message_id_dedup": stages["after_message_id"].count(),
    "after_content_dedup": spark.table(f"{catalog}.silver.emails").count(),
    "recipient_rows": spark.table(f"{catalog}.silver.recipients").count(),
}
counts["passed_quality"] = counts["bronze_rows"] - counts["quarantined"]

for name, value in counts.items():
    print(f"{name:>24}: {value:,}")

# A handful of bad rows is expected. A large share means parsing broke, so fail the
# job rather than publish a near-empty silver layer.
MAX_QUARANTINE_SHARE = 0.05
quarantine_share = counts["quarantined"] / counts["bronze_rows"]
assert quarantine_share <= MAX_QUARANTINE_SHARE, f"{quarantine_share:.1%} of rows quarantined"

metrics = spark.createDataFrame(
    [(name, value) for name, value in counts.items()], "metric string, value long"
).withColumn("run_at", F.current_timestamp())
metrics.write.mode("append").saveAsTable(f"{catalog}.silver.run_metrics")

# COMMAND ----------

display(spark.table(f"{catalog}.silver.dq_quarantine").groupBy("reason").count())

# COMMAND ----------

silver = spark.table(f"{catalog}.silver.emails")
check_cols = ["message_id", "sent_at", "sent_at_houston", "sender", "subject", "body_clean"]
display(silver.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in check_cols]))

# COMMAND ----------

display(
    silver.select("mailbox", "sent_at_houston", "sender", "subject", "body_clean")
    .orderBy(F.rand(42))
    .limit(5)
)
