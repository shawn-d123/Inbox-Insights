# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold analytics
# MAGIC Builds the tables behind the README findings:
# MAGIC `weekly_volume`, `email_categories`, `reply_pairs` and `person_activity`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F

from inbox_insights.frames import save_table
from inbox_insights.gold import (
    add_flags,
    email_categories,
    match_replies,
    person_activity,
    weekly_volume,
)

dbutils.widgets.text("catalog", "inbox_insights")
catalog = dbutils.widgets.get("catalog")
gold = f"{catalog}.gold"

# COMMAND ----------

emails = spark.table(f"{catalog}.silver.emails")
recipients = spark.table(f"{catalog}.silver.recipients")
flagged = add_flags(emails)

save_table(weekly_volume(flagged), f"{gold}.weekly_volume")
save_table(email_categories(flagged), f"{gold}.email_categories")
save_table(match_replies(emails), f"{gold}.reply_pairs")

reply_pairs = spark.table(f"{gold}.reply_pairs")
save_table(person_activity(flagged, recipients, reply_pairs), f"{gold}.person_activity")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Headline numbers

# COMMAND ----------

categories = spark.table(f"{catalog}.gold.email_categories")
totals = categories.agg(
    F.sum("total_emails").alias("emails"),
    F.round(F.sum("meeting_emails") / F.sum("total_emails"), 4).alias("meeting_share"),
    F.round(F.sum("after_hours_emails") / F.sum("total_emails"), 4).alias("after_hours_share"),
    F.round(F.sum("weekend_emails") / F.sum("total_emails"), 4).alias("weekend_share"),
)
display(totals)

display(
    reply_pairs.agg(
        F.count("*").alias("matched_replies"),
        F.round(F.median("response_minutes"), 1).alias("median_response_minutes"),
        F.round(F.avg((F.col("response_minutes") <= 120).cast("int")), 4).alias("share_within_2h"),
    )
)

# COMMAND ----------

display(
    spark.table(f"{catalog}.gold.person_activity")
    .filter("is_enron")
    .orderBy(F.desc("sent_count"))
    .limit(10)
)
