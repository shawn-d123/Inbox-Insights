# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Gold features
# MAGIC Builds `gold.message_features`: one row per email sent to a mailbox owner, with
# MAGIC point-in-time features and the label "a recipient replied within 2 hours".
# MAGIC TF-IDF is not computed here; it is fitted on the training split in notebook 05
# MAGIC so test-set word statistics cannot leak into training.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F

from inbox_insights.features import FEATURE_COLUMNS, LABEL_COLUMN, build_message_features
from inbox_insights.frames import save_table

dbutils.widgets.text("catalog", "inbox_insights")
catalog = dbutils.widgets.get("catalog")
table = f"{catalog}.gold.message_features"

# COMMAND ----------

features = build_message_features(
    spark.table(f"{catalog}.silver.emails"),
    spark.table(f"{catalog}.silver.recipients"),
    spark.table(f"{catalog}.gold.reply_pairs"),
)
save_table(features, table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

saved = spark.table(table)
row_count = saved.count()
print(f"message_features rows: {row_count:,}")

null_counts = saved.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in FEATURE_COLUMNS]
).first()
nulls = {c: v for c, v in null_counts.asDict().items() if v}
assert not nulls, f"Null feature values: {nulls}"

display(
    saved.groupBy(LABEL_COLUMN).count().withColumn("share", F.round(F.col("count") / row_count, 4))
)

# COMMAND ----------

display(saved.select(FEATURE_COLUMNS).summary("mean", "min", "max"))
