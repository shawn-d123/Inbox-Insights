# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze ingest
# MAGIC Loads the Kaggle `emails.csv` from the Volume into `bronze.raw_emails` without
# MAGIC changing it, plus the source path and load time for lineage.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "inbox_insights")
catalog = dbutils.widgets.get("catalog")
source_path = f"/Volumes/{catalog}/bronze/raw_files/emails.csv"

# Known size of the Kaggle file; a different count means the CSV was split wrongly.
EXPECTED_ROWS = 517_401

# COMMAND ----------

# Message bodies contain commas, quotes and newlines, so the CSV must be read as
# multi-line with '"' as the escape character, otherwise rows get split mid-email.
raw = (
    spark.read.option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .csv(source_path)
    .select(
        "file",
        "message",
        F.col("_metadata.file_path").alias("source_path"),
        F.current_timestamp().alias("ingested_at"),
    )
)

raw.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(
    f"{catalog}.bronze.raw_emails"
)

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.raw_emails")
row_count = bronze.count()
print(f"bronze.raw_emails rows: {row_count:,}")

display(
    bronze.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in ["file", "message"]]
    )
)

assert row_count == EXPECTED_ROWS, f"Expected {EXPECTED_ROWS:,} rows, got {row_count:,}"
