# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Creates the schemas and the Volume that holds the raw CSV. Safe to rerun.
# MAGIC
# MAGIC The catalog itself is created once in the UI, because Free Edition only allows
# MAGIC catalogs on default storage to be created there.

# COMMAND ----------

dbutils.widgets.text("catalog", "inbox_insights")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")

for schema, comment in {
    "bronze": "Raw data exactly as ingested",
    "silver": "Cleaned, typed and de-duplicated emails",
    "gold": "Analytics and ML feature tables",
}.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{comment}'")

spark.sql(
    "CREATE VOLUME IF NOT EXISTS bronze.raw_files "
    "COMMENT 'Source files, including the Kaggle Enron emails.csv'"
)

spark.sql(
    "CREATE VOLUME IF NOT EXISTS gold.ml_artifacts "
    "COMMENT 'Scratch space MLflow uses when saving Spark ML models'"
)

spark.sql(
    "CREATE VOLUME IF NOT EXISTS gold.reports "
    "COMMENT 'Charts and headline numbers exported for the README'"
)

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
