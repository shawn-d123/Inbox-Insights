# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Governance
# MAGIC Applies table and column comments, owners, layer tags and `pii = true` tags
# MAGIC from `src/inbox_insights/governance.py`. Runs last, because overwriting a
# MAGIC table can reset its column comments.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from inbox_insights.governance import TABLES, governance_statements

dbutils.widgets.text("catalog", "inbox_insights")
dbutils.widgets.text("owner", "")
catalog = dbutils.widgets.get("catalog")
owner = dbutils.widgets.get("owner") or spark.sql("SELECT current_user()").first()[0]

# COMMAND ----------

statements = governance_statements(catalog, owner)
for statement in statements:
    spark.sql(statement)
print(f"Applied {len(statements)} statements to {len(TABLES)} tables")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT schema_name, table_name, column_name, tag_value AS pii
        FROM {catalog}.information_schema.column_tags
        WHERE tag_name = 'pii'
        ORDER BY schema_name, table_name, column_name
        """
    )
)
