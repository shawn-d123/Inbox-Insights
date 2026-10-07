# Inbox Insights

A PySpark lakehouse on Databricks built over the Enron email corpus (about 517k
emails). It answers two questions: where does an organisation's time go in
email, and which messages actually need attention?

Follow-on from my [Email Calendar Evaluation Pipeline](https://github.com/shawn-d123/email-calendar-evaluation-pipeline),
rebuilt at full scale with Spark, Delta Lake, Unity Catalog and MLflow.

Work in progress.

## Local setup

Needs Python 3.11+ and Java 17.

```bash
python -m venv .venv
pip install -e ".[dev]"
pytest
```

To make the 2,000-row development sample from the Kaggle `emails.csv`:

```bash
python scripts/make_sample.py path/to/emails.csv
```
