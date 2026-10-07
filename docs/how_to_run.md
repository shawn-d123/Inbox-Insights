# How to run

The pipeline runs on Databricks (Free Edition works). All logic lives in
`src/inbox_insights/` and is unit tested locally; the notebooks in `notebooks/` only
call it and write tables.

## 1. Local setup and tests

Needs Python 3.11+ and Java 17 (PySpark runs a local JVM).

```bash
python -m venv .venv
.venv/Scripts/activate        # Windows; use source .venv/bin/activate elsewhere
pip install -e ".[dev]"
pytest
```

The same lint and test steps run in GitHub Actions on every push
(`.github/workflows/ci.yml`).

## 2. Get the data

Download the [Enron email dataset](https://www.kaggle.com/datasets/wcukierski/enron-email-dataset)
from Kaggle and unzip `emails.csv` (about 1.4 GB, 517,401 rows). Keep it outside the
repo; `data/` is git-ignored.

For quick local experiments, make a 2,000-row sample:

```bash
python scripts/make_sample.py path/to/emails.csv
```

## 3. Databricks setup (once)

1. Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) and log in:

   ```bash
   databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile DEFAULT
   ```

2. Create a catalog called `inbox_insights` in **Catalog > + > Create a catalog**.
   Free Edition only allows catalogs on default storage to be created in the UI.

3. Create the schemas and Volumes. The first task of the job does this too, but the
   Volume has to exist before the CSV can be uploaded:

   ```bash
   databricks schemas create bronze inbox_insights
   databricks volumes create inbox_insights bronze raw_files MANAGED
   ```

4. Upload the CSV:

   ```bash
   databricks fs cp path/to/emails.csv dbfs:/Volumes/inbox_insights/bronze/raw_files/emails.csv
   ```

## 4. Deploy and run the job

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run inbox_insights_pipeline
```

The job (`resources/job.yml`) runs these notebooks in order on serverless compute:

| Task | Notebook | Output |
|---|---|---|
| setup | `00_setup` | Schemas and Volumes |
| bronze_ingest | `01_bronze_ingest` | `bronze.raw_emails` |
| silver_clean | `02_silver_clean` | `silver.emails`, `silver.recipients`, `silver.dq_quarantine`, `silver.run_metrics` |
| gold_analytics | `03_gold_analytics` | `gold.weekly_volume`, `gold.email_categories`, `gold.reply_pairs`, `gold.person_activity` |
| gold_features | `04_gold_features` | `gold.message_features` |
| train_priority_model | `05_train_priority_model` | MLflow run, `gold.model_metrics` |
| governance | `06_governance` | Comments, owners, layer and PII tags |
| export_findings | `07_export_findings` | Charts and `findings.json` in the `gold.reports` Volume |

A full run takes about 20 minutes. To rerun only part of it:

```bash
databricks bundle run inbox_insights_pipeline --only gold_features,train_priority_model
```

The job has a weekly schedule (Mondays 06:00 Houston time) that is **paused**, because
Free Edition has a daily compute quota. Unpause it in the Jobs UI if you want it to run
on its own.

## 5. Refresh the README charts

After a run, copy the exported charts and numbers into the repo:

```bash
databricks fs cp -r dbfs:/Volumes/inbox_insights/gold/reports docs/images --overwrite
```

`docs/images/findings.json` holds every number quoted in the README.

## 6. Other scripts

| Script | What it does |
|---|---|
| `scripts/evaluate_meeting_rule.py` | Scores the meeting rule on the 20 labelled emails in `benchmarks/` |
| `scripts/write_data_dictionary.py` | Regenerates `docs/data_dictionary.md` from `governance.py` |
