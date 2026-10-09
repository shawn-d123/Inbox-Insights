# Inbox Insights

**Where does an organisation's time go in email, and which messages actually need attention?**

A medallion lakehouse on Databricks that takes the full Enron email corpus from one messy
CSV to governed Delta tables, a reusable ML feature table and an MLflow-tracked priority
model.

![CI](https://github.com/shawn-d123/Inbox-Insights/actions/workflows/ci.yml/badge.svg)

| | |
|---|---|
| **Data** | Enron email corpus: 517,401 raw emails from about 150 employees, 1998-2002 |
| **Stack** | PySpark, Databricks (serverless), Delta Lake, Unity Catalog, MLflow, Spark ML, Asset Bundles |
| **Output** | 11 governed tables, 5 charts, a feature table and a priority model |
| **Run time** | About 15 minutes end to end as one Databricks Job |
| **Quality** | 91 pytest tests and `ruff` checks in GitHub Actions on every push |

This rebuilds my earlier
[Email Calendar Evaluation Pipeline](https://github.com/shawn-d123/email-calendar-evaluation-pipeline),
which handled 1,000 emails in pandas, at full scale. Its rule-based meeting detector is
reused here as a Spark expression.

**Contents:** [Findings](#findings) · [Pipeline](#pipeline) ·
[Data problems](#data-problems-i-had-to-solve) · [Priority model](#priority-model) ·
[How it is built](#how-it-is-built) · [Running it](#running-it)

---

## Findings

Based on the 254,411 unique emails left after cleaning and de-duplication.

| Finding | Number | What it means |
|---|---|---|
| Duplicate copies removed | **261,677 (50.7%)** | Half the raw corpus is the same email stored in several folders and mailboxes |
| Email that stayed inside Enron | **61.8%** | Sent by an Enron address to Enron addresses only |
| Email about meetings | **17.1%** | About one email in six is arranging, moving or cancelling a meeting |
| Sent after hours | **17.7%** | Before 8am, from 7pm or at weekends, Houston time |
| Sent at weekends | **3.9%** | |
| Busiest week | **9,820 emails** | Week of 22 October 2001, when the SEC inquiry became public |
| Replies matched | **27,204** | Replies linked back to the email they answer |
| Median time to reply | **102 minutes** | 52.5% of replies came within 2 hours |

After-hours email climbed through 2001 as the company went under, and while most replies
were quick, there is a long tail: 15% took between 8 and 24 hours, roughly overnight.

![Weekly email volume](docs/images/weekly_volume.png)

<table>
  <tr>
    <td><img src="docs/images/response_times.png" alt="How long replies took"></td>
    <td><img src="docs/images/after_hours_share.png" alt="Email sent after hours"></td>
  </tr>
  <tr>
    <td><img src="docs/images/meeting_share.png" alt="Email about meetings"></td>
    <td><img src="docs/images/top_senders.png" alt="Busiest senders"></td>
  </tr>
</table>

### Busiest senders

| Sender | Emails sent | Median reply time |
|---|---|---|
| Jeff Dasovich | 5,524 | 117 min |
| Kay Mann | 4,865 | 58 min |
| Sara Shackleton | 4,442 | 108 min |
| Tana Jones | 4,157 | **35 min** (quickest of the top ten) |
| Vince Kaminski | 3,953 | 274 min |

One automated account (`pete.davis`, a schedule-monitoring mailbox) is left out of the
senders chart.

Every number above comes from [`docs/images/findings.json`](docs/images/findings.json),
which the pipeline writes on every run.

---

## Pipeline

```mermaid
flowchart LR
    csv[("emails.csv<br/>UC Volume")] --> bronze

    subgraph Bronze
        bronze["raw_emails<br/>517,401 rows, untouched"]
    end

    subgraph Silver
        emails["emails<br/>254,411 unique"]
        recipients["recipients<br/>To + Cc"]
        quarantine["dq_quarantine<br/>1,313 rows + reason"]
    end

    subgraph Gold
        weekly["weekly_volume"]
        categories["email_categories"]
        pairs["reply_pairs"]
        people["person_activity"]
        features["message_features"]
    end

    bronze --> emails & recipients & quarantine
    emails --> weekly & categories & pairs
    pairs --> people & features
    emails & recipients --> features
    features --> model(["Priority model<br/>MLflow"])
```

### Tables

| Layer | Table | Rows | What it holds |
|---|---|---|---|
| Bronze | `raw_emails` | 517,401 | The CSV exactly as loaded, plus source path and load time |
| Silver | `emails` | 254,411 | Parsed, cleaned, time-corrected, de-duplicated emails |
| Silver | `recipients` | 1,373,871 | One row per email and To/Cc recipient |
| Silver | `dq_quarantine` | 1,313 | Rows that failed a quality rule, with the reason |
| Silver | `run_metrics` | per run | Row counts at every stage of every run |
| Gold | `weekly_volume` | per week | Emails per week, internal vs external |
| Gold | `email_categories` | per month | Meeting, after-hours and weekend shares |
| Gold | `reply_pairs` | 27,204 | Each reply matched to the email it answers |
| Gold | `person_activity` | per address | Volume, after-hours load and reply speed |
| Gold | `message_features` | 89,457 | ML features and label, one row per email |
| Gold | `model_metrics` | per run | Test results for the model and baselines |

### Row counts through the pipeline

| Stage | Rows | Change |
|---|---|---|
| Raw CSV loaded into bronze | 517,401 | |
| Failed a quality check (dates outside 1998-2002) | 1,313 | quarantined with a reason, not dropped |
| Passed quality checks | 516,088 | |
| After de-duplicating on Message-ID | 516,088 | none removed |
| After de-duplicating on content | **254,411** | 261,677 copies removed |

### Governance

| What | Detail |
|---|---|
| Descriptions | Every table, and every gold column, has a comment in Unity Catalog |
| Ownership | Every table has an owner and a `layer` tag |
| PII | All 17 columns holding email addresses or message text are tagged `pii = true` |
| Documentation | [Data dictionary](docs/data_dictionary.md), generated from the same source as the catalogue comments, so the two cannot drift apart |
| Lineage | Tracked by Unity Catalog from bronze through to the feature table (below) |

![Lineage from bronze to features](docs/images/lineage.png)

---

## Data problems I had to solve

Most of the time on this project went into finding out what was wrong with the data.

| Problem | How I found it | What I did | Impact |
|---|---|---|---|
| **Half the corpus is copies** | De-duplicating on Message-ID removed nothing; every copy of an email has its own ID | Second pass on sender, send time, subject and a hash of the body | 261,677 duplicates removed |
| **Lotus Notes timestamps are shifted** | Header times compared with the "Forwarded by ... on" stamps Lotus writes into the body: across about 3,500 emails the body stamp was always exactly 7 hours later (8 in winter) | For the 118,940 emails exported from Lotus Notes, the header's UTC value is treated as Houston local time; Outlook exports were already correct | After-hours share went from 45% (development sample) to a realistic 17.7% |
| **Bcc copies Cc** | The Bcc header matched Cc on every email that had one | Recipients taken from To and Cc only | No double-counted recipients |
| **Quoted text hides the real message** | Most bodies carried old replies and forwarded chains | Bodies cut at `Original Message`, Lotus "Forwarded by" blocks, `From:`/`To:` header blocks and signatures | Average body length halved; the meeting flag still uses the full body, since a forwarded meeting notice is still about a meeting |
| **Regex behaves differently on serverless** | The first full run quarantined every email as `unparseable_date`: under the multi-line flag, `.` matched newlines, so the Date header swallowed every header after it | Patterns rewritten to avoid `.` and `$` for line matching; the job now fails if more than 5% of rows are quarantined | Caught by the quarantine before an empty silver table went out |

### Meeting detector compared with the old project

Scored on the same 20 hand-labelled Enron emails from the earlier project
([`benchmarks/`](benchmarks)):

| Detector | Precision | Recall | F1 |
|---|---|---|---|
| Original pandas rule | 0.857 | 0.857 | 0.857 |
| Spark rule (full body) | 1.000 | 0.857 | **0.923** |

Twenty emails is a sanity check rather than a proper benchmark, and the rule was not tuned
to them.

---

## Priority model

**Question:** will anyone reply to this email within 2 hours?

### Setup

| | |
|---|---|
| **Rows** | 89,457 emails sent to one of the mailbox owners: only for them can a missing reply be trusted, because their sent mail is in the corpus |
| **Label** | A matched reply within 120 minutes (7.75% of rows) |
| **Features** | 22 point-in-time features plus TF-IDF on the subject and body |
| **Model** | Spark ML logistic regression with class weights, tracked in MLflow |
| **Validation** | 22 October to 19 November 2001, used only to choose the decision threshold |
| **Test** | The 17,966 emails after 19 November 2001 (11.2% positive), scored once |

| Feature group | Examples |
|---|---|
| Content | Body length, word count, number of recipients, is a reply, contains a question, position in thread |
| Behaviour | Sender's earlier email count, earlier emails to these recipients, earlier replies from these recipients |
| Time | Hour and weekday sent (Houston time), after hours |
| Text | Urgency keywords, meeting flag, TF-IDF |

### Keeping it honest

| Risk | Guard |
|---|---|
| Label leaking into features | Nothing used to build the label is a feature; the build fails if a label column is added |
| Looking into the future | Every history feature counts only earlier emails; a test adds future emails and checks no earlier feature changes |
| Test set shaping the text features | TF-IDF is fitted on the training period only |
| Optimistic split | Split by time rather than at random, and the test set is scored once |

### Results on the test period

| Approach | Precision | Recall | F1 |
|---|---|---|---|
| Always predict "no reply" | 0.000 | 0.000 | 0.000 |
| Rule: these recipients have replied to this sender before | 0.170 | 0.664 | 0.271 |
| Logistic regression, threshold 0.50 | 0.250 | 0.252 | 0.251 |
| **Logistic regression, threshold 0.25 (chosen on validation)** | **0.208** | **0.444** | **0.283** |

PR-AUC is 0.204 against a base rate of 0.112, and ROC-AUC is 0.685.

The model picks up real signal: it beats the simple rule while flagging far fewer emails,
and its precision-recall curve sits well above chance. It is still a weak predictor, and I
think that is the honest result. Most of what decides whether someone replies quickly is
not in the email itself: whether they are at their desk, or picked up the phone instead.
The test period also covers the bankruptcy and its aftermath, a very different few months
from anything the model trained on.

---

## How it is built

The pipeline logic lives in [`src/inbox_insights/`](src/inbox_insights) as plain functions
that take and return DataFrames. The notebooks only call those functions and write tables,
so the same code is unit tested locally and runs on Databricks.

| Module | Job |
|---|---|
| [`parsing.py`](src/inbox_insights/parsing.py) | Split raw messages into headers and body, parse dates and addresses |
| [`cleaning.py`](src/inbox_insights/cleaning.py) | Strip quoted replies and signatures, decode quoted-printable, fix time zones |
| [`quality.py`](src/inbox_insights/quality.py) | Quality rules; failing rows go to quarantine with a reason |
| [`silver.py`](src/inbox_insights/silver.py) | De-duplication and the recipients table |
| [`rules.py`](src/inbox_insights/rules.py) | Meeting, after-hours and internal flags |
| [`gold.py`](src/inbox_insights/gold.py) | Weekly volume, categories, reply matching, person activity |
| [`features.py`](src/inbox_insights/features.py) | Point-in-time features and the reply label |
| [`model.py`](src/inbox_insights/model.py) | Time split, Spark ML pipeline, threshold choice, metrics |
| [`governance.py`](src/inbox_insights/governance.py) | Comments and tags, and the source of the data dictionary |
| [`charts.py`](src/inbox_insights/charts.py) | The charts in this README |

### The Databricks Job

Defined in [`resources/job.yml`](resources/job.yml) and deployed with Databricks Asset
Bundles. The weekly schedule is set up but paused, because Free Edition has a daily compute
quota.

| # | Task | Produces | Time |
|---|---|---|---|
| 1 | `setup` | Schemas and Volumes | 22s |
| 2 | `bronze_ingest` | `bronze.raw_emails` | 38s |
| 3 | `silver_clean` | Silver tables and quarantine | 6m 35s |
| 4 | `gold_analytics` | Weekly volume, categories, reply pairs, person activity | 33s |
| 5 | `gold_features` | `gold.message_features` | 32s |
| 6 | `train_priority_model` | MLflow run and `gold.model_metrics` | 4m 16s |
| 7 | `governance` | Comments, owners and PII tags | 1m 58s |
| 8 | `export_findings` | Charts and `findings.json` | 17s |

![Successful job run](docs/images/job_run.png)

---

## Running it

Full steps are in [`docs/how_to_run.md`](docs/how_to_run.md). In short:

```bash
pip install -e ".[dev]"
pytest
databricks bundle deploy
databricks bundle run inbox_insights_pipeline
```

### Repo layout

| Folder | Contents |
|---|---|
| [`src/inbox_insights/`](src/inbox_insights) | Pipeline logic, all unit tested |
| [`notebooks/`](notebooks) | `00_setup` to `07_export_findings`, thin wrappers the job runs |
| [`resources/`](resources) | Databricks Job definition |
| [`tests/`](tests) | pytest suite on a local SparkSession |
| [`benchmarks/`](benchmarks) | 20 labelled emails for the meeting rule |
| [`scripts/`](scripts) | Sampling, benchmark and data dictionary scripts |
| [`docs/`](docs) | Data dictionary, how-to-run guide and images |

---

## What I would do next

| Idea | Why |
|---|---|
| Lakeflow Declarative Pipelines | Express the quality rules as expectations instead of custom code |
| Auto Loader | Process new email incrementally instead of rebuilding everything |
| Gradient-boosted model and recipient-level features | For example, how busy the recipient was that day |
| Model Serving endpoint | Score new email as it arrives |

## Licence

MIT. The Enron corpus is public and was released by the US Federal Energy Regulatory
Commission.
