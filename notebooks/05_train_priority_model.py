# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Train priority model
# MAGIC Logistic regression predicting whether an email gets a reply within 2 hours.
# MAGIC
# MAGIC * Time-based split: train on earlier email, test on the latest 20%.
# MAGIC * The decision threshold is picked on a validation slice at the end of the
# MAGIC   training period, never on the test set.
# MAGIC * Compared against "always no reply" and a one-line rule.
# MAGIC * Everything is tracked in MLflow; results are also appended to
# MAGIC   `gold.model_metrics` so README numbers can be traced to a run.

# COMMAND ----------

import os
import sys
from dataclasses import asdict

sys.path.append(os.path.abspath("../src"))

import mlflow
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F

from inbox_insights.features import LABEL_COLUMN
from inbox_insights.model import (
    ModelConfig,
    add_class_weights,
    build_pipeline,
    choose_threshold,
    metrics_at,
    numeric_feature_names,
    time_split,
    with_score,
)

dbutils.widgets.text("catalog", "inbox_insights")
catalog = dbutils.widgets.get("catalog")

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/inbox-insights-priority-model")

# COMMAND ----------

config = ModelConfig()
features = spark.table(f"{catalog}.gold.message_features")

train, test, test_cutoff = time_split(features, config.test_fraction)
# The validation slice is the last part of the training period.
fit_part, validation, validation_cutoff = time_split(
    train, config.validation_fraction / (1 - config.test_fraction)
)

sizes = {
    "train_rows": train.count(),
    "test_rows": test.count(),
    "validation_rows": validation.count(),
}
positive_rates = {
    f"{name}_positive_rate": df.agg(F.avg(LABEL_COLUMN)).first()[0]
    for name, df in {"train": train, "test": test}.items()
}
print(sizes, positive_rates, f"test from {test_cutoff}")

# COMMAND ----------

pipeline = build_pipeline(config)
pr_auc = BinaryClassificationEvaluator(labelCol=LABEL_COLUMN, metricName="areaUnderPR")
roc_auc = BinaryClassificationEvaluator(labelCol=LABEL_COLUMN, metricName="areaUnderROC")

with mlflow.start_run(run_name="logistic_regression_tfidf") as run:
    mlflow.log_params(
        {
            **asdict(config),
            **sizes,
            "test_cutoff": test_cutoff,
            "validation_cutoff": validation_cutoff,
        }
    )
    mlflow.log_metrics(positive_rates)

    # 1. Fit on the earlier part of training and tune the threshold on validation.
    tuning_model = pipeline.fit(add_class_weights(fit_part))
    threshold = choose_threshold(with_score(tuning_model.transform(validation)))

    # 2. Refit on the full training period and score the untouched test set once.
    model = pipeline.fit(add_class_weights(train))
    scored = with_score(model.transform(test))

    results = {
        "model": metrics_at(scored, F.col("score") >= threshold, threshold),
        "model_at_0.5": metrics_at(scored, F.col("score") >= 0.5, 0.5),
        "baseline_always_no": metrics_at(scored, F.lit(False)),
        "baseline_prior_reply_rule": metrics_at(scored, F.col("prior_replies_from_recipients") > 0),
    }
    test_pr_auc = pr_auc.evaluate(scored)
    test_roc_auc = roc_auc.evaluate(scored)

    mlflow.log_metrics(
        {
            "decision_threshold": threshold,
            "test_pr_auc": test_pr_auc,
            "test_roc_auc": test_roc_auc,
            **{f"{name}_{k}": v for name, m in results.items() for k, v in asdict(m).items()},
        }
    )

    # Weights of the numeric features (they are standardised, so comparable).
    coefficients = model.stages[-1].coefficients.toArray()
    numeric_weights = dict(zip(numeric_feature_names(), map(float, coefficients), strict=False))
    mlflow.log_dict(numeric_weights, "numeric_weights.json")

    mlflow.spark.log_model(
        model, artifact_path="model", dfs_tmpdir=f"/Volumes/{catalog}/gold/ml_artifacts/tmp"
    )
    run_id = run.info.run_id

# COMMAND ----------

metric_rows = [
    (run_id, test_cutoff, name, m.threshold, m.precision, m.recall, m.f1, m.positives_predicted)
    for name, m in results.items()
]
metrics_df = spark.createDataFrame(
    metric_rows,
    "run_id string, test_from string, model string, threshold double, precision double, "
    "recall double, f1 double, positives_predicted long",
).withColumns(
    {
        "test_pr_auc": F.when(F.col("model").startswith("model"), F.lit(test_pr_auc)),
        "test_roc_auc": F.when(F.col("model").startswith("model"), F.lit(test_roc_auc)),
        "test_rows": F.lit(sizes["test_rows"]),
        "test_positive_rate": F.lit(positive_rates["test_positive_rate"]),
        "trained_at": F.current_timestamp(),
    }
)
metrics_df.write.mode("append").saveAsTable(f"{catalog}.gold.model_metrics")
display(metrics_df)

# COMMAND ----------

ranked_weights = sorted(numeric_weights.items(), key=lambda kv: -abs(kv[1]))
display(spark.createDataFrame(ranked_weights, "feature string, weight double"))

# COMMAND ----------

# An F1 near 1.0 on this task would almost certainly mean label leakage.
assert results["model"].f1 < 0.95, "Suspiciously high F1: check for leakage"
