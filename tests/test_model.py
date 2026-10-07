from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from inbox_insights.features import FEATURE_COLUMNS, LABEL_COLUMN
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

START = datetime(2001, 1, 1, 9, 0)


def make_features(spark, n=200):
    """
    Synthetic feature rows where replies follow a clear pattern: short emails with
    a question that go to people who have replied before.
    """
    rows = []
    for i in range(n):
        replied = int(i % 5 == 0)
        row = {c: 0 for c in FEATURE_COLUMNS}
        row.update(
            message_id=f"m{i}",
            sent_at=START + timedelta(hours=i),
            text="can you check this today?" if replied else "weekly newsletter update",
            has_question=replied,
            prior_replies_from_recipients=3 * replied,
            recipient_count=1 if replied else 12,
            body_length=40 if replied else 900,
            **{LABEL_COLUMN: replied},
        )
        rows.append(row)
    return spark.createDataFrame(rows)


def test_time_split_puts_latest_emails_in_test(spark):
    df = make_features(spark)
    train, test, cutoff = time_split(df, test_fraction=0.2)

    latest_train = train.agg(F.max("sent_at")).first()[0]
    earliest_test = test.agg(F.min("sent_at")).first()[0]
    assert latest_train < earliest_test
    assert train.count() + test.count() == 200
    assert 30 <= test.count() <= 50
    assert cutoff.startswith("2001-01-")


def test_class_weights_balance_the_classes(spark):
    weighted = add_class_weights(make_features(spark))
    totals = {
        r[LABEL_COLUMN]: r.total
        for r in weighted.groupBy(LABEL_COLUMN).agg(F.sum("class_weight").alias("total")).collect()
    }
    assert totals[1] == pytest.approx(totals[0])


def test_pipeline_learns_an_obvious_pattern(spark):
    df = add_class_weights(make_features(spark))
    config = ModelConfig(text_features=256, min_doc_freq=1, max_iter=50)
    model = build_pipeline(config).fit(df)
    scored = with_score(model.transform(df))

    metrics = metrics_at(scored, F.col("score") >= 0.5)
    assert metrics.f1 > 0.9
    # Numeric block of the coefficient vector lines up with the names we log.
    assert model.stages[-1].numFeatures == len(numeric_feature_names()) + 256


def test_metrics_at_counts_correctly(spark):
    scored = spark.createDataFrame(
        [(1, 0.9), (1, 0.2), (0, 0.7), (0, 0.1), (0, 0.05)], f"{LABEL_COLUMN} int, score double"
    )
    m = metrics_at(scored, F.col("score") >= 0.5)

    assert (m.precision, m.recall, m.f1, m.positives_predicted) == (0.5, 0.5, 0.5, 2)


def test_choose_threshold_picks_best_f1(spark):
    scored = spark.createDataFrame(
        [(1, 0.35), (1, 0.4), (0, 0.3), (0, 0.1), (0, 0.6)], f"{LABEL_COLUMN} int, score double"
    )
    # 0.35 catches both positives with one false alarm (F1 0.8); 0.5 catches none.
    assert choose_threshold(scored, [0.35, 0.5]) == 0.35
