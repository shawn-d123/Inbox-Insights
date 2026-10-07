"""Train and evaluate the message priority classifier (replied to within 2 hours)."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import (
    IDF,
    HashingTF,
    SQLTransformer,
    StandardScaler,
    StopWordsRemover,
    Tokenizer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from inbox_insights.features import FEATURE_COLUMNS, LABEL_COLUMN

# Long-tailed counts (a sender with 5,000 prior emails vs one with 5) are log-scaled
# so a handful of very active people do not dominate the coefficients.
COUNT_FEATURES = [
    "body_length",
    "word_count",
    "to_count",
    "cc_count",
    "recipient_count",
    "thread_position",
    "sender_prior_emails",
    "prior_emails_to_recipients",
    "prior_replies_from_recipients",
]


@dataclass(frozen=True)
class ModelConfig:
    """Hyperparameters, kept in one place so they can be logged to MLflow as-is."""

    text_features: int = 2**16
    min_doc_freq: int = 5
    reg_param: float = 0.01
    elastic_net: float = 0.0
    max_iter: int = 100
    test_fraction: float = 0.2
    validation_fraction: float = 0.1


@dataclass(frozen=True)
class Metrics:
    threshold: float
    precision: float
    recall: float
    f1: float
    positives_predicted: int


def time_split(df: DataFrame, test_fraction: float) -> tuple[DataFrame, DataFrame, str]:
    """
    Split by send time: the latest `test_fraction` of emails become the test set.

    A random split would let the model learn from emails sent after the ones it is
    tested on, which overstates how well it would do on new mail.
    """
    cutoff_epoch = df.select(F.unix_timestamp("sent_at").alias("t")).approxQuantile(
        "t", [1 - test_fraction], 0.001
    )[0]
    cutoff = F.timestamp_seconds(F.lit(int(cutoff_epoch)))
    train = df.filter(F.col("sent_at") < cutoff)
    test = df.filter(F.col("sent_at") >= cutoff)
    cutoff_text = df.select(F.date_format(cutoff, "yyyy-MM-dd HH:mm:ss")).first()[0]
    return train, test, cutoff_text


def add_class_weights(train: DataFrame) -> DataFrame:
    """
    Weight positives by the negative:positive ratio so both classes count equally.

    Without this, with under 10% positives the cheapest fit is to predict "no
    reply" almost everywhere.
    """
    positive_rate = train.agg(F.avg(LABEL_COLUMN)).first()[0]
    positive_weight = (1 - positive_rate) / positive_rate
    return train.withColumn(
        "class_weight",
        F.when(F.col(LABEL_COLUMN) == 1, F.lit(positive_weight)).otherwise(F.lit(1.0)),
    )


def build_pipeline(config: ModelConfig) -> Pipeline:
    """TF-IDF on the text plus scaled numeric features, into a logistic regression."""
    log_columns = ", ".join(f"log1p({c}) AS {c}_log" for c in COUNT_FEATURES)
    numeric_inputs = [f"{c}_log" for c in COUNT_FEATURES] + [
        c for c in FEATURE_COLUMNS if c not in COUNT_FEATURES
    ]

    stages = [
        SQLTransformer(statement=f"SELECT *, {log_columns} FROM __THIS__"),
        Tokenizer(inputCol="text", outputCol="tokens"),
        StopWordsRemover(inputCol="tokens", outputCol="words"),
        HashingTF(inputCol="words", outputCol="tf", numFeatures=config.text_features),
        IDF(inputCol="tf", outputCol="tfidf", minDocFreq=config.min_doc_freq),
        VectorAssembler(inputCols=numeric_inputs, outputCol="numeric_raw"),
        StandardScaler(inputCol="numeric_raw", outputCol="numeric", withMean=False),
        VectorAssembler(inputCols=["numeric", "tfidf"], outputCol="features"),
        LogisticRegression(
            featuresCol="features",
            labelCol=LABEL_COLUMN,
            weightCol="class_weight",
            regParam=config.reg_param,
            elasticNetParam=config.elastic_net,
            maxIter=config.max_iter,
        ),
    ]
    return Pipeline(stages=stages)


def numeric_feature_names() -> list[str]:
    """Names in the same order as the numeric block of the assembled feature vector."""
    return [f"{c}_log" for c in COUNT_FEATURES] + [
        c for c in FEATURE_COLUMNS if c not in COUNT_FEATURES
    ]


def with_score(predictions: DataFrame) -> DataFrame:
    """Add `score`, the predicted probability of a reply within 2 hours."""
    return predictions.withColumn("score", vector_to_array("probability").getItem(1))


def metrics_at(scored: DataFrame, prediction: Column, threshold: float = 0.5) -> Metrics:
    """Precision, recall and F1 for the positive class, for any 0/1 prediction column."""
    label = F.col(LABEL_COLUMN) == 1
    counts = scored.agg(
        F.sum((prediction & label).cast("int")).alias("tp"),
        F.sum((prediction & ~label).cast("int")).alias("fp"),
        F.sum((~prediction & label).cast("int")).alias("fn"),
    ).first()
    tp, fp, fn = counts.tp or 0, counts.fp or 0, counts.fn or 0

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * tp / (2 * tp + fp + fn) if tp else 0.0
    return Metrics(threshold, precision, recall, f1, tp + fp)


def choose_threshold(scored: DataFrame, candidates: list[float] | None = None) -> float:
    """
    Pick the probability cut-off with the best F1 on validation data.

    Class weighting shifts probabilities upwards, so 0.5 is rarely the best cut-off.
    This must only ever see validation rows, never the test set.
    """
    candidates = candidates or [round(0.05 * i, 2) for i in range(2, 20)]
    results = [metrics_at(scored, F.col("score") >= t, t) for t in candidates]
    return max(results, key=lambda m: m.f1).threshold
