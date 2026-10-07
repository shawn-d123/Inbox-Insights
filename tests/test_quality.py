from pyspark.sql import functions as F

from inbox_insights.quality import split_by_quality


def make_emails(spark, rows):
    # Dates go in as strings (and are never collected back) because Python on
    # Windows cannot convert a year-1 timestamp.
    schema = "file string, message_id string, sent_at string, sender string"
    return spark.createDataFrame(rows, schema).withColumn("sent_at", F.to_timestamp("sent_at"))


def test_good_rows_pass_and_bad_rows_get_every_reason(spark):
    rows = [
        ("a/1.", "id-1", "2001-05-14 12:00:00", "a@enron.com"),
        ("a/2.", None, "2001-05-14 12:00:00", "a@enron.com"),
        ("a/3.", "id-3", None, "a@enron.com"),
        ("a/4.", "id-4", "0001-12-24 22:00:00", "a@enron.com"),
        ("a/5.", "id-5", "2044-01-01 00:00:00", None),
    ]
    passed, quarantined = split_by_quality(make_emails(spark, rows))

    assert [r.file for r in passed.collect()] == ["a/1."]
    assert "reason" not in passed.columns

    reasons = {r.file: r.reason for r in quarantined.select("file", "reason").collect()}
    assert reasons == {
        "a/2.": "missing_message_id",
        "a/3.": "unparseable_date",
        "a/4.": "date_out_of_range",
        "a/5.": "date_out_of_range; missing_sender",
    }
    assert quarantined.filter("quarantined_at is null").count() == 0


def test_boundary_years_are_kept(spark):
    rows = [
        ("a/1.", "id-1", "1998-01-01 00:00:00", "a@enron.com"),
        ("a/2.", "id-2", "2002-12-31 23:00:00", "a@enron.com"),
    ]
    passed, quarantined = split_by_quality(make_emails(spark, rows))

    assert passed.count() == 2
    assert quarantined.count() == 0
