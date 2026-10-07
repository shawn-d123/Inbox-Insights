import pandas as pd
from matplotlib.figure import Figure

from inbox_insights.charts import (
    bucket_response_times,
    monthly_share_chart,
    response_time_chart,
    top_senders_chart,
    volume_chart,
)


def test_bucket_response_times_counts_into_ordered_buckets():
    minutes = pd.Series([1, 14.9, 15, 59, 61, 125, 3000, 10_000])
    buckets = bucket_response_times(minutes)

    assert list(buckets["bucket"])[:3] == ["< 15 min", "15-60 min", "1-2 h"]
    assert dict(zip(buckets["bucket"], buckets["replies"], strict=True)) == {
        "< 15 min": 2,
        "15-60 min": 2,
        "1-2 h": 1,
        "2-4 h": 1,
        "4-8 h": 0,
        "8-24 h": 0,
        "1-3 days": 1,
        "3-14 days": 1,
    }


def test_charts_render():
    weeks = pd.date_range("2001-01-01", periods=10, freq="W-MON")
    months = pd.date_range("2001-01-01", periods=6, freq="MS")
    weekly = pd.DataFrame({"week_start": weeks, "internal_emails": 10, "external_emails": 5})
    monthly = pd.DataFrame({"month": months, "total_emails": 100, "meeting_share": 0.2})
    people = pd.DataFrame({"person": ["a@enron.com", "b@enron.com"], "sent_count": [50, 80]})

    figures = [
        volume_chart(weekly),
        top_senders_chart(people),
        response_time_chart(bucket_response_times(pd.Series([5, 30, 90])), 30),
        monthly_share_chart(monthly, "meeting_share", "Meetings", "Share of email"),
    ]
    assert all(isinstance(f, Figure) for f in figures)
