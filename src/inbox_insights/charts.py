"""README charts built from the gold tables (small pandas frames, matplotlib)."""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")  # no display on Databricks or CI

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.figure import Figure  # noqa: E402
from matplotlib.ticker import FuncFormatter, PercentFormatter  # noqa: E402

SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"
SERIES = ["#2a78d6", "#eb6834"]  # blue, orange: the first two categorical slots

# Reply-time buckets for the distribution chart, as (label, upper bound in minutes).
RESPONSE_BUCKETS = [
    ("< 15 min", 15),
    ("15-60 min", 60),
    ("1-2 h", 120),
    ("2-4 h", 240),
    ("4-8 h", 480),
    ("8-24 h", 1440),
    ("1-3 days", 4320),
    ("3-14 days", 20160),
]


def _new_chart(title: str, subtitle: str, size: tuple[float, float] = (9, 4.5)):
    """Figure with the shared look: light surface, recessive grid, left-aligned title."""
    fig, ax = plt.subplots(figsize=size, dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    fig.text(0.01, 0.97, title, fontsize=13, fontweight="bold", color=TEXT, va="top")
    fig.text(0.01, 0.905, subtitle, fontsize=9.5, color=TEXT_SECONDARY, va="top")

    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    return fig, ax


def _finish(fig: Figure) -> Figure:
    fig.tight_layout(rect=(0, 0, 1, 0.86))
    return fig


def _thousands(ax, axis: str = "y") -> None:
    formatter = FuncFormatter(lambda v, _: f"{v:,.0f}")
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(formatter)


def volume_chart(weekly: pd.DataFrame) -> Figure:
    """Stacked area of internal vs external emails per week."""
    fig, ax = _new_chart(
        "Weekly email volume",
        "Unique emails per week after de-duplication, Houston time",
    )
    ax.stackplot(
        weekly["week_start"],
        weekly["internal_emails"],
        weekly["external_emails"],
        colors=SERIES,
        labels=["Internal (Enron to Enron only)", "External"],
        edgecolor=SURFACE,
        linewidth=0.6,  # thin surface line keeps the two bands visually separate
    )
    _thousands(ax)
    ax.margins(x=0)
    ax.legend(loc="upper left", frameon=False, fontsize=9, labelcolor=TEXT)
    return _finish(fig)


def top_senders_chart(people: pd.DataFrame) -> Figure:
    """Horizontal bars of the busiest senders, largest at the top."""
    people = people.sort_values("sent_count")
    fig, ax = _new_chart(
        "Busiest senders",
        "Unique emails sent; automated accounts excluded",
        size=(9, 4.8),
    )
    names = people["person"].str.replace("@enron.com", "", regex=False)
    bars = ax.barh(names, people["sent_count"], color=SERIES[0], height=0.6)
    ax.bar_label(
        bars,
        labels=[f"{v:,}" for v in people["sent_count"]],
        padding=4,
        fontsize=8.5,
        color=TEXT_SECONDARY,
    )
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.tick_params(axis="y", labelcolor=TEXT)
    _thousands(ax, "x")
    return _finish(fig)


def bucket_response_times(minutes: pd.Series) -> pd.DataFrame:
    """Count replies per RESPONSE_BUCKETS bucket, in bucket order."""
    edges = [0] + [upper for _, upper in RESPONSE_BUCKETS]
    labels = [label for label, _ in RESPONSE_BUCKETS]
    buckets = pd.cut(minutes, bins=edges, labels=labels, right=False)
    counts = buckets.value_counts().reindex(labels, fill_value=0)
    return pd.DataFrame({"bucket": labels, "replies": counts.to_numpy()})


def response_time_chart(buckets: pd.DataFrame, median_minutes: float) -> Figure:
    """Bar chart of how long replies took, with the median called out."""
    fig, ax = _new_chart(
        "How long replies took",
        f"Matched replies by time to respond. Median: {median_minutes:.0f} minutes",
    )
    bars = ax.bar(buckets["bucket"], buckets["replies"], color=SERIES[0], width=0.65)
    share = buckets["replies"] / buckets["replies"].sum()
    ax.bar_label(
        bars, labels=[f"{s:.0%}" for s in share], padding=3, fontsize=8.5, color=TEXT_SECONDARY
    )
    _thousands(ax)
    ax.tick_params(axis="x", labelcolor=TEXT)
    return _finish(fig)


def monthly_share_chart(monthly: pd.DataFrame, column: str, title: str, subtitle: str) -> Figure:
    """
    Line of a monthly share (0-1) with a dashed average line.

    The average is stated in the subtitle rather than labelled on the plot, where it
    would collide with the line wherever the two cross.
    """
    # Weighted by volume, so quiet months do not pull the average around.
    average = (monthly[column] * monthly["total_emails"]).sum() / monthly["total_emails"].sum()
    fig, ax = _new_chart(
        title, f"{subtitle}. Dashed line: average across the months shown ({average:.1%})"
    )
    ax.plot(monthly["month"], monthly[column], color=SERIES[0], linewidth=2)
    ax.axhline(average, color=TEXT_SECONDARY, linewidth=1, linestyle=(0, (4, 3)))
    ax.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    ax.set_ylim(bottom=0)
    ax.margins(x=0)
    return _finish(fig)
