"""Build a small random sample of the Kaggle Enron emails.csv for local development.

The full file is about 1.4 GB, so it is streamed row by row rather than loaded
into memory. Usage:

    python scripts/make_sample.py path/to/emails.csv --rows 2000
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from collections.abc import Iterable
from pathlib import Path

DEFAULT_OUTPUT = Path("data") / "sample_emails.csv"


def reservoir_sample(rows: Iterable[list[str]], k: int, seed: int = 42) -> list[list[str]]:
    """
    Pick k rows uniformly at random from an iterable of unknown length.

    Uses reservoir sampling so only k rows are ever held in memory, and a fixed
    seed so the same sample comes out every time.
    """
    rng = random.Random(seed)
    sample: list[list[str]] = []

    for index, row in enumerate(rows):
        if index < k:
            sample.append(row)
        else:
            # Replace an existing row with probability k / (index + 1), which keeps
            # every row seen so far equally likely to be in the sample.
            slot = rng.randint(0, index)
            if slot < k:
                sample[slot] = row

    return sample


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("source", type=Path, help="Path to the full Kaggle emails.csv")
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    # Some Enron messages are several MB long, well past csv's default field limit.
    csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

    with args.source.open(newline="", encoding="utf-8") as source_file:
        reader = csv.reader(source_file)
        header = next(reader)
        sample = reservoir_sample(reader, args.rows, args.seed)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file, quoting=csv.QUOTE_ALL)
        writer.writerow(header)
        writer.writerows(sample)

    print(f"Wrote {len(sample)} rows to {args.output}")


if __name__ == "__main__":
    main()
