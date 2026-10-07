"""Regenerate docs/data_dictionary.md from the table definitions in governance.py."""

from pathlib import Path

from inbox_insights.governance import render_data_dictionary

OUTPUT = Path("docs") / "data_dictionary.md"

if __name__ == "__main__":
    OUTPUT.write_text(render_data_dictionary(), encoding="utf-8", newline="\n")
    print(f"Wrote {OUTPUT}")
