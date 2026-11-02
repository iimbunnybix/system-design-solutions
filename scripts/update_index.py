"""Rebuild the problem index in README.md from each problem's title:  python scripts/update_index.py"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
START, END = "<!-- INDEX:START -->", "<!-- INDEX:END -->"

rows = []
for d in sorted((ROOT / "problems").iterdir()):
    readme = d / "README.md"
    if not readme.exists():
        continue
    title = readme.read_text(encoding="utf-8").splitlines()[0].lstrip("# ").strip()
    num, _, text = title.partition("·")
    rows.append(f"| {num.strip()} | [{text.strip()}](problems/{d.name}) |")

table = "\n".join(["| # | Problem |", "|---|---|", *rows])
readme = ROOT / "README.md"
content = readme.read_text(encoding="utf-8")
content = re.sub(f"{START}.*{END}", f"{START}\n{table}\n{END}", content, flags=re.S)
readme.write_text(content, encoding="utf-8")
print(f"Indexed {len(rows)} problems")
