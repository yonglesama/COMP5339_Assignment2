from __future__ import annotations

import csv
import json
from pathlib import Path


def project_path(project_root: Path, relative_path: str) -> Path:
    return project_root.resolve() / Path(relative_path)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_metric_csv(path: Path, rows: list[tuple[str, object, str]]) -> None:
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value", "note"])
        writer.writerows(rows)
