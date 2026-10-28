"""Project one complete real generated session and its preceding station prices."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
from common.paths import write_metric_csv


def make_demo(data_root: Path) -> Path:
    prices: dict[str, list] = {}
    with (data_root / "generated/prices_hourly.csv").open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            prices.setdefault(row["station_id"], []).append((row["ts"], float(row["price_per_kwh"])))
    first_change = {}
    for station, rows in prices.items():
        rows.sort()
        for time, price in rows[1:]:
            if price != rows[0][1]:
                first_change[station] = time
                break
    with (data_root / "generated/sessions.csv").open(newline="", encoding="utf-8") as handle:
        candidates = [r for r in csv.DictReader(handle) if 10 <= float(r["charge_minutes"]) <= 40]
    if not candidates:
        raise ValueError("No 10-40 minute session available for a short demonstration")
    after_change = [r for r in candidates if r["station_id"] in first_change
                    and r["start_ts"] >= first_change[r["station_id"]]]
    session = min(after_change or candidates, key=lambda r: (r["start_ts"], r["session_id"]))
    output = data_root / "demo/event_stream.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with (data_root / "events/event_stream.jsonl").open(encoding="utf-8") as source, output.open("w", encoding="utf-8", newline="\n") as target:
        for line in source:
            event = json.loads(line)
            if event["event_time"] > session["end_ts"]:
                break
            if event["station_id"] == session["station_id"] and (
                event["event_type"] == "price_update" or event["payload"].get("session_id") == session["session_id"]
            ):
                target.write(line)
                count += 1
    write_metric_csv(data_root / "quality/demo_summary.csv", [
        ("station_id", session["station_id"], "Actual A1 station"),
        ("session_id", session["session_id"], "Complete session projected from the full assessed stream"),
        ("events", count, "Original event_id and sequence_number retained; sequence may have gaps"),
        ("minimum_replay_seconds", round((count - 1) * 0.1, 1), "Other station sessions intentionally excluded"),
    ])
    print(f"Wrote {count} demo events -> {output}")
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    args = parser.parse_args()
    make_demo(args.project_root.resolve() / "A2/data")
