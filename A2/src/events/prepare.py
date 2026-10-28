"""Validate generator CSVs and externally sort events into a replayable JSONL file."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import sqlite3
import sys
import tempfile

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.paths import write_metric_csv
from events.contract import PRIORITY, envelope, number, timestamp, validate_event
from generation.export_stations import sha256


def read_csv(path: Path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        yield from csv.DictReader(handle)


def prepare(root: Path, data_root: Path | None = None) -> dict:
    data_root = data_root or root / "A2/data"
    generated = data_root / "generated"
    output = data_root / "events/event_stream.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    stations = {r["station_id"]: r for r in read_csv(generated / "stations.csv")}
    if not stations:
        raise ValueError("No generated stations")
    sessions: dict[str, dict] = {}
    occupied: dict[tuple, list] = defaultdict(list)
    counts: Counter = Counter()
    # SQLite is an A-only temporary sorting workspace, not C's final event store.
    with tempfile.TemporaryDirectory(prefix=".prepare-", dir=output.parent) as directory:
        temporary = Path(directory)
        db = sqlite3.connect(temporary / "sort.sqlite3")
        try:
            db.execute("PRAGMA journal_mode=OFF")
            db.execute("PRAGMA synchronous=OFF")
            db.execute("PRAGMA cache_size=-32768")
            db.execute("CREATE TABLE prices (station TEXT, time TEXT, price REAL, wholesale REAL, PRIMARY KEY(station,time))")
            db.execute("CREATE TABLE events (time TEXT, priority INTEGER, station TEXT, session TEXT, payload TEXT, PRIMARY KEY(time,priority,station,session)) WITHOUT ROWID")

            def add(kind, time, station, payload):
                db.execute("INSERT INTO events VALUES (?,?,?,?,?)", (
                    timestamp(time), PRIORITY[kind], station, payload.get("session_id", ""),
                    json.dumps(payload, separators=(",", ":"), allow_nan=False)))
                counts[kind] += 1

            for row in read_csv(generated / "prices_hourly.csv"):
                if row["station_id"] not in stations:
                    raise ValueError(f"Unknown price station: {row['station_id']}")
                db.execute("INSERT INTO prices VALUES (?,?,?,?)", (
                    row["station_id"], timestamp(row["ts"]), number(row["price_per_kwh"]),
                    number(row["wholesale_per_kwh"], minimum=None)))
                counts["input_prices"] += 1
            if {r[0] for r in db.execute("SELECT DISTINCT station FROM prices")} != set(stations):
                raise ValueError("Missing initial price for a generated station")
            for station, time, price, wholesale, previous in db.execute("""
                SELECT station,time,price,wholesale,
                       LAG(price) OVER (PARTITION BY station ORDER BY time)
                FROM prices ORDER BY station,time
            """):
                if previous is None or price != previous:
                    add("price_update", time, station, {"price_per_kwh": price,
                        "wholesale_per_kwh": wholesale, "currency": "AUD"})

            for row in read_csv(generated / "sessions.csv"):
                sid, station = row["session_id"], row["station_id"]
                connector = int(row["connector_id"])
                if sid in sessions or station not in stations or not 1 <= connector <= int(stations[station]["connectors"]):
                    raise ValueError(f"Duplicate session or invalid station/connector: {sid}")
                start, charge_end, end = [timestamp(row[k]) for k in ("start_ts", "charge_end_ts", "end_ts")]
                if not start < charge_end <= end:
                    raise ValueError(f"Invalid session lifetime: {sid}")
                price_row = db.execute("SELECT price FROM prices WHERE station=? AND time<=? ORDER BY time DESC LIMIT 1", (station, start)).fetchone()
                if price_row is None or price_row[0] != number(row["price_per_kwh"]):
                    raise ValueError(f"Session price does not match effective tariff: {sid}")
                if row["site_type"] != stations[station]["site_type"]:
                    raise ValueError(f"Session site type mismatch: {sid}")
                sessions[sid] = {"station": station, "connector": connector, "start": start,
                                 "charge_end": charge_end, "end": end, "last_time": "",
                                 "energy": 0.0, "soc": 0.0, "meters": 0}
                occupied[station, connector].append((start, end, sid))
                common = {"session_id": sid, "connector_id": connector}
                add("session_start", start, station, {**common, "site_type": row["site_type"],
                    "vehicle_model": row["vehicle_model"], "price_per_kwh": number(row["price_per_kwh"])})
                payload = {**common, "charge_end_time": charge_end, "end_time": end}
                payload.update({k: number(row[k]) for k in (
                    "charge_minutes", "energy_kwh", "avg_kw", "peak_kw", "price_per_kwh", "cost_aud")})
                add("session_end", end, station, payload)
            for intervals in occupied.values():
                intervals.sort()
                for first, second in zip(intervals, intervals[1:]):
                    if first[1] > second[0]:
                        raise ValueError(f"Overlapping connector sessions: {first[2]}, {second[2]}")
            for row in read_csv(generated / "session_metering.csv"):
                sid, station, time = row["session_id"], row["station_id"], timestamp(row["ts"])
                session = sessions.get(sid)
                connector = int(row["connector_id"])
                if (session is None or station != session["station"] or connector != session["connector"]
                        or not session["start"] < time <= session["charge_end"]):
                    raise ValueError(f"Orphan or out-of-session meter: {sid}")
                payload = {"session_id": sid, "connector_id": connector}
                payload.update({k: number(row[k]) for k in ("voltage_v", "current_a", "power_kw", "energy_kwh", "soc_pct")})
                if (time <= session["last_time"] or payload["energy_kwh"] < session["energy"]
                        or payload["soc_pct"] < session["soc"] or payload["soc_pct"] > 100):
                    raise ValueError(f"Invalid meter progression: {sid}")
                session.update(last_time=time, energy=payload["energy_kwh"], soc=payload["soc_pct"], meters=session["meters"] + 1)
                add("meter", time, station, payload)
                if counts["meter"] % 250000 == 0:
                    print(f"Validated {counts['meter']} meter records", flush=True)
            if not sessions or any(s["meters"] == 0 for s in sessions.values()):
                raise ValueError("Every session must have telemetry for the assessed live curve")
            db.commit()
            stream = temporary / "event_stream.jsonl"
            inverse = {v: k for k, v in PRIORITY.items()}
            with stream.open("w", encoding="utf-8", newline="\n") as handle:
                for sequence, (time, priority, station, _, payload) in enumerate(
                    db.execute("SELECT * FROM events ORDER BY time,priority,station,session"), 1
                ):
                    event = envelope(inverse[priority], time, station, json.loads(payload))
                    event["sequence_number"] = sequence
                    validate_event(event)
                    handle.write(json.dumps(event, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n")
                    if sequence % 500000 == 0:
                        print(f"Wrote {sequence} ordered events", flush=True)
            stream.replace(output)
        finally:
            db.close()
    total = sum(counts[k] for k in PRIORITY)
    metrics = [("stations", len(stations), "A1 station_id join key"),
               ("sessions", len(sessions), "One start and one end per session"),
               ("input_price_rows", counts["input_prices"], "Includes unchanged hourly prices"),
               ("unchanged_prices_suppressed", counts["input_prices"] - counts["price_update"], "Compare retail tariff, not wholesale"),
               *[(f"{k}_events", counts[k], "Validated events") for k in PRIORITY],
               ("total_events", total, "Strict sequence_number from 1"),
               ("minimum_full_replay_hours", round(max(0, total - 1) * 0.1 / 3600, 3), "Excludes MQTT overhead"),
               ("stream_sha256", sha256(output), "Deterministic content fingerprint"),
               ("validation_passed", True, "Ordering, identifiers, prices, session lifetime, occupancy and telemetry checked")]
    write_metric_csv(data_root / "quality/event_preparation_summary.csv", metrics)
    print(f"Prepared {total} events -> {output}")
    return dict(counts, total_events=total)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--data-root", type=Path)
    args = parser.parse_args(argv)
    prepare(args.project_root.resolve(), args.data_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
