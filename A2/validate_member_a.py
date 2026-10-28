"""Independent reconciliation of Member A's packaged dataset and replay stream."""
import argparse
from collections import Counter
import csv
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "A2/src"))
from common.paths import load_json, write_metric_csv
from events.contract import envelope, validate_event
from generation.export_stations import sha256


def validate(root: Path) -> None:
    import duckdb
    data = root / "A2/data"
    manifest = load_json(data / "generated/generation_manifest.json")
    for name, digest in manifest["outputs"].items():
        assert sha256(data / "generated" / name) == digest, f"Changed generated file: {name}"
    assert sha256(root / "A2/external/data_generator/generate_dataset.py") == manifest["generator_sha256"]
    with (data / "quality/station_export_summary.csv").open(newline="", encoding="utf-8") as handle:
        summary = {r["metric"]: r["value"] for r in csv.DictReader(handle)}
    assert sha256(root / "A1/data/final/ev_chargers.duckdb") == summary["a1_database_sha256"]
    with (data / "input/generator_stations.csv").open(newline="", encoding="utf-8") as handle:
        stations = {r["station_id"]: int(r["connectors"]) for r in csv.DictReader(handle)}
    with duckdb.connect(str(root / "A1/data/final/ev_chargers.duckdb"), read_only=True) as db:
        source = dict(db.execute("""SELECT c.station_id, SUM(cc.number_of_plugs)
            FROM chargers c JOIN charger_characteristics cc USING(charger_id)
            WHERE cc.charger_type='DC' AND cc.rating_kw>0 GROUP BY c.station_id""").fetchall())
    assert stations == source, "Assessed data must cover the full eligible A1 station/plug set"
    counts = Counter()
    active, finished, prices = {}, set(), {}
    last_key, total = None, 0
    first_time = last_time = None
    with (data / "events/event_stream.jsonl").open(encoding="utf-8") as handle:
        for line in handle:
            event = json.loads(line)
            key = validate_event(event)
            total += 1
            assert event["sequence_number"] == total
            assert last_key is None or key >= last_key, "Stream order regressed"
            assert event["station_id"] in stations
            canonical = envelope(event["event_type"], event["event_time"], event["station_id"], event["payload"])
            assert canonical["event_id"] == event["event_id"], "Unstable or corrupted event ID"
            first_time = first_time or event["event_time"]
            last_time = event["event_time"]
            kind, station, payload = event["event_type"], event["station_id"], event["payload"]
            if kind == "price_update":
                price = payload["price_per_kwh"]
                assert prices.get(station) != price, "Redundant retail price event"
                prices[station] = price
            else:
                sid = payload["session_id"]
                connector = payload["connector_id"]
                assert 1 <= connector <= stations[station]
                if kind == "session_start":
                    assert sid not in active and sid not in finished
                    assert prices[station] == payload["price_per_kwh"]
                    active[sid] = {"station": station, "connector": connector, "energy": 0,
                                   "soc": 0, "time": event["event_time"], "meters": 0}
                else:
                    session = active[sid]
                    assert station == session["station"] and connector == session["connector"]
                    if kind == "meter":
                        assert event["event_time"] > session["time"]
                        assert payload["energy_kwh"] >= session["energy"] and payload["soc_pct"] >= session["soc"]
                        session.update(energy=payload["energy_kwh"], soc=payload["soc_pct"], time=event["event_time"], meters=session["meters"] + 1)
                    else:
                        assert session["meters"] > 0
                        assert session["energy"] <= payload["energy_kwh"] + .011
                        assert session["time"] <= payload["charge_end_time"] <= payload["end_time"]
                        finished.add(sid)
                        del active[sid]
            counts[kind] += 1
            last_key = key
            if total % 500000 == 0:
                print(f"Independently reconciled {total} events", flush=True)
    assert not active
    rows = manifest["row_counts"]
    assert counts["session_start"] == counts["session_end"] == rows["sessions.csv"]
    assert counts["meter"] == rows["session_metering.csv"]
    assert len(prices) == rows["stations.csv"]
    write_metric_csv(data / "quality/member_a_validation.csv", [
        ("validation_passed", True, "Independent complete-file reconciliation; not an MQTT full replay"),
        ("full_a1_station_coverage", len(stations), "All eligible DC station IDs and plug counts match"),
        ("source_fingerprints_unchanged", True, "A1 database, professor generator and generated CSVs"),
        ("total_events", total, "Checked every event's ID, sequence, time/type order and references"),
        *[(kind, count, "Reconciled event count") for kind, count in counts.items()],
        ("completed_sessions", len(finished), "No unmatched starts or ends"),
        ("active_sessions_at_end", len(active), "All generated sessions include their end events"),
        ("first_event_time", first_time, "Supplied generator's simulated timestamp labels"),
        ("last_event_time", last_time, "Sessions can extend beyond the last start date"),
    ])
    print(f"Member A validation passed: {total} events, {len(finished)} complete sessions")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=ROOT)
    args = parser.parse_args()
    validate(args.project_root.resolve())
