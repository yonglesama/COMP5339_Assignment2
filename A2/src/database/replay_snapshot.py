"""Build an auditable C-history baseline for replaying unchanged A events.

Only a UUID-owned working copy is edited. Three completed sessions are held out
and subsequently restored through the existing A -> MQTT -> B -> C pipeline.
This is a selected-session replay baseline, not a global point-in-time snapshot.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import shutil

import duckdb


def fingerprint(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def prepare_replay_snapshot(source: Path, destination: Path, stream: Path,
                            output_stream: Path, station_count: int = 3) -> dict:
    source, destination = source.resolve(), destination.resolve()
    if destination == source or output_stream.resolve() == stream.resolve():
        raise ValueError("Replay outputs must be separate from source files")
    source_hash = fingerprint(source)
    with duckdb.connect(str(source), read_only=True) as connection:
        # The latest completed session at each of three distinct stations.
        selected = connection.execute("""
            SELECT session_id, station_id, start_event_id, end_event_id
            FROM charging_sessions WHERE end_event_id IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY station_id ORDER BY start_time DESC, session_id DESC
            ) = 1
            ORDER BY start_time DESC, session_id DESC LIMIT ?
        """, [station_count]).fetchall()
        if len(selected) != station_count:
            raise ValueError(f"C database needs {station_count} completed stations for this replay")
        session_ids = [row[0] for row in selected]
        station_ids = [row[1] for row in selected]
        price_ids = [row[0] for row in connection.execute(
            "SELECT event_id FROM latest_station_prices WHERE station_id IN (SELECT unnest(?))",
            [station_ids]).fetchall()]
        expected_ids = {row[2] for row in selected} | {row[3] for row in selected} | set(price_ids)
        expected_ids.update(row[0] for row in connection.execute(
            "SELECT event_id FROM meter_events WHERE session_id IN (SELECT unnest(?))",
            [session_ids]).fetchall())
        target = dict(zip([d[0] for d in connection.execute(
            "SELECT * FROM historical_charging_metrics").description],
            connection.fetchone()))
        source_count = connection.execute("SELECT count(*) FROM mqtt_events").fetchone()[0]
        expected = {row[0]: row[1:] for row in connection.execute("""
            SELECT event_id, station_id, event_type, sequence_number, payload
            FROM mqtt_events WHERE event_id IN (SELECT unnest(?))
        """, [sorted(expected_ids)]).fetchall()}

    # Preserve A's original JSONL lines, IDs, timestamps, values and sequence order.
    selected_lines, events = [], []
    with stream.open(encoding="utf-8-sig") as handle:
        for line in handle:
            event = json.loads(line)
            if event["event_id"] not in expected_ids:
                continue
            station, kind, sequence, payload = expected[event["event_id"]]
            if (station, kind, sequence, json.loads(payload)) != (
                    event["station_id"], event["event_type"],
                    event["sequence_number"], event["payload"]):
                raise ValueError("A stream and C database disagree on a selected event")
            events.append(event)
            selected_lines.append(line.rstrip("\r\n") + "\n")
    if len(events) != len(expected_ids) or {e["event_id"] for e in events} != expected_ids:
        raise ValueError("A stream does not contain exactly the selected C events")

    shutil.copy2(source, destination)
    with duckdb.connect(str(destination)) as connection:
        # Separate committed statements satisfy DuckDB's FK delete semantics.
        connection.execute("DELETE FROM meter_events WHERE session_id IN (SELECT unnest(?))", [session_ids])
        connection.execute("DELETE FROM charging_sessions WHERE session_id IN (SELECT unnest(?))", [session_ids])
        connection.execute("DELETE FROM price_events WHERE event_id IN (SELECT unnest(?))", [price_ids])
        connection.execute("DELETE FROM mqtt_events WHERE event_id IN (SELECT unnest(?))", [sorted(expected_ids)])
        cursor = connection.execute("SELECT * FROM historical_charging_metrics")
        baseline = dict(zip([d[0] for d in cursor.description], cursor.fetchone()))
        if baseline["completed_session_count"] != target["completed_session_count"] - len(selected):
            raise RuntimeError("Replay baseline count is inconsistent")
        if connection.execute("SELECT count(*) FROM mqtt_events").fetchone()[0] != source_count - len(events):
            raise RuntimeError("Replay baseline event count is inconsistent")
        connection.execute("CHECKPOINT")
    if fingerprint(source) != source_hash:
        raise RuntimeError("C source database changed while preparing its replay copy")
    output_stream.write_text("".join(selected_lines), encoding="utf-8")
    metadata = {
        "source_database": str(source), "source_sha256": source_hash,
        "source_stream": str(stream), "selected_session_count": len(selected),
        "station_ids": station_ids, "session_ids": session_ids,
        "event_count": len(events), "event_types": dict(Counter(e["event_type"] for e in events)),
        "meter_counts": {sid: sum(e["event_type"] == "meter" and e["payload"].get("session_id") == sid
                                  for e in events) for sid in session_ids},
        "baseline_historical_metrics": baseline, "target_historical_metrics": target,
        "description": "C history minus selected sessions; unchanged A events restore them once through MQTT",
    }
    (destination.parent / "replay_provenance.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return metadata
