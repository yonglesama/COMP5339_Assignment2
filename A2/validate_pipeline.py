from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import sys
from typing import Any

import duckdb

sys.dont_write_bytecode = True

SRC_ROOT = Path(__file__).resolve().parent / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.paths import project_path, write_metric_csv
from database.repository import EventRepository
from events.contract import validate_event


EVENT_TYPES = ("price_update", "session_start", "meter", "session_end")


def require_nonempty_file(path: Path, label: str) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise FileNotFoundError(f"{label} is missing or empty: {path}")


def scan_prepared_stream(path: Path) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    total = 0
    first_event_time: str | None = None
    last_event_time: str | None = None

    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                raise ValueError(f"Blank event-stream line: {line_number}")
            try:
                event = json.loads(line)
                validate_event(event)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"Invalid prepared event at line {line_number}: {exc}") from exc

            total += 1
            counts[event["event_type"]] += 1
            first_event_time = first_event_time or event["event_time"]
            last_event_time = event["event_time"]
            if total % 500000 == 0:
                print(f"Scanned {total} prepared events", flush=True)

    if total == 0:
        raise ValueError("Prepared event stream contains no events")
    return {
        "total": total,
        "counts": counts,
        "first_event_time": first_event_time,
        "last_event_time": last_event_time,
    }


def scalar(connection: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return connection.execute(sql).fetchone()[0]


def validate_pipeline(project_root: Path) -> dict[str, Any]:
    root = project_root.resolve()
    stream_path = project_path(root, "A2/data/events/event_stream.jsonl")
    database_path = project_path(root, "A2/data/final/ev_chargers_a2.duckdb")
    report_path = project_path(root, "A2/data/quality/final_validation.csv")
    require_nonempty_file(stream_path, "prepared event stream")
    require_nonempty_file(database_path, "A2 database")

    prepared = scan_prepared_stream(stream_path)
    prepared_counts: Counter[str] = prepared["counts"]
    failures: list[str] = []
    results: dict[str, Any] = {
        "prepared_events": prepared["total"],
        "prepared_first_event_time": prepared["first_event_time"],
        "prepared_last_event_time": prepared["last_event_time"],
    }
    for kind in EVENT_TYPES:
        results[f"prepared_{kind}"] = prepared_counts[kind]

    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        raw_counts = dict(
            connection.execute(
                """SELECT event_type, COUNT(*)
                   FROM mqtt_events
                   GROUP BY event_type"""
            ).fetchall()
        )
        raw_total = scalar(connection, "SELECT COUNT(*) FROM mqtt_events")
        unique_event_ids = scalar(
            connection, "SELECT COUNT(DISTINCT event_id) FROM mqtt_events"
        )
        unique_sequences = scalar(
            connection, "SELECT COUNT(DISTINCT sequence_number) FROM mqtt_events"
        )
        results.update(
            raw_events=raw_total,
            unique_event_ids=unique_event_ids,
            duplicate_event_ids=raw_total - unique_event_ids,
            duplicate_sequence_numbers=raw_total - unique_sequences,
        )
        for kind in EVENT_TYPES:
            results[f"raw_{kind}"] = raw_counts.get(kind, 0)
            if raw_counts.get(kind, 0) != prepared_counts[kind]:
                failures.append(
                    f"{kind} count prepared={prepared_counts[kind]} "
                    f"stored={raw_counts.get(kind, 0)}"
                )
        if raw_total != prepared["total"]:
            failures.append(
                f"total event count prepared={prepared['total']} stored={raw_total}"
            )
        if results["duplicate_event_ids"]:
            failures.append(f"duplicate event_id rows={results['duplicate_event_ids']}")
        if results["duplicate_sequence_numbers"]:
            failures.append(
                f"duplicate sequence_number rows={results['duplicate_sequence_numbers']}"
            )

        results["unknown_station_events"] = scalar(
            connection,
            """SELECT COUNT(*)
               FROM mqtt_events AS e
               LEFT JOIN stations AS s USING (station_id)
               WHERE s.station_id IS NULL""",
        )
        results["price_events"] = scalar(connection, "SELECT COUNT(*) FROM price_events")
        results["charging_sessions"] = scalar(
            connection, "SELECT COUNT(*) FROM charging_sessions"
        )
        results["completed_sessions"] = scalar(
            connection,
            "SELECT COUNT(*) FROM charging_sessions WHERE end_event_id IS NOT NULL",
        )
        results["active_sessions_at_end"] = scalar(
            connection,
            "SELECT COUNT(*) FROM charging_sessions WHERE end_event_id IS NULL",
        )
        results["meter_events"] = scalar(connection, "SELECT COUNT(*) FROM meter_events")
        results["orphan_meter_events"] = scalar(
            connection,
            """SELECT COUNT(*)
               FROM meter_events AS m
               LEFT JOIN charging_sessions AS s USING (session_id)
               WHERE s.session_id IS NULL""",
        )

        expected_normalised = {
            "price_events": prepared_counts["price_update"],
            "charging_sessions": prepared_counts["session_start"],
            "completed_sessions": prepared_counts["session_end"],
            "meter_events": prepared_counts["meter"],
        }
        for metric, expected in expected_normalised.items():
            if results[metric] != expected:
                failures.append(f"{metric} expected={expected} actual={results[metric]}")
        for zero_metric in (
            "unknown_station_events",
            "active_sessions_at_end",
            "orphan_meter_events",
        ):
            if results[zero_metric] != 0:
                failures.append(f"{zero_metric}={results[zero_metric]}")

        results["price_value_mismatches"] = scalar(
            connection,
            """SELECT COUNT(*)
               FROM price_events AS p
               JOIN mqtt_events AS e USING (event_id)
               WHERE e.event_type <> 'price_update'
                  OR p.station_id <> e.station_id
                  OR ABS(
                       p.price_per_kwh
                       - CAST(JSON_EXTRACT_STRING(e.payload, '$.price_per_kwh') AS DOUBLE)
                     ) > 0.000000001
                  OR ABS(
                       p.wholesale_per_kwh
                       - CAST(JSON_EXTRACT_STRING(e.payload, '$.wholesale_per_kwh') AS DOUBLE)
                     ) > 0.000000001
                  OR p.currency <> JSON_EXTRACT_STRING(e.payload, '$.currency')""",
        )
        results["session_start_mismatches"] = scalar(
            connection,
            """SELECT COUNT(*)
               FROM charging_sessions AS s
               JOIN mqtt_events AS e ON e.event_id = s.start_event_id
               WHERE e.event_type <> 'session_start'
                  OR s.station_id <> e.station_id
                  OR s.session_id <> JSON_EXTRACT_STRING(e.payload, '$.session_id')
                  OR s.connector_id <>
                     CAST(JSON_EXTRACT_STRING(e.payload, '$.connector_id') AS INTEGER)
                  OR ABS(
                       s.price_per_kwh
                       - CAST(JSON_EXTRACT_STRING(e.payload, '$.price_per_kwh') AS DOUBLE)
                     ) > 0.000000001""",
        )
        results["session_end_mismatches"] = scalar(
            connection,
            """SELECT COUNT(*)
               FROM charging_sessions AS s
               JOIN mqtt_events AS e ON e.event_id = s.end_event_id
               WHERE e.event_type <> 'session_end'
                  OR s.station_id <> e.station_id
                  OR s.session_id <> JSON_EXTRACT_STRING(e.payload, '$.session_id')
                  OR ABS(
                       s.energy_kwh
                       - CAST(JSON_EXTRACT_STRING(e.payload, '$.energy_kwh') AS DOUBLE)
                     ) > 0.000000001
                  OR ABS(
                       s.cost_aud
                       - CAST(JSON_EXTRACT_STRING(e.payload, '$.cost_aud') AS DOUBLE)
                     ) > 0.000000001""",
        )
        results["meter_identity_mismatches"] = scalar(
            connection,
            """SELECT COUNT(*)
               FROM meter_events AS m
               JOIN mqtt_events AS e USING (event_id)
               WHERE e.event_type <> 'meter'
                  OR m.station_id <> e.station_id
                  OR m.session_id <> JSON_EXTRACT_STRING(e.payload, '$.session_id')
                  OR m.connector_id <>
                     CAST(JSON_EXTRACT_STRING(e.payload, '$.connector_id') AS INTEGER)""",
        )
        for mismatch_metric in (
            "price_value_mismatches",
            "session_start_mismatches",
            "session_end_mismatches",
            "meter_identity_mismatches",
        ):
            if results[mismatch_metric] != 0:
                failures.append(f"{mismatch_metric}={results[mismatch_metric]}")

        results["latest_price_view_rows"] = scalar(
            connection, "SELECT COUNT(*) FROM latest_station_prices"
        )
        results["priced_station_count"] = scalar(
            connection, "SELECT COUNT(DISTINCT station_id) FROM price_events"
        )
        results["active_session_view_rows"] = scalar(
            connection, "SELECT COUNT(*) FROM active_charging_sessions"
        )
        results["latest_meter_view_rows"] = scalar(
            connection, "SELECT COUNT(*) FROM latest_session_meter_events"
        )
        results["metered_session_count"] = scalar(
            connection, "SELECT COUNT(DISTINCT session_id) FROM meter_events"
        )
        results["mapped_station_count"] = scalar(
            connection,
            "SELECT COUNT(DISTINCT station_id) FROM station_connector_mapping",
        )
        sample_session = connection.execute(
            "SELECT session_id FROM charging_sessions ORDER BY session_id LIMIT 1"
        ).fetchone()
        results["sample_session_id"] = sample_session[0] if sample_session else None
        connection.execute("SELECT * FROM current_charging_metrics").fetchone()
        connection.execute("SELECT * FROM historical_charging_metrics").fetchone()
        results["analysis_views_queryable"] = True
    finally:
        connection.close()

    repository = EventRepository(database_path, read_only=True)
    try:
        dashboard_stations = repository.get_dashboard_stations()
        latest_prices = repository.get_latest_prices()
        active_sessions = repository.get_active_sessions()
        current_metrics = repository.get_current_metrics()
        historical_metrics = repository.get_historical_metrics()
        curve_rows = 0
        if results["sample_session_id"] is not None:
            curve_rows = len(
                repository.get_session_meter_events(results["sample_session_id"])
            )
        results.update(
            dashboard_station_rows=len(dashboard_stations),
            repository_latest_price_rows=len(latest_prices),
            repository_active_session_rows=len(active_sessions),
            repository_sample_curve_rows=curve_rows,
            repository_current_metrics_queryable=bool(current_metrics),
            repository_historical_metrics_queryable=bool(historical_metrics),
        )
    finally:
        repository.close()

    if results["repository_active_session_rows"] != 0:
        failures.append(
            f"repository_active_session_rows={results['repository_active_session_rows']}"
        )
    if results["latest_price_view_rows"] != results["priced_station_count"]:
        failures.append(
            "latest price view row count does not match priced station count"
        )
    if results["active_session_view_rows"] != results["active_sessions_at_end"]:
        failures.append(
            "active session view row count does not match unfinished session count"
        )
    if results["latest_meter_view_rows"] != results["metered_session_count"]:
        failures.append(
            "latest meter view row count does not match metered session count"
        )
    if results["repository_latest_price_rows"] != results["latest_price_view_rows"]:
        failures.append("repository latest-price result does not match its view")
    if results["dashboard_station_rows"] != results["mapped_station_count"]:
        failures.append(
            "dashboard station query row count does not match mapped station count"
        )
    if prepared_counts["meter"] and results["repository_sample_curve_rows"] == 0:
        failures.append("sample session curve query returned no rows")

    rows: list[tuple[str, object, str]] = [
        ("validation_passed", not failures, "; ".join(failures) or "all checks passed"),
        ("prepared_events", results["prepared_events"], "JSONL event count"),
        ("raw_events", results["raw_events"], "Unique stored MQTT events"),
        ("duplicate_event_ids", results["duplicate_event_ids"], "Must be zero"),
        (
            "duplicate_sequence_numbers",
            results["duplicate_sequence_numbers"],
            "Must be zero",
        ),
    ]
    for kind in EVENT_TYPES:
        rows.append(
            (
                f"{kind}_prepared_vs_raw",
                f"{results[f'prepared_{kind}']}:{results[f'raw_{kind}']}",
                "Counts must match",
            )
        )
    rows.extend(
        [
            ("price_events", results["price_events"], "Must match price_update"),
            ("charging_sessions", results["charging_sessions"], "Must match session_start"),
            ("completed_sessions", results["completed_sessions"], "Must match session_end"),
            ("meter_events", results["meter_events"], "Must match meter"),
            ("unknown_station_events", results["unknown_station_events"], "Must be zero"),
            ("orphan_meter_events", results["orphan_meter_events"], "Must be zero"),
            ("active_sessions_at_end", results["active_sessions_at_end"], "Must be zero"),
            ("price_value_mismatches", results["price_value_mismatches"], "Must be zero"),
            ("session_start_mismatches", results["session_start_mismatches"], "Must be zero"),
            ("session_end_mismatches", results["session_end_mismatches"], "Must be zero"),
            ("meter_identity_mismatches", results["meter_identity_mismatches"], "Must be zero"),
            (
                "analysis_views_queryable",
                results["analysis_views_queryable"],
                "All five analytical views executed",
            ),
            (
                "dashboard_station_rows",
                results["dashboard_station_rows"],
                "Repository map query",
            ),
            (
                "repository_latest_price_rows",
                results["repository_latest_price_rows"],
                "Repository latest-price query",
            ),
            (
                "repository_sample_curve_rows",
                results["repository_sample_curve_rows"],
                "One completed session curve",
            ),
            (
                "prepared_first_event_time",
                results["prepared_first_event_time"],
                "First JSONL event",
            ),
            (
                "prepared_last_event_time",
                results["prepared_last_event_time"],
                "Last JSONL event",
            ),
        ]
    )
    write_metric_csv(report_path, rows)
    results["validation_passed"] = not failures
    results["failures"] = failures
    if failures:
        raise RuntimeError("Final pipeline validation failed: " + "; ".join(failures))

    print(
        f"Final pipeline validation passed: {results['raw_events']} events, "
        f"{results['completed_sessions']} completed sessions; report written to {report_path}"
    )
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Member C/shared: validate the completed A2 pipeline.")
    parser.add_argument("--project-root", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_pipeline(args.project_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
