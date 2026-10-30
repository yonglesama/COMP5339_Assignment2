"""Import the complete prepared stream through C's validated repository boundary."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.paths import write_metric_csv
from database.repository import EventRepository


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _database_snapshot(path: Path) -> tuple[tuple, tuple]:
    import duckdb

    with duckdb.connect(str(path), read_only=True) as connection:
        tables = connection.execute(
            """SELECT table_name
               FROM information_schema.tables
               WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
               ORDER BY table_name"""
        ).fetchall()
        table_counts = tuple(
            (name, connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0])
            for (name,) in tables
        )
        relations = tuple(
            connection.execute(
                """SELECT table_name, table_type
                   FROM information_schema.tables
                   WHERE table_schema = 'main'
                   ORDER BY table_type, table_name"""
            ).fetchall()
        )
        indexes = tuple(
            connection.execute(
                "SELECT index_name FROM duckdb_indexes() ORDER BY index_name"
            ).fetchall()
        )
    return table_counts, relations + indexes


def compact_database(database_path: Path) -> tuple[int, int]:
    """Rewrite a transaction-heavy DuckDB and atomically replace it after verification."""
    import duckdb

    original_size = database_path.stat().st_size
    source_snapshot = _database_snapshot(database_path)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{database_path.stem}-compact-",
        suffix=database_path.suffix,
        dir=database_path.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    temporary.unlink()
    source_sql = database_path.as_posix().replace("'", "''")
    target_sql = temporary.as_posix().replace("'", "''")
    try:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(f"ATTACH '{source_sql}' AS source (READ_ONLY)")
            connection.execute(f"ATTACH '{target_sql}' AS compact")
            connection.execute("COPY FROM DATABASE source TO compact")
        finally:
            connection.close()
        if _database_snapshot(temporary) != source_snapshot:
            raise RuntimeError("Compacted database schema, indexes or table counts differ")
        temporary.replace(database_path)
    finally:
        temporary.unlink(missing_ok=True)
    return original_size, database_path.stat().st_size


def import_event_stream(
    stream_path: Path,
    database_path: Path,
    report_path: Path,
    *,
    progress_every: int = 10_000,
) -> dict[str, object]:
    stream_path = stream_path.resolve()
    database_path = database_path.resolve()
    report_path = report_path.resolve()
    if not stream_path.is_file() or stream_path.stat().st_size == 0:
        raise FileNotFoundError(f"Prepared event stream is missing or empty: {stream_path}")
    if not database_path.is_file() or database_path.stat().st_size == 0:
        raise FileNotFoundError(f"Initialised A2 database is missing or empty: {database_path}")
    if progress_every < 1:
        raise ValueError("progress_every must be positive")

    repository = EventRepository(database_path)
    counts: Counter[str] = Counter()
    stored = duplicates = 0
    started = time.perf_counter()
    try:
        with stream_path.open(encoding="utf-8-sig") as handle:
            for line_number, line in enumerate(handle, 1):
                try:
                    event = json.loads(line)
                except (ValueError, TypeError) as exc:
                    raise ValueError(f"Invalid JSON on line {line_number}") from exc
                topic = (
                    f"comp5339/ev/{event.get('station_id', '')}/"
                    f"{event.get('event_type', '')}"
                )
                if repository.store_event(topic, event):
                    stored += 1
                else:
                    duplicates += 1
                counts[event["event_type"]] += 1
                if line_number % progress_every == 0:
                    elapsed = time.perf_counter() - started
                    print(
                        f"Imported {line_number} events "
                        f"({line_number / elapsed:.1f} events/second)",
                        flush=True,
                    )
    finally:
        repository.close()

    elapsed = time.perf_counter() - started
    total = sum(counts.values())
    import duckdb

    with duckdb.connect(str(database_path), read_only=True) as connection:
        raw_events, price_events, sessions, meter_events = connection.execute(
            """SELECT
                   (SELECT COUNT(*) FROM mqtt_events),
                   (SELECT COUNT(*) FROM price_events),
                   (SELECT COUNT(*) FROM charging_sessions),
                   (SELECT COUNT(*) FROM meter_events)"""
        ).fetchone()
    if raw_events != total:
        raise RuntimeError(
            f"Database contains {raw_events} raw events but the selected stream contains {total}"
        )

    uncompacted_size, compacted_size = compact_database(database_path)
    result: dict[str, object] = {
        "import_passed": True,
        "stream_sha256": sha256(stream_path),
        "stream_events": total,
        "stored_events_this_run": stored,
        "duplicate_events_this_run": duplicates,
        "raw_events": raw_events,
        "price_events": price_events,
        "charging_sessions": sessions,
        "meter_events": meter_events,
        "elapsed_seconds": round(elapsed, 3),
        "events_per_second": round(total / elapsed, 3),
        "uncompacted_database_size_bytes": uncompacted_size,
        "database_size_bytes": compacted_size,
        "database_sha256": sha256(database_path),
    }
    notes = {
        "import_passed": "Complete prepared stream reconciled with the populated database",
        "stream_sha256": "Must match event_preparation_summary.csv",
        "stream_events": "All prepared events; MQTT transport is validated separately",
        "stored_events_this_run": "New event IDs committed through EventRepository",
        "duplicate_events_this_run": "Existing IDs safely ignored during an idempotent rerun",
        "raw_events": "mqtt_events rows after import",
        "price_events": "Normalised price rows after import",
        "charging_sessions": "One row per session lifecycle",
        "meter_events": "Normalised meter rows after import",
        "elapsed_seconds": "Wall-clock import duration without MQTT pacing",
        "events_per_second": "Average validated repository throughput",
        "uncompacted_database_size_bytes": "Size before verified physical compaction",
        "database_size_bytes": "Compacted closed DuckDB file size",
        "database_sha256": "Populated DuckDB fingerprint",
    }
    write_metric_csv(
        report_path,
        [(key, value, notes[key]) for key, value in result.items()],
    )
    print(
        f"Imported {total} events into {database_path}; "
        f"report written to {report_path}"
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--stream", type=Path)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--progress-every", type=int, default=10_000)
    args = parser.parse_args(argv)
    root = args.project_root.resolve()
    import_event_stream(
        args.stream or root / "A2/data/events/event_stream.jsonl",
        args.database or root / "A2/data/final/ev_chargers_a2.duckdb",
        args.report or root / "A2/data/quality/database_import.csv",
        progress_every=args.progress_every,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
