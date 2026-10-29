from __future__ import annotations

import argparse
import csv
import hashlib
import math
import os
from pathlib import Path
import shutil
import sys
import tempfile

import duckdb

sys.dont_write_bytecode = True

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.paths import ensure_parent, project_path, write_metric_csv


MAPPING_FIELDS = [
    "station_id",
    "connector_id",
    "charger_id",
    "plug_number",
    "source_rating_kw",
]

REQUIRED_A1_TABLES = {
    "regions",
    "stations",
    "operators",
    "charger_locations",
    "chargers",
    "charger_characteristics",
    "charger_enrichment",
}

REQUIRED_A2_TABLES = {
    "station_connector_mapping",
    "mqtt_events",
    "price_events",
    "charging_sessions",
    "meter_events",
}

REQUIRED_A2_VIEWS = {
    "latest_station_prices",
    "active_charging_sessions",
    "latest_session_meter_events",
    "current_charging_metrics",
    "historical_charging_metrics",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_nonempty_file(path: Path, label: str) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise FileNotFoundError(f"{label} is missing or empty: {path}")


def validate_mapping_csv(path: Path) -> int:
    """Validate types and keys before DuckDB enforces A1 relationships."""
    station_connectors: set[tuple[str, int]] = set()
    charger_plugs: set[tuple[str, int]] = set()
    row_count = 0

    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != MAPPING_FIELDS:
            raise ValueError(
                f"Unexpected connector mapping columns: {reader.fieldnames}; "
                f"expected {MAPPING_FIELDS}"
            )

        for line_number, row in enumerate(reader, 2):
            try:
                station_id = row["station_id"].strip()
                charger_id = row["charger_id"].strip()
                connector_id = int(row["connector_id"])
                plug_number = int(row["plug_number"])
                source_rating_kw = float(row["source_rating_kw"])
            except (AttributeError, TypeError, ValueError) as exc:
                raise ValueError(f"Invalid connector mapping row {line_number}: {row}") from exc

            if (
                not station_id
                or not charger_id
                or connector_id <= 0
                or plug_number <= 0
                or not math.isfinite(source_rating_kw)
                or source_rating_kw <= 0
            ):
                raise ValueError(f"Invalid connector mapping row {line_number}: {row}")

            station_connector = (station_id, connector_id)
            charger_plug = (charger_id, plug_number)
            if station_connector in station_connectors:
                raise ValueError(f"Duplicate station connector at row {line_number}: {station_connector}")
            if charger_plug in charger_plugs:
                raise ValueError(f"Duplicate charger plug at row {line_number}: {charger_plug}")

            station_connectors.add(station_connector)
            charger_plugs.add(charger_plug)
            row_count += 1

    if row_count == 0:
        raise ValueError("Connector mapping contains no data rows")
    return row_count


def database_objects(connection: duckdb.DuckDBPyConnection) -> tuple[set[str], set[str]]:
    rows = connection.execute(
        """SELECT table_name, table_type
           FROM information_schema.tables
           WHERE table_schema = 'main'"""
    ).fetchall()
    tables = {name for name, kind in rows if kind == "BASE TABLE"}
    views = {name for name, kind in rows if kind == "VIEW"}
    return tables, views


def initialise_database(project_root: Path) -> Path:
    root = project_root.resolve()
    source_database = project_path(root, "A1/data/final/ev_chargers.duckdb")
    schema_path = project_path(root, "A2/sql/a2_schema.sql")
    mapping_path = project_path(root, "A2/data/input/station_connector_mapping.csv")
    destination = project_path(root, "A2/data/final/ev_chargers_a2.duckdb")
    report_path = project_path(root, "A2/data/quality/database_initialisation.csv")

    require_nonempty_file(source_database, "A1 database")
    require_nonempty_file(schema_path, "A2 schema")
    require_nonempty_file(mapping_path, "station connector mapping")

    mapping_csv_rows = validate_mapping_csv(mapping_path)
    source_hash_before = sha256(source_database)
    ensure_parent(destination)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".ev_chargers_a2-",
        suffix=".duckdb",
        dir=destination.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)

    connection: duckdb.DuckDBPyConnection | None = None
    metrics: dict[str, object] = {}
    try:
        shutil.copy2(source_database, temporary)
        connection = duckdb.connect(str(temporary))
        connection.execute(schema_path.read_text(encoding="utf-8-sig"))

        escaped_mapping_path = str(mapping_path).replace("'", "''")
        connection.execute(
            f"COPY station_connector_mapping FROM '{escaped_mapping_path}' "
            "(HEADER, DELIMITER ',')"
        )

        tables, views = database_objects(connection)
        missing_a1_tables = sorted(REQUIRED_A1_TABLES - tables)
        missing_a2_tables = sorted(REQUIRED_A2_TABLES - tables)
        missing_a2_views = sorted(REQUIRED_A2_VIEWS - views)
        if missing_a1_tables or missing_a2_tables or missing_a2_views:
            raise RuntimeError(
                "Database objects missing after initialisation: "
                f"A1 tables={missing_a1_tables}, A2 tables={missing_a2_tables}, "
                f"A2 views={missing_a2_views}"
            )

        mapping_rows = connection.execute(
            "SELECT COUNT(*) FROM station_connector_mapping"
        ).fetchone()[0]
        unknown_stations = connection.execute(
            """SELECT COUNT(*)
               FROM station_connector_mapping AS m
               LEFT JOIN stations AS s USING (station_id)
               WHERE s.station_id IS NULL"""
        ).fetchone()[0]
        unknown_chargers = connection.execute(
            """SELECT COUNT(*)
               FROM station_connector_mapping AS m
               LEFT JOIN chargers AS c USING (charger_id)
               WHERE c.charger_id IS NULL"""
        ).fetchone()[0]
        charger_station_mismatches = connection.execute(
            """SELECT COUNT(*)
               FROM station_connector_mapping AS m
               JOIN chargers AS c USING (charger_id)
               WHERE m.station_id <> c.station_id"""
        ).fetchone()[0]
        if mapping_rows != mapping_csv_rows:
            raise RuntimeError(
                f"Connector mapping row mismatch: CSV={mapping_csv_rows}, database={mapping_rows}"
            )
        if unknown_stations or unknown_chargers or charger_station_mismatches:
            raise RuntimeError(
                "Invalid connector mapping relationships: "
                f"unknown_stations={unknown_stations}, "
                f"unknown_chargers={unknown_chargers}, "
                f"charger_station_mismatches={charger_station_mismatches}"
            )

        station_count = connection.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
        charger_count = connection.execute("SELECT COUNT(*) FROM chargers").fetchone()[0]
        current_metrics = connection.execute(
            "SELECT * FROM current_charging_metrics"
        ).fetchone()
        historical_metrics = connection.execute(
            "SELECT * FROM historical_charging_metrics"
        ).fetchone()

        metrics = {
            "station_count": station_count,
            "charger_count": charger_count,
            "mapping_rows": mapping_rows,
            "unknown_stations": unknown_stations,
            "unknown_chargers": unknown_chargers,
            "charger_station_mismatches": charger_station_mismatches,
            "a1_tables_preserved": len(REQUIRED_A1_TABLES),
            "a2_tables_created": len(REQUIRED_A2_TABLES),
            "a2_views_created": len(REQUIRED_A2_VIEWS),
            "empty_current_metrics": current_metrics,
            "empty_historical_metrics": historical_metrics,
        }
        connection.execute("CHECKPOINT")
        connection.close()
        connection = None

        source_hash_after = sha256(source_database)
        if source_hash_after != source_hash_before:
            raise RuntimeError("A1 database changed during A2 initialisation")

        temporary.replace(destination)
        destination_hash = sha256(destination)
        write_metric_csv(
            report_path,
            [
                ("initialisation_passed", True, "Schema, mapping and relationships validated"),
                ("a1_database_sha256", source_hash_before, "Read-only source fingerprint"),
                ("a1_database_unchanged", True, "SHA-256 checked before and after"),
                ("a2_database_sha256", destination_hash, "Initialised database fingerprint"),
                ("station_count", metrics["station_count"], "Copied A1 stations"),
                ("charger_count", metrics["charger_count"], "Copied A1 chargers"),
                ("mapping_rows", metrics["mapping_rows"], "Imported station connectors"),
                ("unknown_stations", metrics["unknown_stations"], "Must be zero"),
                ("unknown_chargers", metrics["unknown_chargers"], "Must be zero"),
                (
                    "charger_station_mismatches",
                    metrics["charger_station_mismatches"],
                    "Must be zero",
                ),
                ("a1_tables_preserved", metrics["a1_tables_preserved"], "Required A1 tables"),
                ("a2_tables_created", metrics["a2_tables_created"], "Required A2 tables"),
                ("a2_views_created", metrics["a2_views_created"], "Required A2 views"),
                (
                    "empty_current_metrics",
                    metrics["empty_current_metrics"],
                    "Expected zero values before event ingestion",
                ),
                (
                    "empty_historical_metrics",
                    metrics["empty_historical_metrics"],
                    "Expected zero values before event ingestion",
                ),
            ],
        )
    finally:
        if connection is not None:
            connection.close()
        temporary.unlink(missing_ok=True)
        temporary.with_suffix(temporary.suffix + ".wal").unlink(missing_ok=True)

    print(
        f"Initialised {destination} with {metrics['mapping_rows']} connector mappings; "
        f"report written to {report_path}"
    )
    return destination


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Member C: initialise the integrated A2 DuckDB.")
    parser.add_argument("--project-root", type=Path, required=True)
    args = parser.parse_args(argv)
    initialise_database(args.project_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
