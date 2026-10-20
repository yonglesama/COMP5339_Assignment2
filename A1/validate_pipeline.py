"""Independently validate the final outputs of the complete data pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

import duckdb
import pandas as pd


sys.dont_write_bytecode = True
LOG = logging.getLogger("final_validation")

EXPECTED_TABLES = {
    "regions", "stations", "operators", "charger_locations", "chargers",
    "charger_characteristics", "charger_enrichment",
}


def add_result(rows: list[dict[str, object]], section: str, check: str,
               actual: object, expected: object, passed: bool,
               details: str = "") -> None:
    rows.append({
        "section": section,
        "check": check,
        "status": "PASS" if passed else "FAIL",
        "actual": actual,
        "expected": expected,
        "details": details,
    })


def nonempty_file(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 0


def count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def compare_query_with_parquet(con: duckdb.DuckDBPyConnection, query: str,
                               parquet_path: Path) -> tuple[int, int]:
    comparison = f"""
        WITH database_rows AS ({query}),
             parquet_rows AS (SELECT * FROM read_parquet(?))
        SELECT
          (SELECT COUNT(*) FROM (
             SELECT * FROM parquet_rows EXCEPT ALL SELECT * FROM database_rows
          )),
          (SELECT COUNT(*) FROM (
             SELECT * FROM database_rows EXCEPT ALL SELECT * FROM parquet_rows
          ))
    """
    return con.execute(comparison, [str(parquet_path)]).fetchone()


def validate(root: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    clean_path = root / "data/processed/chargers_clean.parquet"
    enrichment_path = root / "data/processed/charger_enrichment.parquet"
    database_path = root / "data/final/ev_chargers.duckdb"
    required = {
        "clean_parquet": clean_path,
        "enrichment_parquet": enrichment_path,
        "duckdb_database": database_path,
    }
    for name, path in required.items():
        exists = nonempty_file(path)
        add_result(rows, "inputs", f"nonempty_{name}", path.stat().st_size if exists else 0,
                   "> 0 bytes", exists, path.relative_to(root).as_posix())
    if not all(nonempty_file(path) for path in required.values()):
        return rows

    clean = pd.read_parquet(clean_path)
    enrichment = pd.read_parquet(enrichment_path)
    add_result(rows, "source_data", "clean_charger_id_unique",
               int(clean["charger_id"].nunique()), len(clean),
               clean["charger_id"].notna().all() and clean["charger_id"].is_unique)
    add_result(rows, "source_data", "enrichment_charger_id_unique",
               int(enrichment["charger_id"].nunique()), len(enrichment),
               enrichment["charger_id"].notna().all()
               and enrichment["charger_id"].is_unique)
    dc_ids = set(clean.loc[clean["charger_type"].eq("DC"), "charger_id"])
    enrichment_ids = set(enrichment["charger_id"])
    add_result(rows, "source_data", "enrichment_ids_equal_clean_dc_ids",
               len(enrichment_ids), len(dc_ids), enrichment_ids == dc_ids,
               f"orphans={len(enrichment_ids - set(clean['charger_id']))}, "
               f"missing_dc={len(dc_ids - enrichment_ids)}")

    con = duckdb.connect(str(database_path), read_only=True)
    spatial_loaded = False
    try:
        try:
            con.execute("LOAD spatial")
            spatial_state = con.execute("""
                SELECT installed, loaded, extension_version
                FROM duckdb_extensions() WHERE extension_name = ?
            """, ["spatial"]).fetchone()
            spatial_loaded = bool(spatial_state and spatial_state[0] and spatial_state[1])
            add_result(
                rows, "spatial", "spatial_extension_loaded",
                f"installed={spatial_state[0]}, loaded={spatial_state[1]}"
                if spatial_state else "not found",
                "installed=True, loaded=True", spatial_loaded,
                f"extension_version={spatial_state[2]}" if spatial_state else "",
            )
        except Exception as exc:
            add_result(rows, "spatial", "spatial_extension_loaded", str(exc),
                       "loads without error", False)

        actual_tables = {item[0] for item in con.execute("SHOW TABLES").fetchall()}
        add_result(rows, "schema", "expected_tables_present",
                   ";".join(sorted(actual_tables)), ";".join(sorted(EXPECTED_TABLES)),
                   EXPECTED_TABLES.issubset(actual_tables),
                   f"missing={sorted(EXPECTED_TABLES - actual_tables)}")
        if not EXPECTED_TABLES.issubset(actual_tables):
            return rows

        expected_counts = {
            "chargers": len(clean),
            "charger_characteristics": len(clean),
            "charger_enrichment": len(enrichment),
            "charger_locations": len(clean[["longitude", "latitude"]].drop_duplicates()),
            "stations": int(clean["station_id"].nunique()),
            "operators": int(clean["operator_name"].nunique()),
            "regions": int(clean["sa4_region"].nunique()),
        }
        for table, expected_count in expected_counts.items():
            actual_count = count(con, table)
            add_result(rows, "row_counts", f"row_count_{table}", actual_count,
                       expected_count, actual_count == expected_count)

        primary_keys = {
            "regions": "sa4_code", "stations": "station_id",
            "operators": "operator_id", "charger_locations": "location_id",
            "chargers": "charger_id", "charger_characteristics": "charger_id",
            "charger_enrichment": "charger_id",
        }
        for table, key in primary_keys.items():
            duplicates_or_nulls = con.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT {key}) FROM {table}
            """).fetchone()[0]
            add_result(rows, "primary_keys", f"pk_unique_{table}",
                       duplicates_or_nulls, 0, duplicates_or_nulls == 0,
                       f"key={table}.{key}")

        foreign_keys = {
            "stations_regions": """
                SELECT COUNT(*) FROM stations c LEFT JOIN regions p
                  ON c.sa4_code = p.sa4_code
                WHERE c.sa4_code IS NOT NULL AND p.sa4_code IS NULL
            """,
            "chargers_stations": """
                SELECT COUNT(*) FROM chargers c LEFT JOIN stations p
                  ON c.station_id = p.station_id WHERE p.station_id IS NULL
            """,
            "chargers_operators": """
                SELECT COUNT(*) FROM chargers c LEFT JOIN operators p
                  ON c.operator_id = p.operator_id WHERE p.operator_id IS NULL
            """,
            "chargers_locations": """
                SELECT COUNT(*) FROM chargers c LEFT JOIN charger_locations p
                  ON c.location_id = p.location_id WHERE p.location_id IS NULL
            """,
            "characteristics_chargers": """
                SELECT COUNT(*) FROM charger_characteristics c LEFT JOIN chargers p
                  ON c.charger_id = p.charger_id WHERE p.charger_id IS NULL
            """,
            "enrichment_chargers": """
                SELECT COUNT(*) FROM charger_enrichment c LEFT JOIN chargers p
                  ON c.charger_id = p.charger_id WHERE p.charger_id IS NULL
            """,
        }
        for name, query in foreign_keys.items():
            orphans = con.execute(query).fetchone()[0]
            add_result(rows, "foreign_keys", f"fk_{name}", orphans, 0, orphans == 0,
                       "child rows without a matching parent")

        joined_rows = con.execute("""
            SELECT COUNT(*) FROM chargers LEFT JOIN charger_enrichment USING (charger_id)
        """).fetchone()[0]
        add_result(rows, "enrichment", "join_preserves_charger_count", joined_rows,
                   len(clean), joined_rows == len(clean))
        ac_enrichment = con.execute("""
            SELECT COUNT(*) FROM charger_enrichment e
            JOIN charger_characteristics c USING (charger_id)
            WHERE c.charger_type = 'AC'
        """).fetchone()[0]
        add_result(rows, "enrichment", "ac_has_no_enrichment", ac_enrichment, 0,
                   ac_enrichment == 0)

        null_coordinates = con.execute("""
            SELECT COUNT(*) FROM charger_locations
            WHERE latitude IS NULL OR longitude IS NULL
        """).fetchone()[0]
        add_result(rows, "completeness", "coordinates_not_null", null_coordinates, 0,
                   null_coordinates == 0)
        missing_sa4 = con.execute("""
            SELECT COUNT(*) FROM chargers c JOIN stations s USING (station_id)
            LEFT JOIN regions r USING (sa4_code)
            WHERE s.sa4_code IS NULL OR r.sa4_code IS NULL
        """).fetchone()[0]
        add_result(rows, "completeness", "sa4_available", missing_sa4, 0,
                   missing_sa4 == 0)

        clean_database_query = """
            SELECT c.charger_id, c.station_id, c.source_objectid, c.station_name,
                   c.address, o.operator_name, k.number_of_plugs, k.charger_type,
                   k.rating_text, l.latitude, l.longitude, c.lga_name, s.postcode,
                   c.source_program, r.sa4_name AS sa4_region
            FROM chargers c JOIN operators o USING (operator_id)
            JOIN charger_locations l USING (location_id)
            JOIN charger_characteristics k USING (charger_id)
            JOIN stations s USING (station_id) LEFT JOIN regions r USING (sa4_code)
        """
        clean_source_only, clean_database_only = compare_query_with_parquet(
            con, clean_database_query, clean_path
        )
        add_result(rows, "reconciliation", "clean_parquet_equals_database",
                   f"source_only={clean_source_only}, database_only={clean_database_only}",
                   "source_only=0, database_only=0",
                   clean_source_only == 0 and clean_database_only == 0)

        enrichment_database_query = """
            SELECT c.charger_id, c.station_id, c.station_name, c.address,
                   o.operator_name, k.number_of_plugs, k.rating_text, k.rating_kw,
                   l.latitude, l.longitude, c.lga_name, s.postcode,
                   c.source_program, r.sa4_name AS sa4_region,
                   e.plug_types, e.charging_bays, e.pricing_information
            FROM chargers c JOIN operators o USING (operator_id)
            JOIN charger_locations l USING (location_id)
            JOIN charger_characteristics k USING (charger_id)
            JOIN stations s USING (station_id) LEFT JOIN regions r USING (sa4_code)
            LEFT JOIN charger_enrichment e USING (charger_id)
            WHERE k.charger_type = 'DC'
        """
        enrichment_source_only, enrichment_database_only = compare_query_with_parquet(
            con, enrichment_database_query, enrichment_path
        )
        add_result(rows, "reconciliation", "enrichment_parquet_equals_database",
                   f"source_only={enrichment_source_only}, "
                   f"database_only={enrichment_database_only}",
                   "source_only=0, database_only=0",
                   enrichment_source_only == 0 and enrichment_database_only == 0)

        if spatial_loaded:
            bad_points = con.execute("""
                SELECT COUNT(*) FROM charger_locations
                WHERE ST_GeometryType(location_geom) <> 'POINT'
                   OR ABS(ST_X(location_geom) - longitude) > 1e-9
                   OR ABS(ST_Y(location_geom) - latitude) > 1e-9
            """).fetchone()[0]
            add_result(rows, "spatial", "point_geometry_matches_coordinates",
                       bad_points, 0, bad_points == 0)
            spatial_matches = con.execute("""
                SELECT COUNT(*) FROM chargers c
                JOIN charger_locations l USING (location_id)
                JOIN stations s USING (station_id) JOIN regions r USING (sa4_code)
                WHERE ST_Intersects(l.location_geom, r.boundary_geom)
            """).fetchone()[0]
            add_result(rows, "spatial", "spatial_intersection_query",
                       spatial_matches, "> 0", spatial_matches > 0,
                       "one documented coastal fallback lies outside its polygon")
    finally:
        con.close()

    try:
        reopened = duckdb.connect(str(database_path), read_only=True)
        try:
            reopened.execute("LOAD spatial")
            reopened_count = count(reopened, "chargers")
            spatial_probe = reopened.execute("""
                SELECT ST_AsText(location_geom) FROM charger_locations LIMIT 1
            """).fetchone()[0]
            add_result(rows, "reopen", "closed_database_reopens",
                       reopened_count, len(clean),
                       reopened_count == len(clean) and spatial_probe.startswith("POINT"),
                       "read-only reopen plus spatial query")
        finally:
            reopened.close()
    except Exception as exc:
        add_result(rows, "reopen", "closed_database_reopens", str(exc),
                   "reopens and queries", False)
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate final pipeline outputs")
    parser.add_argument("--project-root", type=Path, default=Path("."))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    root = args.project_root.resolve()
    report_path = root / "data/quality/final_quality_report.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        results = validate(root)
    except Exception as exc:
        results = [{
            "section": "runtime", "check": "validation_completed", "status": "FAIL",
            "actual": type(exc).__name__, "expected": "no exception", "details": str(exc),
        }]
    pd.DataFrame(results).to_csv(report_path, index=False)
    failures = sum(row["status"] != "PASS" for row in results)
    if failures:
        LOG.error("Final validation failed: %s/%s checks failed; report: %s",
                  failures, len(results), report_path)
        return 1
    LOG.info("Final validation passed: %s checks; report: %s",
             len(results), report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
