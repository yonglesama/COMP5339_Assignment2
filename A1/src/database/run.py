"""Build and validate the DuckDB database for the EV charger pipeline."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import tempfile

import duckdb
import numpy as np
import pandas as pd


LOG = logging.getLogger("database")


def input_path(root: Path, relative: str) -> Path:
    path = root / relative
    if not path.is_file() or path.stat().st_size == 0:
        raise FileNotFoundError(f"Missing or empty input: {path}")
    return path


def require_key(df: pd.DataFrame, column: str, label: str) -> None:
    if df[column].isna().any() or not df[column].is_unique:
        raise ValueError(f"{label}.{column} must be non-null and unique")


def validate_inputs(clean: pd.DataFrame, enrichment: pd.DataFrame,
                    audit: pd.DataFrame) -> None:
    clean_columns = {
        "charger_id", "station_id", "source_objectid", "station_name", "address",
        "operator_name", "number_of_plugs", "charger_type", "rating_text",
        "latitude", "longitude", "lga_name", "postcode", "source_program",
        "sa4_region",
    }
    enrichment_columns = {
        "charger_id", "station_id", "rating_kw", "plug_types", "charging_bays",
        "pricing_information",
    }
    for label, frame, required in (
        ("clean", clean, clean_columns),
        ("enrichment", enrichment, enrichment_columns),
        ("audit", audit, {"charger_id", "rating_kw"}),
    ):
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"{label} is missing columns: {sorted(missing)}")
        require_key(frame, "charger_id", label)

    if clean.empty or clean["station_id"].isna().any():
        raise ValueError("Clean data must contain non-null station IDs")
    if clean["operator_name"].isna().any():
        raise ValueError("Clean data must contain non-null operator names")
    if set(audit["charger_id"]) != set(clean["charger_id"]):
        raise ValueError("Audit charger IDs do not match clean charger IDs")

    dc_ids = set(clean.loc[clean["charger_type"].eq("DC"), "charger_id"])
    enrichment_ids = set(enrichment["charger_id"])
    orphan_ids = enrichment_ids - set(clean["charger_id"])
    if orphan_ids:
        raise ValueError(f"Enrichment contains orphan charger IDs: {sorted(orphan_ids)[:5]}")
    if enrichment_ids != dc_ids:
        raise ValueError("Enrichment charger IDs must equal the clean DC subset")
    matched = clean.set_index("charger_id").loc[enrichment["charger_id"]]
    if matched["station_id"].tolist() != enrichment["station_id"].tolist():
        raise ValueError("Enrichment station IDs disagree with clean data")
    audit_kw = audit.set_index("charger_id").loc[enrichment["charger_id"], "rating_kw"]
    if not np.allclose(audit_kw.to_numpy(dtype=float),
                       enrichment["rating_kw"].to_numpy(dtype=float), equal_nan=True):
        raise ValueError("Enrichment ratings disagree with the cleaning audit")

    joined = clean[["charger_id", "charger_type"]].merge(
        enrichment[["charger_id"]], on="charger_id", how="left",
        validate="one_to_one", indicator=True,
    )
    if len(joined) != len(clean):
        raise ValueError("Enrichment join changed the number of clean charger rows")
    ac_matches = int(joined.loc[joined["charger_type"].eq("AC"), "_merge"].eq("both").sum())
    missing_dc = int(joined.loc[joined["charger_type"].eq("DC"), "_merge"].ne("both").sum())
    if ac_matches or missing_dc:
        raise ValueError(
            f"Invalid enrichment membership: {ac_matches} AC matches, "
            f"{missing_dc} missing DC matches"
        )
    augmented_rows = int(enrichment[
        ["plug_types", "charging_bays", "pricing_information"]
    ].notna().any(axis=1).sum())
    LOG.info(
        "Enrichment keys validated: %s unique DC charger IDs, %s joined rows, "
        "0 AC matches, 0 orphans, %s rows with added attributes",
        len(enrichment), len(joined), augmented_rows,
    )

    for label, frame, columns in (
        ("clean", clean, ["postcode", "sa4_region"]),
        ("enrichment", enrichment,
         ["plug_types", "charging_bays", "pricing_information"]),
    ):
        conflicts = frame.groupby("station_id")[columns].nunique(dropna=False).gt(1)
        if conflicts.any().any():
            raise ValueError(f"{label} has conflicting station-level values: "
                             f"{conflicts.index[conflicts.any(axis=1)].tolist()[:5]}")


def count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def build_database(root: Path, destination: Path) -> dict[str, int]:
    clean = pd.read_parquet(input_path(root, "data/processed/chargers_clean.parquet"))
    enrichment = pd.read_parquet(
        input_path(root, "data/processed/charger_enrichment.parquet")
    )
    audit = pd.read_csv(
        input_path(root, "data/quality/chargers_clean_audit.csv"),
        usecols=["charger_id", "rating_kw"],
        dtype={"charger_id": "string"},
    )
    schema = input_path(root, "sql/schema.sql").read_text(encoding="utf-8-sig")
    shapefile = input_path(
        root,
        "data/raw/abs/SA4_2026_AUST_SHP_GDA2020/SA4_2026_AUST_GDA2020.shp",
    )
    validate_inputs(clean, enrichment, audit)

    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix="ev_chargers_", suffix=".duckdb", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    temporary.unlink()  # DuckDB creates its own database file at this reserved path.
    try:
        con = duckdb.connect(str(temporary))
        try:
            con.execute(schema)
            con.register("clean_input", clean)
            con.register("enrichment_input", enrichment)
            con.register("audit_input", audit)
            con.begin()
            try:
                con.execute("""
                    INSERT INTO regions
                    SELECT SA4_CODE26, SA4_NAME26,
                           ST_Transform(geom, 'EPSG:7844', 'EPSG:4326', TRUE)
                    FROM ST_Read(?)
                    WHERE STE_CODE26 = '1' AND geom IS NOT NULL
                """, [str(shapefile)])
                unknown_regions = con.execute("""
                    SELECT DISTINCT sa4_region FROM clean_input
                    WHERE sa4_region IS NOT NULL
                    EXCEPT SELECT sa4_name FROM regions
                """).fetchall()
                if unknown_regions:
                    raise ValueError(f"Unknown SA4 names: {unknown_regions[:5]}")

                con.execute("""
                    INSERT INTO operators
                    SELECT ROW_NUMBER() OVER (ORDER BY operator_name), operator_name
                    FROM (SELECT DISTINCT operator_name FROM clean_input)
                """)
                con.execute("""
                    INSERT INTO stations
                    SELECT c.station_id, r.sa4_code, c.postcode
                    FROM (SELECT DISTINCT station_id, sa4_region, postcode
                          FROM clean_input) AS c
                    LEFT JOIN regions AS r ON c.sa4_region = r.sa4_name
                """)
                con.execute("""
                    INSERT INTO charger_locations
                        (location_id, latitude, longitude, location_geom)
                    SELECT CAST(ROW_NUMBER() OVER (
                               ORDER BY longitude, latitude) AS INTEGER),
                           latitude, longitude, ST_Point(longitude, latitude)
                    FROM (SELECT DISTINCT latitude, longitude FROM clean_input)
                """)
                con.execute("""
                    INSERT INTO chargers
                        (charger_id, station_id, operator_id, location_id,
                         source_objectid, station_name, address, lga_name,
                         source_program)
                    SELECT c.charger_id, c.station_id, o.operator_id, l.location_id,
                           c.source_objectid, c.station_name, c.address,
                           c.lga_name, c.source_program
                    FROM clean_input AS c
                    JOIN operators AS o ON c.operator_name = o.operator_name
                    JOIN charger_locations AS l
                      ON c.longitude = l.longitude AND c.latitude = l.latitude
                """)
                con.execute("""
                    INSERT INTO charger_characteristics
                        (charger_id, charger_type, number_of_plugs,
                         rating_text, rating_kw)
                    SELECT c.charger_id, c.charger_type, c.number_of_plugs,
                           c.rating_text, a.rating_kw
                    FROM clean_input AS c
                    JOIN audit_input AS a ON c.charger_id = a.charger_id
                """)
                con.execute("""
                    INSERT INTO charger_enrichment
                        (charger_id, plug_types, charging_bays, pricing_information)
                    SELECT charger_id, plug_types, charging_bays, pricing_information
                    FROM enrichment_input
                """)

                expected = {
                    "regions": clean["sa4_region"].nunique(),
                    "stations": clean["station_id"].nunique(),
                    "operators": clean["operator_name"].nunique(),
                    "charger_locations": len(
                        clean[["longitude", "latitude"]].drop_duplicates()
                    ),
                    "chargers": len(clean),
                    "charger_characteristics": len(clean),
                    "charger_enrichment": len(enrichment),
                }
                for table, expected_rows in expected.items():
                    if count(con, table) != expected_rows:
                        raise ValueError(f"{table}: expected {expected_rows} rows")
                if expected["regions"] == 0:
                    raise ValueError("No NSW SA4 regions were imported")
                con.commit()
            except Exception:
                con.rollback()
                raise
        finally:
            con.close()

        # Reopening proves that the file and its spatial columns are queryable.
        validation_rows: list[dict[str, object]] = []

        def check_result(name: str, actual: object, expected_value: object,
                         passed: bool, details: str) -> None:
            validation_rows.append({
                "check": name,
                "status": "PASS" if passed else "FAIL",
                "actual": actual,
                "expected": expected_value,
                "details": details,
            })
            if not passed:
                raise ValueError(f"Database validation failed: {name} ({details})")

        verify = duckdb.connect(str(temporary), read_only=True)
        try:
            verify.execute("LOAD spatial")
            spatial_state = verify.execute("""
                SELECT installed, loaded, extension_version
                FROM duckdb_extensions()
                WHERE extension_name = ?
            """, ["spatial"]).fetchone()
            spatial_ok = bool(spatial_state and spatial_state[0] and spatial_state[1])
            check_result(
                "spatial_extension_loaded",
                f"installed={spatial_state[0]}, loaded={spatial_state[1]}"
                if spatial_state else "extension not found",
                "installed=True, loaded=True",
                spatial_ok,
                f"extension_version={spatial_state[2]}" if spatial_state else "",
            )
            for table, expected_rows in expected.items():
                actual_rows = count(verify, table)
                check_result(
                    f"row_count_{table}", actual_rows, expected_rows,
                    actual_rows == expected_rows,
                    "database was closed and reopened before this query",
                )

            primary_keys = {
                "regions": "sa4_code",
                "stations": "station_id",
                "operators": "operator_id",
                "charger_locations": "location_id",
                "chargers": "charger_id",
                "charger_characteristics": "charger_id",
                "charger_enrichment": "charger_id",
            }
            for table, key in primary_keys.items():
                duplicates = verify.execute(f"""
                    SELECT COUNT(*) - COUNT(DISTINCT {key}) FROM {table}
                """).fetchone()[0]
                check_result(
                    f"pk_unique_{table}", duplicates, 0, duplicates == 0,
                    f"duplicate or null values for {table}.{key}",
                )

            foreign_key_checks = {
                "fk_stations_regions": """
                    SELECT COUNT(*) FROM stations c LEFT JOIN regions p
                      ON c.sa4_code = p.sa4_code
                    WHERE c.sa4_code IS NOT NULL AND p.sa4_code IS NULL
                """,
                "fk_chargers_stations": """
                    SELECT COUNT(*) FROM chargers c LEFT JOIN stations p
                      ON c.station_id = p.station_id WHERE p.station_id IS NULL
                """,
                "fk_chargers_operators": """
                    SELECT COUNT(*) FROM chargers c LEFT JOIN operators p
                      ON c.operator_id = p.operator_id WHERE p.operator_id IS NULL
                """,
                "fk_chargers_locations": """
                    SELECT COUNT(*) FROM chargers c LEFT JOIN charger_locations p
                      ON c.location_id = p.location_id WHERE p.location_id IS NULL
                """,
                "fk_characteristics_chargers": """
                    SELECT COUNT(*) FROM charger_characteristics c LEFT JOIN chargers p
                      ON c.charger_id = p.charger_id WHERE p.charger_id IS NULL
                """,
                "fk_enrichment_chargers": """
                    SELECT COUNT(*) FROM charger_enrichment c LEFT JOIN chargers p
                      ON c.charger_id = p.charger_id WHERE p.charger_id IS NULL
                """,
            }
            for name, query in foreign_key_checks.items():
                orphan_count = verify.execute(query).fetchone()[0]
                check_result(
                    name, orphan_count, 0, orphan_count == 0,
                    "child rows without a matching parent",
                )

            joined_rows = verify.execute("""
                SELECT COUNT(*) FROM chargers
                LEFT JOIN charger_enrichment USING (charger_id)
            """).fetchone()[0]
            check_result(
                "enrichment_join_preserves_chargers",
                joined_rows, expected["chargers"],
                joined_rows == expected["chargers"],
                "a one-to-one left join must not multiply charger rows",
            )

            null_coordinates = verify.execute("""
                SELECT COUNT(*) FROM charger_locations
                WHERE latitude IS NULL OR longitude IS NULL
            """).fetchone()[0]
            check_result(
                "coordinates_not_null", null_coordinates, 0,
                null_coordinates == 0, "null latitude or longitude values",
            )

            missing_sa4 = verify.execute("""
                SELECT COUNT(*) FROM chargers AS c
                JOIN stations AS s USING (station_id)
                LEFT JOIN regions AS r USING (sa4_code)
                WHERE s.sa4_code IS NULL OR r.sa4_code IS NULL
            """).fetchone()[0]
            check_result(
                "sa4_available_for_all_chargers", missing_sa4, 0,
                missing_sa4 == 0, "chargers without a valid assigned SA4 region",
            )

            bad_points = verify.execute("""
                SELECT COUNT(*) FROM charger_locations
                WHERE ST_GeometryType(location_geom) <> 'POINT'
                   OR ABS(ST_X(location_geom) - longitude) > 1e-9
                   OR ABS(ST_Y(location_geom) - latitude) > 1e-9
            """).fetchone()[0]
            check_result(
                "point_geometry_matches_coordinates", bad_points, 0,
                bad_points == 0, "invalid point type or X/Y differs from longitude/latitude",
            )
            point_count = verify.execute("""
                SELECT COUNT(*) FROM charger_locations
                WHERE ST_GeometryType(location_geom) = 'POINT'
            """).fetchone()[0]
            check_result(
                "point_geometry_query", point_count, expected["charger_locations"],
                point_count == expected["charger_locations"],
                "ST_GeometryType successfully queried stored charger geometry",
            )
            region_geometry_count = verify.execute("""
                SELECT COUNT(*) FROM regions
                WHERE ST_GeometryType(boundary_geom) IN ('POLYGON', 'MULTIPOLYGON')
            """).fetchone()[0]
            check_result(
                "region_geometry_query", region_geometry_count, expected["regions"],
                region_geometry_count == expected["regions"],
                "stored SA4 geometries are polygons or multipolygons",
            )
            bad_enrichment_links = verify.execute("""
                SELECT COUNT(*)
                FROM charger_enrichment AS e
                JOIN charger_characteristics AS k USING (charger_id)
                WHERE k.charger_type <> 'DC'
            """).fetchone()[0]
            check_result(
                "enrichment_dc_only", bad_enrichment_links, 0,
                bad_enrichment_links == 0, "AC chargers with enrichment rows",
            )
            spatial_matches = verify.execute("""
                SELECT COUNT(*) FROM chargers AS c
                JOIN charger_locations AS l USING (location_id)
                JOIN stations AS s USING (station_id)
                JOIN regions AS r USING (sa4_code)
                WHERE ST_Intersects(l.location_geom, r.boundary_geom)
            """).fetchone()[0]
            check_result(
                "spatial_intersection_query", spatial_matches, "> 0",
                spatial_matches > 0,
                "ST_Intersects on charger points and assigned SA4 polygons",
            )
            reopened_count = verify.execute("SELECT COUNT(*) FROM chargers").fetchone()[0]
            check_result(
                "database_reopen_query", reopened_count, expected["chargers"],
                reopened_count == expected["chargers"],
                "read-only query after closing the build connection and reopening the file",
            )
            LOG.info(
                "Spatial extension %s loaded; %s point geometries, %s region "
                "geometries, %s point-in-region matches",
                spatial_state[2], point_count, region_geometry_count, spatial_matches,
            )
        finally:
            verify.close()

        os.replace(temporary, destination)
        validation_path = root / "data/quality/database_validation.csv"
        validation_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(validation_rows).to_csv(validation_path, index=False)
        LOG.info("Database validation: %s checks passed; report: %s",
                 len(validation_rows), validation_path)
        return expected
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the EV charger DuckDB database")
    parser.add_argument("--project-root", type=Path, default=Path("."))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    root = args.project_root.resolve()
    try:
        destination = root / "data/final/ev_chargers.duckdb"
        counts = build_database(root, destination)
        LOG.info("Created %s with %s", destination, counts)
        return 0
    except Exception as exc:
        LOG.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
