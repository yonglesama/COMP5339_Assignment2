from __future__ import annotations

import argparse
from pathlib import Path
import sys

import duckdb

sys.dont_write_bytecode = True

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.paths import project_path, write_metric_csv


REQUIRED_TABLES = {
    "regions",
    "stations",
    "operators",
    "charger_locations",
    "chargers",
    "charger_characteristics",
    "charger_enrichment",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the Assignment 1 database contract for A2.")
    parser.add_argument("--project-root", type=Path, required=True)
    args = parser.parse_args(argv)
    database = project_path(args.project_root, "A1/data/final/ev_chargers.duckdb")
    output = project_path(args.project_root, "A2/data/quality/a1_validation.csv")

    rows: list[tuple[str, object, str]] = []
    failures: list[str] = []
    connection = duckdb.connect(str(database), read_only=True)
    try:
        actual_tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        missing = sorted(REQUIRED_TABLES - actual_tables)
        rows.append(("required_tables_present", not missing, ", ".join(missing) or "all present"))
        if missing:
            failures.append(f"missing A1 tables: {', '.join(missing)}")
        else:
            checks = [
                ("charger_count", "SELECT COUNT(*) FROM chargers", "A1 charger records"),
                ("station_count", "SELECT COUNT(*) FROM stations", "A1 station records"),
                (
                    "dc_station_count",
                    """SELECT COUNT(DISTINCT c.station_id)
                       FROM chargers c
                       JOIN charger_characteristics cc USING (charger_id)
                       WHERE cc.charger_type = 'DC' AND cc.rating_kw IS NOT NULL""",
                    "eligible fast-charger stations",
                ),
                (
                    "orphan_chargers",
                    """SELECT COUNT(*) FROM chargers c
                       LEFT JOIN stations s USING (station_id)
                       LEFT JOIN charger_locations l USING (location_id)
                       LEFT JOIN operators o USING (operator_id)
                       WHERE s.station_id IS NULL OR l.location_id IS NULL OR o.operator_id IS NULL""",
                    "must be zero",
                ),
                (
                    "invalid_coordinates",
                    """SELECT COUNT(*) FROM charger_locations
                       WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180""",
                    "must be zero",
                ),
            ]
            for metric, sql, note in checks:
                value = connection.execute(sql).fetchone()[0]
                rows.append((metric, value, note))
                if metric in {"orphan_chargers", "invalid_coordinates"} and value != 0:
                    failures.append(f"{metric}={value}")
    finally:
        connection.close()

    rows.append(("validation_passed", not failures, "; ".join(failures) or "all checks passed"))
    write_metric_csv(output, rows)
    if failures:
        raise RuntimeError("; ".join(failures))
    print(f"A1 contract valid; report written to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
