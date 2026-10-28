"""Read-only A1 adapter. One generator row represents one A1 station."""
from __future__ import annotations

import argparse
import csv
import hashlib
from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.paths import load_json, write_metric_csv

STATION_FIELDS = ["station_id", "name", "lat", "lon", "site_type", "connectors", "max_kw"]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def export_stations(root: Path, profile: str, data_root: Path | None = None) -> Path:
    import duckdb

    data_root = data_root or root / "A2/data"
    config = load_json(root / f"A2/config/generator.{profile}.json")
    database = root / "A1/data/final/ev_chargers.duckdb"
    with duckdb.connect(str(database), read_only=True) as connection:
        records = connection.execute("""
            SELECT c.station_id, c.charger_id, c.station_name, c.address,
                   l.latitude, l.longitude, cc.number_of_plugs, cc.rating_kw,
                   r.sa4_name
            FROM chargers c
            JOIN charger_characteristics cc USING (charger_id)
            JOIN charger_locations l USING (location_id)
            JOIN stations s USING (station_id)
            LEFT JOIN regions r USING (sa4_code)
            WHERE cc.charger_type = 'DC' AND cc.rating_kw > 0
            ORDER BY c.station_id, cc.rating_kw DESC, c.charger_id
        """).fetchall()
    groups: dict[str, list] = {}
    for row in records:
        groups.setdefault(row[0], []).append(row)
    eligible_count = len(groups)
    limit = config.get("max_stations")
    selected = sorted(groups)[:limit] if limit is not None else sorted(groups)
    if not selected:
        raise ValueError("No eligible A1 DC stations found")
    urban = set(config["urban_sa4_regions"])
    stations, mapping = [], []
    heterogeneous = 0
    for station_id in selected:
        rows = groups[station_id]
        _, _, name, address, lat, lon, _, rating, region = rows[0]
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError(f"Invalid coordinates: {station_id}")
        connectors = sum(row[6] for row in rows)
        heterogeneous += len({row[7] for row in rows}) > 1
        stations.append(dict(zip(STATION_FIELDS, [
            station_id, name or address, lat, lon,
            "urban" if region in urban else "regional", connectors, rating,
        ])))
        connector_id = 0
        for row in rows:
            for plug_number in range(1, row[6] + 1):
                connector_id += 1
                mapping.append({"station_id": station_id, "connector_id": connector_id,
                                "charger_id": row[1], "plug_number": plug_number,
                                "source_rating_kw": row[7]})
    destination = data_root / "input/generator_stations.csv"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=STATION_FIELDS)
        writer.writeheader()
        writer.writerows(stations)
    with destination.with_name("station_connector_mapping.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(mapping[0]))
        writer.writeheader()
        writer.writerows(mapping)
    write_metric_csv(data_root / "quality/station_export_summary.csv", [
        ("profile", profile, "Generator profile used for station selection"),
        ("eligible_dc_stations", eligible_count, "A1 stations with rated DC chargers"),
        ("exported_stations", len(stations), "Sorted A1 station_id; no identifiers invented"),
        ("exported_connectors", len(mapping), "Sum of eligible DC number_of_plugs"),
        ("urban_stations", sum(s["site_type"] == "urban" for s in stations), "Configured SA4 proxy"),
        ("heterogeneous_power_stations", heterogeneous, "Maximum DC rating used for every connector"),
        ("a1_database_sha256", sha256(database), "Read-only source fingerprint"),
        ("station_csv_sha256", sha256(destination), "Checked by generator wrapper"),
    ])
    print(f"Exported {len(stations)} stations / {len(mapping)} connectors -> {destination}")
    return destination


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--profile", choices=["dev", "assessment"], default="assessment")
    parser.add_argument("--data-root", type=Path, help="Separate output tree, e.g. A2/data/dev")
    args = parser.parse_args(argv)
    export_stations(args.project_root.resolve(), args.profile, args.data_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
