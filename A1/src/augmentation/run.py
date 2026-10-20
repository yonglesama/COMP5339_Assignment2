"""Add three OpenStreetMap fields to Member B's DC charger data.

Final output: the 14-column spatially joined DC data plus three OSM attributes.
Matching details are written to a separate review CSV so the final table stays simple.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import re
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd


LOG = logging.getLogger("augmentation")

INPUT_DC = Path("data/processed/dc_candidates.parquet")
RAW_DIR = Path("data/raw/augmentation")
RAW_JSON = RAW_DIR / "osm_nsw_charging_stations.json"
RAW_CSV = RAW_DIR / "osm_nsw_charging_stations.csv"
SOURCE_METADATA = RAW_DIR / "source_metadata.json"
OUT_ENRICHMENT = Path("data/processed/charger_enrichment.parquet")
OUT_COVERAGE = Path("data/quality/augmentation_coverage.csv")
OUT_REVIEW = Path("data/quality/matching_review.csv")

NEW_COLUMNS = ["plug_types", "charging_bays", "pricing_information"]
MAX_MATCH_DISTANCE_M = 100.0
COORDINATE_ONLY_DISTANCE_M = 25.0
TARGET_PERCENT = 50.0

OVERPASS_ENDPOINTS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)
OVERPASS_QUERY = """[out:json][timeout:120];
nwr["amenity"="charging_station"](-37.6,140.9,-28.0,153.7);
out center tags;
"""
USER_AGENT = "COMP5339-assignment-data-augmentation/1.0"
OSM_ATTRIBUTION = "© OpenStreetMap contributors"
OSM_LICENCE = "Open Data Commons Open Database License (ODbL)"

REQUIRED_COLUMNS = {
    "station_id",
    "station_name",
    "operator_name",
    "address",
    "longitude",
    "latitude",
    "sa4_region",
}

SOCKET_LABELS = {
    "type2_combo": "CCS Type 2",
    "chademo": "CHAdeMO",
    "type2": "Type 2",
    "type2_cable": "Type 2 cable",
    "tesla_supercharger": "Tesla Supercharger",
    "tesla_supercharger_ccs": "Tesla Supercharger CCS",
    "nacs": "NACS",
    "type1_combo": "CCS Type 1",
    "type1": "Type 1",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_value(value: Any) -> str | None:
    """Turn blank values and NaN into None."""
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def normalise_operator(value: Any) -> str:
    """Make common operator name variants comparable."""
    text = re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())
    aliases = (
        (("evienetworks", "evie"), "evie"),
        (("ampcharge", "ampol"), "ampol"),
        (("bppulse", "bp"), "bp"),
        (("teslasupercharger", "tesla"), "tesla"),
        (("nrmaelectric", "nrma"), "nrma"),
        (("joltcharge", "jolt"), "jolt"),
        (("chargefox",), "chargefox"),
        (("exploren",), "exploren"),
        (("engie",), "engie"),
        (("everty",), "everty"),
        (("eveaustralia",), "eveaustralia"),
        (("wevolt",), "wevolt"),
        (("zeusrenewables", "zeus"), "zeus"),
        (("evup",), "evup"),
        (("chargehub",), "chargehub"),
        (("vivaenergyaustralia", "vivaenergy"), "vivaenergy"),
    )
    for variants, standard_name in aliases:
        if any(variant in text for variant in variants):
            return standard_name
    return text


# Backwards-compatible name used by the existing tests and documentation.
canonical_operator = normalise_operator


def operator_compatible(source_values: Any, osm_values: list[Any]) -> bool:
    if not isinstance(source_values, (list, tuple, set)):
        source_values = [source_values]
    source_names = {normalise_operator(value) for value in source_values if clean_value(value)}
    osm_names = {normalise_operator(value) for value in osm_values if clean_value(value)}
    return bool(source_names & osm_names)


def osm_coordinates(element: dict[str, Any]) -> tuple[float | None, float | None]:
    if element.get("lat") is not None and element.get("lon") is not None:
        return float(element["lat"]), float(element["lon"])
    center = element.get("center") or {}
    if center.get("lat") is not None and center.get("lon") is not None:
        return float(center["lat"]), float(center["lon"])
    return None, None


def flatten_osm(payload: dict[str, Any]) -> pd.DataFrame:
    """Convert the downloaded OSM JSON into a table."""
    rows = []
    for element in payload.get("elements", []):
        latitude, longitude = osm_coordinates(element)
        if latitude is None or longitude is None:
            continue
        rows.append(
            {
                "osm_element_type": element.get("type"),
                "osm_element_id": element.get("id"),
                "latitude": latitude,
                "longitude": longitude,
                **(element.get("tags") or {}),
            }
        )
    if not rows:
        raise ValueError("The OSM response has no charging stations with coordinates")
    return pd.DataFrame(rows).sort_values(
        ["osm_element_type", "osm_element_id"], kind="stable"
    ).reset_index(drop=True)


def download_osm() -> tuple[dict[str, Any], str]:
    body = urlencode({"data": OVERPASS_QUERY}).encode("utf-8")
    errors = []
    for endpoint in OVERPASS_ENDPOINTS:
        for attempt in range(1, 4):
            request = Request(endpoint, data=body, headers={"User-Agent": USER_AGENT})
            try:
                with urlopen(request, timeout=180) as response:
                    payload = json.load(response)
                if payload.get("elements"):
                    return payload, endpoint
                raise ValueError("response has no elements")
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
                errors.append(f"{endpoint} attempt {attempt}: {exc}")
                if attempt < 3:
                    time.sleep(attempt * 2)
    raise RuntimeError("Unable to retrieve OSM data. " + " | ".join(errors))


def load_osm(root: Path, refresh: bool = False) -> tuple[dict[str, Any], dict[str, Any]]:
    """Use the saved response by default; download only when needed."""
    json_path = root / RAW_JSON
    metadata_path = root / SOURCE_METADATA
    json_path.parent.mkdir(parents=True, exist_ok=True)

    if json_path.exists() and not refresh:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        else:
            metadata = {
                "source_name": "OpenStreetMap charging stations via Overpass API",
                "endpoint": OVERPASS_ENDPOINTS[0],
                "osm_data_timestamp_utc": (payload.get("osm3s") or {}).get("timestamp_osm_base"),
                "retrieved_at_utc": datetime.fromtimestamp(json_path.stat().st_mtime, timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
                "record_count": len(payload.get("elements", [])),
                "attribution": OSM_ATTRIBUTION,
                "licence": OSM_LICENCE,
            }
            metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        metadata["cache_used"] = True
        return payload, metadata

    payload, endpoint = download_osm()
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    metadata = {
        "source_name": "OpenStreetMap charging stations via Overpass API",
        "endpoint": endpoint,
        "query": OVERPASS_QUERY,
        "retrieved_at_utc": utc_now(),
        "osm_data_timestamp_utc": (payload.get("osm3s") or {}).get("timestamp_osm_base"),
        "record_count": len(payload.get("elements", [])),
        "attribution": OSM_ATTRIBUTION,
        "licence": OSM_LICENCE,
        "cache_used": False,
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload, metadata


def make_station_table(dc: pd.DataFrame) -> pd.DataFrame:
    """Combine repeated charger rows so each station is matched only once."""
    missing = sorted(REQUIRED_COLUMNS - set(dc.columns))
    if missing:
        raise ValueError(f"DC input is missing columns: {', '.join(missing)}")
    if dc["station_id"].isna().any():
        raise ValueError("DC input contains a null station_id")

    def join_unique(values: pd.Series) -> str | None:
        items = []
        for value in values:
            text = clean_value(value)
            if text and text not in items:
                items.append(text)
        return " | ".join(items) if items else None

    rows = []
    sort_columns = ["station_id"] + (["charger_id"] if "charger_id" in dc.columns else [])
    for station_id, group in dc.sort_values(sort_columns, kind="stable").groupby("station_id", sort=False):
        rows.append(
            {
                "station_id": station_id,
                "station_name": join_unique(group["station_name"]),
                "operator_name": join_unique(group["operator_name"]),
                "operator_values": group["operator_name"].dropna().unique().tolist(),
                "address": join_unique(group["address"]),
                "postcode": join_unique(group["postcode"]) if "postcode" in group else None,
                "lga_name": join_unique(group["lga_name"]) if "lga_name" in group else None,
                "longitude": float(group["longitude"].median()),
                "latitude": float(group["latitude"].median()),
            }
        )
    return pd.DataFrame(rows)


def distance_matrix(stations: pd.DataFrame, osm: pd.DataFrame) -> np.ndarray:
    """Calculate all station-to-OSM distances with the Haversine formula."""
    lat1 = np.radians(stations["latitude"].to_numpy(dtype=float))[:, None]
    lon1 = np.radians(stations["longitude"].to_numpy(dtype=float))[:, None]
    lat2 = np.radians(osm["latitude"].to_numpy(dtype=float))[None, :]
    lon2 = np.radians(osm["longitude"].to_numpy(dtype=float))[None, :]
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    value = np.sin(delta_lat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(delta_lon / 2) ** 2
    return 6_371_000 * 2 * np.arcsin(np.sqrt(value))


def choose_matches(stations: pd.DataFrame, osm: pd.DataFrame) -> pd.DataFrame:
    """Match by operator + distance, or by very close coordinates when OSM has no operator."""
    if "operator_values" not in stations:
        stations = stations.copy()
        stations["operator_values"] = stations["operator_name"].map(lambda value: [value])

    distances = distance_matrix(stations, osm)
    rows = []
    operator_columns = ["operator", "brand", "network"]

    for station_position, (_, station) in enumerate(stations.iterrows()):
        nearby = np.flatnonzero(distances[station_position] <= MAX_MATCH_DISTANCE_M)
        candidates = []
        for osm_position in nearby:
            osm_row = osm.iloc[int(osm_position)]
            names = [osm_row.get(column) for column in operator_columns]
            has_name = any(clean_value(value) for value in names)
            same_operator = operator_compatible(station["operator_values"], names)
            candidates.append(
                (int(osm_position), float(distances[station_position, osm_position]), same_operator, has_name)
            )

        compatible = [candidate for candidate in candidates if candidate[2]]
        chosen = None
        method = None
        confidence = None
        if compatible:
            chosen = min(compatible, key=lambda item: item[1])
            method = "operator_and_coordinate"
            confidence = "high" if chosen[1] <= 50 else "medium"
            reason = "operator-compatible match within 100 m"
        elif candidates:
            nearest = min(candidates, key=lambda item: item[1])
            if nearest[1] <= COORDINATE_ONLY_DISTANCE_M and not nearest[3]:
                chosen = nearest
                method = "coordinate_only_no_external_operator"
                confidence = "medium" if nearest[1] <= 10 else "low"
                reason = "coordinate-only match; OSM has no operator, brand or network"
            elif nearest[3]:
                reason = "nearby OSM object has a conflicting operator, brand or network"
            else:
                reason = "nearest unlabelled OSM object is beyond 25 m"
        else:
            reason = "no OSM charging station within 100 m"

        row = {
            "station_id": station["station_id"],
            "source_station_name": clean_value(station["station_name"]),
            "source_operator": clean_value(station["operator_name"]),
            "source_address": clean_value(station["address"]),
            "source_postcode": clean_value(station["postcode"]),
            "source_lga_name": clean_value(station["lga_name"]),
            "source_latitude": float(station["latitude"]),
            "source_longitude": float(station["longitude"]),
            "candidate_count_100m": len(candidates),
            "match_status": "matched" if chosen else "unmatched",
            "match_method": method,
            "match_confidence": confidence,
            "review_reason": reason,
            "osm_row_index": None,
        }
        if chosen:
            osm_row = osm.iloc[chosen[0]]
            row.update(
                {
                    "match_distance_m": round(chosen[1], 3),
                    "operator_match": bool(chosen[2]),
                    "osm_row_index": chosen[0],
                    "osm_element_type": osm_row.get("osm_element_type"),
                    "osm_element_id": osm_row.get("osm_element_id"),
                    "osm_name": osm_row.get("name"),
                    "osm_operator": osm_row.get("operator"),
                    "osm_brand": osm_row.get("brand"),
                    "osm_latitude": osm_row.get("latitude"),
                    "osm_longitude": osm_row.get("longitude"),
                }
            )
        rows.append(row)
    return pd.DataFrame(rows)


def read_plug_types(osm_row: pd.Series) -> str | None:
    plugs: list[tuple[str, str]] = []
    for column, value in osm_row.items():
        column = str(column)
        if not column.startswith("socket:"):
            continue
        socket_name = column.removeprefix("socket:")
        if ":" in socket_name or not clean_value(value):
            continue
        if str(value).strip().casefold() in {"no", "0", "false"}:
            continue
        label = SOCKET_LABELS.get(socket_name, socket_name.replace("_", " ").title())
        if (socket_name, label) not in plugs:
            plugs.append((socket_name, label))
    return "; ".join(label for _, label in sorted(plugs)) if plugs else None


def socket_attributes(row: pd.Series) -> tuple[str | None, str | None]:
    """Compatibility helper: the final file uses only the first returned value."""
    values = {}
    for column, value in row.items():
        column = str(column)
        if not column.startswith("socket:"):
            continue
        socket_name = column.removeprefix("socket:")
        if ":" in socket_name or not clean_value(value):
            continue
        if str(value).strip().casefold() in {"no", "0", "false"}:
            continue
        values[socket_name] = str(value).strip()
    return read_plug_types(row), (json.dumps(values, ensure_ascii=False, sort_keys=True) if values else None)


def positive_integer(value: Any) -> int | None:
    text = clean_value(value)
    if not text or not re.fullmatch(r"\d+", text):
        return None
    number = int(text)
    return number if number > 0 else None


def read_pricing(osm_row: pd.Series) -> str | None:
    charge = clean_value(osm_row.get("charge"))
    fee = clean_value(osm_row.get("fee"))
    if charge:
        return charge
    if not fee:
        return None
    if fee.casefold() in {"no", "false", "0"}:
        return "free"
    if fee.casefold() in {"yes", "true", "1"}:
        return "fee required; rate not specified"
    return fee


def add_new_attributes(matches: pd.DataFrame, osm: pd.DataFrame) -> pd.DataFrame:
    """Read only the three fields that will be appended to the final data."""
    result = matches.copy()
    for column in NEW_COLUMNS:
        result[column] = None

    for index, match in result[result["match_status"].eq("matched")].iterrows():
        osm_row = osm.iloc[int(match["osm_row_index"])]
        result.at[index, "plug_types"] = read_plug_types(osm_row)
        result.at[index, "charging_bays"] = positive_integer(osm_row.get("capacity"))
        result.at[index, "pricing_information"] = read_pricing(osm_row)

    result["charging_bays"] = pd.array(result["charging_bays"], dtype="Int64")
    result["attribute_count"] = result[NEW_COLUMNS].notna().sum(axis=1).astype("Int64")
    result["augmented"] = result["attribute_count"].gt(0)
    return result


def build_enhanced_file(dc: pd.DataFrame, station_results: pd.DataFrame) -> pd.DataFrame:
    """Keep Member B's spatially joined DC columns and append three OSM fields."""
    lookup = station_results.set_index("station_id")[NEW_COLUMNS]
    enhanced = dc.copy()
    for column in NEW_COLUMNS:
        enhanced[column] = enhanced["station_id"].map(lookup[column])
    enhanced["charging_bays"] = pd.array(enhanced["charging_bays"], dtype="Int64")
    return enhanced[list(dc.columns) + NEW_COLUMNS]


def build_review(station_results: pd.DataFrame) -> pd.DataFrame:
    """Create a separate table for checking how every location was matched."""
    review = station_results.copy()
    matched = review[review["match_status"].eq("matched")]
    object_counts = matched.groupby(["osm_element_type", "osm_element_id"], dropna=True).size()
    count_lookup = object_counts.to_dict()
    review["shared_osm_match_count"] = review.apply(
        lambda row: count_lookup.get((row.get("osm_element_type"), row.get("osm_element_id")), 0), axis=1
    )
    shared = review["shared_osm_match_count"].gt(1)
    review.loc[shared, "review_reason"] = (
        review.loc[shared, "review_reason"].astype(str) + "; OSM object shared by multiple station_id values"
    )
    columns = [
        "station_id", "source_station_name", "source_operator", "source_address",
        "source_postcode", "source_lga_name", "source_latitude", "source_longitude",
        "candidate_count_100m", "match_status", "match_method", "match_confidence",
        "match_distance_m", "operator_match", "osm_element_type", "osm_element_id",
        "osm_name", "osm_operator", "osm_brand", "osm_latitude", "osm_longitude",
        "attribute_count", "augmented", "shared_osm_match_count", "review_reason",
    ]
    for column in columns:
        if column not in review:
            review[column] = None
    return review[columns]


def build_coverage(
    dc: pd.DataFrame,
    osm: pd.DataFrame,
    station_results: pd.DataFrame,
    enhanced: pd.DataFrame,
    metadata: dict[str, Any],
) -> pd.DataFrame:
    location_count = len(station_results)
    record_count = len(dc)
    matched = station_results["match_status"].eq("matched")
    augmented = station_results["augmented"]
    target_locations = math.ceil(location_count * TARGET_PERCENT / 100)
    target_records = math.ceil(record_count * TARGET_PERCENT / 100)
    augmented_records = int(enhanced[NEW_COLUMNS].notna().any(axis=1).sum())

    objects = station_results.loc[
        augmented & station_results["osm_element_id"].notna(),
        ["osm_element_type", "osm_element_id"],
    ].drop_duplicates()
    distinct_objects = len(objects)
    matched_object_counts = station_results.loc[
        matched & station_results["osm_element_id"].notna(),
        ["osm_element_type", "osm_element_id"],
    ].value_counts()

    metrics = [
        ("input", "dc_charger_records", record_count, "Rows in dc_candidates.parquet"),
        ("input", "unique_dc_locations", location_count, "Coverage denominator: unique station_id"),
        ("target", "target_percent", TARGET_PERCENT, "Assignment target for DC fast charging locations"),
        ("target", "target_location_count", target_locations, "Ceiling of 50% of unique DC locations"),
        ("target", "target_record_count", target_records, "50% of DC charger rows, rounded up"),
        ("external", "osm_charging_station_elements", len(osm), "OSM objects in the query bounding box"),
        ("external", "osm_data_timestamp_utc", metadata.get("osm_data_timestamp_utc"), "Timestamp returned by Overpass"),
        ("matching", "matched_locations", int(matched.sum()), "Accepted OSM matches"),
        ("matching", "matched_location_percent", round(matched.mean() * 100, 2), "Matched unique DC locations"),
        ("matching", "unmatched_locations", int((~matched).sum()), "Kept with empty new fields"),
        ("augmentation", "augmented_locations", int(augmented.sum()), "At least one of the three new fields"),
        ("augmentation", "augmented_location_percent", round(augmented.mean() * 100, 2), "Primary coverage measure"),
        ("augmentation", "location_target_met", bool(augmented.sum() >= target_locations), "Must be true"),
        ("augmentation", "augmented_charger_records", augmented_records, "Rows with at least one new field"),
        ("augmentation", "augmented_record_percent", round(augmented_records / record_count * 100, 2), "Secondary coverage measure"),
        ("augmentation", "record_target_met", bool(augmented_records >= target_records), "Secondary check"),
        ("augmentation", "distinct_osm_objects_for_augmented_locations", distinct_objects, "Shared OSM objects counted once"),
        ("augmentation", "conservative_distinct_object_percent", round(distinct_objects / location_count * 100, 2), "Robustness check"),
        ("augmentation", "conservative_distinct_object_target_met", bool(distinct_objects >= target_locations), "Must still meet 50%"),
        ("matching", "shared_osm_objects", int(matched_object_counts.gt(1).sum()), "OSM objects used by multiple station_id values"),
        ("matching", "locations_using_shared_osm_objects", int(matched_object_counts[matched_object_counts.gt(1)].sum()), "Locations flagged in matching_review.csv"),
    ]
    for column in NEW_COLUMNS:
        metrics.append(("attribute", f"locations_with_{column}", int(station_results[column].notna().sum()), f"Non-null {column}"))
    for confidence in ("high", "medium", "low"):
        count = int(station_results["match_confidence"].eq(confidence).sum())
        metrics.append(("matching", f"{confidence}_confidence_matches", count, ""))
    return pd.DataFrame(metrics, columns=["section", "metric", "value", "note"])


def validate_outputs(
    dc: pd.DataFrame,
    enhanced: pd.DataFrame,
    station_results: pd.DataFrame,
    coverage: pd.DataFrame,
) -> None:
    expected_columns = list(dc.columns) + NEW_COLUMNS
    if list(enhanced.columns) != expected_columns or len(enhanced) != len(dc):
        raise ValueError("Enhanced output must contain the original rows/columns plus three new columns")
    pd.testing.assert_frame_equal(
        enhanced[list(dc.columns)].reset_index(drop=True),
        dc.reset_index(drop=True),
        check_dtype=True,
    )
    if "charger_id" in enhanced and not enhanced["charger_id"].is_unique:
        raise ValueError("charger_id must remain unique")
    if set(station_results["station_id"]) != set(dc["station_id"]):
        raise ValueError("Not every station_id was retained during matching")
    accepted = station_results["match_status"].eq("matched")
    if station_results.loc[accepted, "match_distance_m"].gt(MAX_MATCH_DISTANCE_M).any():
        raise ValueError("An accepted match is farther than 100 m")
    metrics = coverage.set_index("metric")["value"]
    if str(metrics["location_target_met"]).casefold() != "true":
        raise ValueError("The 50% location augmentation target was not met")


def run(project_root: Path, refresh: bool = False) -> dict[str, Any]:
    root = project_root.resolve()
    input_path = root / INPUT_DC
    if not input_path.exists() or input_path.stat().st_size == 0:
        raise FileNotFoundError(f"Missing Member B input: {input_path}")

    # 1. Read Member B's DC data and the saved/downloaded OSM response.
    dc = pd.read_parquet(input_path)
    payload, metadata = load_osm(root, refresh=refresh)
    osm = flatten_osm(payload)
    osm.to_csv(root / RAW_CSV, index=False, encoding="utf-8")

    # 2. Match each location once, then copy the three new fields to every charger row.
    stations = make_station_table(dc)
    matches = choose_matches(stations, osm)
    station_results = add_new_attributes(matches, osm)
    enhanced = build_enhanced_file(dc, station_results)

    # 3. Keep matching evidence separate from the simple 17-column final table.
    review = build_review(station_results)
    coverage = build_coverage(dc, osm, station_results, enhanced, metadata)
    validate_outputs(dc, enhanced, station_results, coverage)

    for path in (OUT_ENRICHMENT, OUT_COVERAGE, OUT_REVIEW):
        (root / path).parent.mkdir(parents=True, exist_ok=True)
    enhanced.to_parquet(root / OUT_ENRICHMENT, index=False)
    coverage.to_csv(root / OUT_COVERAGE, index=False, encoding="utf-8")
    review.to_csv(root / OUT_REVIEW, index=False, encoding="utf-8")

    metrics = coverage.set_index("metric")["value"]
    summary = {
        "dc_charger_records": len(dc),
        "unique_dc_locations": len(stations),
        "osm_elements": len(osm),
        "matched_locations": int(station_results["match_status"].eq("matched").sum()),
        "augmented_locations": int(station_results["augmented"].sum()),
        "augmented_location_percent": float(metrics["augmented_location_percent"]),
        "output_columns": len(enhanced.columns),
        "cache_used": bool(metadata.get("cache_used")),
    }
    LOG.info("Augmentation summary: %s", json.dumps(summary, sort_keys=True))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Add OSM fields to NSW DC charger data")
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--refresh", action="store_true", help="Download a new OSM response")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run(args.project_root, refresh=args.refresh)
        return 0
    except Exception as exc:
        LOG.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
