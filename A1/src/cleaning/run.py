#!/usr/bin/env python3
"""Stage ``cleaning`` (member B): raw TfNSW EV-charger CSV -> trusted core data.

Contract with ``run_pipeline.py`` (see ``config/pipeline.json``):

    python src/cleaning/run.py --project-root <root>

    input   data/raw/nsw/ev_20251216.csv                       (written by member A)
            data/raw/abs/.../SA4_2026_AUST_GDA2020.shp        (written by member A)
    output  data/processed/chargers_clean.parquet / .csv       (15 columns incl. SA4)
            data/processed/dc_candidates.parquet / .csv        (14-column DC subset)
            data/quality/cleaning_summary.csv                  (before/after statistics)
            data/quality/excluded_records.csv                  (every raw row that was NOT kept + reason)
            data/quality/duplicate_review.csv                  (every candidate duplicate pair + decision)
            data/quality/chargers_clean_audit.csv              (per-record trace)

Each Parquet output has a CSV copy with the same rows, values, columns and order.
Column names use lower-case ``snake_case`` throughout the processed datasets.

Every cleaning decision is a named rule below so that the report (and the tutor) can
trace it.  The raw file is never modified.  Re-running gives byte-identical IDs.

Design notes (see docs/data_dictionary.md for the field-level view):

* Granularity.  One raw row = one *charger record*: a group of ``Number_of_plugs`` plugs of one
  operator / current type / power rating at one coordinate.  We keep that grain and call the key
  ``charger_id``.  Several charger records at (almost) the same coordinate form a *station*
  (``station_id``), because the raw file has no station identifier.
* The raw file is a union of several TfNSW programmes (``Source`` column) plus a block of 121
  rows with an empty ``Source`` (file lines 985-1105).  The same physical charger is therefore
  listed more than once in different programmes with slightly different spelling, coordinates
  (0-30 m) and rating text.  Rule D1-D3 collapse those cross-programme duplicates.
* ``Upcoming`` rows are planned, not installed, chargers -> excluded (rule R1).

Rule index (the report and docs/data_dictionary.md use the same names)
    R1 drop ``Upcoming`` rows                     R2 drop rows with invalid coordinates
    T1 trim / one-line text                       T2 one-line address, 'Australia' removed
    T3 operator mapping + truncation repair       T4 rating text -> rating_kw / rating_text
    T5 plug count -> integer                      T6 postcode (address beats the PCODE column)
    T7 station name (blank or operator-only -> missing; no name is invented)
    D1 cross-programme records <= 30 m, compatible operator / rating   -> same charger, merge
    D2 same normalised street address <= 100 m, compatible             -> same charger, merge
    D3 never let a chain merge two different known AC ratings; DC re-rating with equal plugs is allowed
    I1 charger_id = sha1(coords, operator, type, kW)                   I2 station_id = records within 30 m
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import math
import re
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import geopandas as gpd

LOG = logging.getLogger("cleaning")

RAW_NSW_CSV = "data/raw/nsw/ev_20251216.csv"
RAW_SA4_SHP = "data/raw/abs/SA4_2026_AUST_SHP_GDA2020/SA4_2026_AUST_GDA2020.shp"
OUT_CLEAN = "data/processed/chargers_clean.parquet"
OUT_CLEAN_CSV = "data/processed/chargers_clean.csv"
OUT_DC = "data/processed/dc_candidates.parquet"
OUT_DC_CSV = "data/processed/dc_candidates.csv"
OUT_SUMMARY = "data/quality/cleaning_summary.csv"
OUT_EXCLUDED = "data/quality/excluded_records.csv"
OUT_REVIEW = "data/quality/duplicate_review.csv"
OUT_AUDIT = "data/quality/chargers_clean_audit.csv"

EXCLUDED_COLUMNS = ["source_row", "source_program", "operator_raw", "address", "latitude", "longitude",
                    "raw_type", "raw_rating", "raw_plugs", "reason", "detail"]

ID_COLUMNS = ["charger_id", "station_id"]

# Output order follows the source layout, with consistent processed-data names.
BASE_CLEAN_COLUMNS = [
    "charger_id", "station_id", "source_objectid", "station_name", "address",
    "operator_name", "number_of_plugs", "charger_type", "rating_text", "latitude",
    "longitude", "lga_name", "postcode", "source_program",
]
CLEAN_COLUMNS = BASE_CLEAN_COLUMNS + ["sa4_region"]

# Member C receives the same common fields in nearly the same order.
BASE_DC_COLUMNS = [
    "charger_id", "station_id", "station_name", "address", "operator_name",
    "number_of_plugs", "rating_text", "rating_kw", "latitude", "longitude",
    "lga_name", "postcode", "source_program",
]
DC_COLUMNS = BASE_DC_COLUMNS + ["sa4_region"]

RAW_COLUMNS = [
    "OBJECTID", "Station_name", "Station_address", "Operator", "Number_of_plugs",
    "Charger_Type", "Charger_rating", "Latitude", "Longitude", "LGANAME", "PCODE", "Source",
]

GENERIC_OPERATORS = {"Non-networked"}          # placeholder used when the operator is unknown

# lower-case raw value -> canonical operator name.  Anything not listed is only trimmed.
OPERATOR_MAP = {
    "tesla motors": "Tesla",
    "bp australia": "BP",
    "evie networks": "Evie",
    "nrma electric": "NRMA",
    "non-networked": "Non-networked",
    "non networked": "Non-networked",
    "plus es manag": "PLUS ES",                  # truncated at 13 characters in the source
    "charge hub": "ChargeHub",
    "viva energy a": "Viva Energy Australia",    # truncated at 13 characters
    "energy austra": "Energy Australia",         # truncated at 13 characters
    "fast cities a": "Fast Cities Australia",    # truncated at 13 characters (assumption, see dictionary)
    "porsche destination charging": "Porsche",
    "porsche smart mobility": "Porsche",
}
TRUNCATED_OPERATORS = {"plus es manag", "viva energy a", "energy austra", "fast cities a", "university of"}


@dataclass(frozen=True)
class Config:
    nsw_bbox: tuple = (-37.6, -28.0, 140.9, 153.7)   # lat_min, lat_max, lon_min, lon_max (loose NSW box)
    dup_distance_m: float = 30.0       # cross-programme records closer than this may be the same charger
    same_spot_m: float = 2.0           # "same coordinate": operator may differ (re-branding)
    address_match_m: float = 100.0     # ...or closer than this when the normalised street address is identical
    review_distance_m: float = 100.0   # candidate pairs up to this distance are written to the review file
    station_radius_m: float = 30.0     # charger records within this radius belong to one station
    plugs_outlier: int = 25            # >= this many plugs at one record is flagged (not changed)
    ac_dc_split_kw: float = 24.0       # observed: AC <= 23 kW, DC >= 24 kW
    sensitivity_m: tuple = (5.0, 10.0, 30.0, 50.0, 100.0)


# --------------------------------------------------------------------------- text helpers
def present(value) -> bool:
    """True when value is a real value (not None / NaN / pd.NA).  pandas 2 and 3 disagree on None vs NaN."""
    return value is not None and not pd.isna(value)


def clean_text(value) -> Optional[str]:
    """Trim, turn NBSP / CR / LF into spaces, collapse runs of whitespace; blank -> None."""
    if value is None:
        return None
    text = str(value).replace("\u00a0", " ").replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def normalise_address(value) -> Optional[str]:
    """One-line address: newlines -> ', ', drop trailing 'Australia', 'New South Wales' -> 'NSW'."""
    if value is None:
        return None
    text = str(value).replace("\u00a0", " ").replace("\r", "\n")
    text = re.sub(r"\s*\n\s*", ", ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\bNew South Wales\b", "NSW", text, flags=re.I)
    text = re.sub(r"[\s,]*\bAustralia\b\s*$", "", text, flags=re.I)
    text = re.sub(r"\s+,", ",", text)          # "Goulburn , 2580" -> "Goulburn, 2580"
    text = re.sub(r",\s*,+", ",", text)
    text = re.sub(r"^[,\s]+", "", text)        # leading comma when the street is missing
    text = re.sub(r"[,\s]+$", "", text)
    return text or None


_POSTCODE_END = re.compile(r"\b(\d{4})\b(?:[,\s]+(?:NSW|VIC|ACT|QLD))?\s*$", re.I)


def extract_postcode(address: Optional[str]) -> Optional[str]:
    """Postcode = the 4-digit token at the END of the address (a street number is never last)."""
    if not address:
        return None
    match = _POSTCODE_END.search(address)
    return match.group(1) if match else None


def normalise_operator(raw, address: Optional[str] = None) -> tuple[Optional[str], bool]:
    """Return (canonical operator, repaired_truncated_value)."""
    text = clean_text(raw)
    if text is None:
        return None, False
    key = text.lower()
    if key == "university of":                 # cut off by the 13-character field limit
        if address and "university of wollongong" in address.lower():
            return "University of Wollongong", True
        return "University of", False
    return OPERATOR_MAP.get(key, text), key in TRUNCATED_OPERATORS


def address_key(address: Optional[str]) -> str:
    """Loose comparison key: lower case, no punctuation, no 'nsw' token."""
    if not address:
        return ""
    text = re.sub(r"[^a-z0-9 ]", " ", address.lower())
    return " ".join(t for t in text.split() if t != "nsw")


_RATING_PART = re.compile(r"(?:(\d+)\s*x\s*)?(\d+(?:\.\d+)?)\s*kW", re.I)


def parse_rating(raw) -> Optional[dict]:
    """'22 kW' / '22' / '2x350kW & 2x175kW'  ->  max kW, normalised text, mixed flag.

    The literal 'AC' (522 rows) is a placeholder, not a rating -> None.
    """
    text = clean_text(raw)
    if text is None:
        return None
    parts = _RATING_PART.findall(text)
    if parts:
        counts = [int(n) if n else 1 for n, _ in parts]
        kws = [float(k) for _, k in parts]
        pieces = [f"{c}x{k:g} kW" if c > 1 else f"{k:g} kW" for c, k in zip(counts, kws)]
        explicit = any(n for n, _ in parts)     # "2x350kW" states a plug count, "22 kW" does not
        return {"kw_max": max(kws), "text": " & ".join(pieces), "mixed": len(set(kws)) > 1,
                "plugs_in_text": sum(counts) if explicit else None}
    if re.fullmatch(r"\d+(?:\.\d+)?", text):   # bare number, unit missing
        value = float(text)
        return {"kw_max": value, "text": f"{value:g} kW", "mixed": False, "plugs_in_text": None}
    return None


# --------------------------------------------------------------------------- geometry
def haversine_matrix(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    la, lo = np.radians(lat), np.radians(lon)
    dlat = la[:, None] - la[None, :]
    dlon = lo[:, None] - lo[None, :]
    a = np.sin(dlat / 2) ** 2 + np.cos(la)[:, None] * np.cos(la)[None, :] * np.sin(dlon / 2) ** 2
    return 2 * 6371008.8 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


class DSU:
    def __init__(self, n: int):
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


# --------------------------------------------------------------------------- step 1: parse rows
def parse_raw(raw: pd.DataFrame, cfg: Config, stats: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rules T1-T7 + R1-R2.  Returns (kept records, excluded rows)."""
    missing_cols = [c for c in RAW_COLUMNS if c not in raw.columns]
    if missing_cols:
        raise ValueError(f"raw file is missing columns: {missing_cols}")

    lat_min, lat_max, lon_min, lon_max = cfg.nsw_bbox
    kept, excluded = [], []
    for idx, row in enumerate(raw.to_dict("records")):
        source_row = idx + 2                       # line number in the CSV (header = line 1)
        flags: list[str] = []

        address = normalise_address(row["Station_address"])
        if address != clean_text(row["Station_address"]):
            stats["address_reformatted"] += 1
        if str(row["Station_address"]).lstrip().startswith(","):
            flags.append("address_no_street")
        if str(row["Station_address"]) != str(row["Station_address"]).strip() or "\n" in str(row["Station_address"]):
            stats["address_whitespace_or_newline"] += 1

        operator_raw = clean_text(row["Operator"])
        if str(row["Operator"]) != str(row["Operator"]).strip():
            stats["operator_trailing_space"] += 1
        operator, repaired = normalise_operator(row["Operator"], address)
        if operator != operator_raw:
            stats["operator_remapped_rows"] += 1
        if repaired:
            stats["operator_truncation_repaired_rows"] += 1
            flags.append("operator_truncation_repaired")

        # R2: coordinates
        try:
            lat, lon = float(row["Latitude"]), float(row["Longitude"])
        except (TypeError, ValueError):
            lat = lon = float("nan")
        coord_problem = None
        if math.isnan(lat) or math.isnan(lon):
            coord_problem = "coordinates_not_numeric"
        elif lat == 0 or lon == 0:
            coord_problem = "coordinates_zero"
        elif lat > 0 or lon < 0:
            coord_problem = "coordinates_swapped_or_wrong_hemisphere"
        elif not (lat_min <= lat <= lat_max and lon_min <= lon <= lon_max):
            coord_problem = "coordinates_outside_nsw_bbox"

        raw_type = (clean_text(row["Charger_Type"]) or "").upper()
        base = {"source_row": source_row, "objectid": clean_text(row["OBJECTID"]),
                "source_program": clean_text(row["Source"]),
                "operator_raw": operator_raw, "address": address, "latitude": lat, "longitude": lon,
                "raw_type": raw_type, "raw_rating": clean_text(row["Charger_rating"]),
                "raw_plugs": clean_text(row["Number_of_plugs"])}

        if raw_type == "UPCOMING":                 # R1
            excluded.append({**base, "reason": "upcoming_not_installed", "detail": ""})
            continue
        if raw_type not in ("AC", "DC"):
            excluded.append({**base, "reason": "unknown_charger_type", "detail": raw_type})
            continue
        if coord_problem:
            excluded.append({**base, "reason": coord_problem, "detail": ""})
            continue

        # T4: rating
        rating = parse_rating(row["Charger_rating"])
        if rating is None:
            stats["rating_placeholder_or_missing"] += 1
        else:
            if not re.search(r"kW", str(row["Charger_rating"]), re.I):
                stats["rating_unit_added"] += 1
            if rating["mixed"]:
                stats["rating_mixed_text_rows"] += 1

        # T5: plugs
        plugs = pd.to_numeric(clean_text(row["Number_of_plugs"]), errors="coerce")
        if pd.isna(plugs) or plugs <= 0:
            plugs = pd.NA
            flags.append("plugs_missing_or_invalid")
        else:
            plugs = int(plugs)

        # T6: postcode.  Address postcode wins over the source PCODE column when both exist and differ.
        pc_raw = clean_text(row["PCODE"])
        pc_fixed = None
        if pc_raw:
            m = re.fullmatch(r"(?:NSW\s*)?(\d{4})", pc_raw, flags=re.I)
            if m:
                pc_fixed = m.group(1)
                if pc_fixed != pc_raw:
                    stats["postcode_prefix_stripped"] += 1
            else:
                flags.append("postcode_unparseable")
        pc_addr = extract_postcode(address)
        if pc_addr and pc_fixed and pc_addr != pc_fixed:
            postcode = pc_addr
            stats["postcode_overridden_by_address"] += 1
            flags.append("postcode_overridden_by_address")
        elif pc_fixed:
            postcode = pc_fixed
        elif pc_addr:
            postcode = pc_addr
            stats["postcode_filled_from_address"] += 1
            flags.append("postcode_filled_from_address")
        else:
            postcode = None

        # T7: station name.  A name that only repeats the operator carries no information.
        name = clean_text(row["Station_name"])
        if str(row["Station_name"]) != str(row["Station_name"]).strip() and name:
            stats["station_name_trimmed"] += 1
        if name and operator and name.lower() in {operator.lower(), (operator_raw or "").lower()}:
            name = None
            stats["station_name_equals_operator"] += 1

        kept.append({
            **base,
            "station_name": name,
            "operator_name": operator,
            "postcode": postcode,
            "lga_name": clean_text(row["LGANAME"]),
            "charger_type": raw_type,
            "number_of_plugs": plugs,
            "rating_kw": rating["kw_max"] if rating else np.nan,
            "rating_text": rating["text"] if rating else None,
            "rating_plugs_in_text": rating["plugs_in_text"] if rating else None,
            "transform_flags": flags,
        })

    kept_df = pd.DataFrame(kept)
    excluded_df = pd.DataFrame(excluded, columns=EXCLUDED_COLUMNS)
    return kept_df, excluded_df


# --------------------------------------------------------------------------- step 2: duplicates
def evaluate_pairs(df: pd.DataFrame, dist: np.ndarray, cfg: Config) -> list[dict]:
    """Rules D1-D3.  Returns one dict per candidate pair (decision 'merged' / 'kept_separate')."""
    types = df["charger_type"].to_numpy()
    close = np.triu(dist <= cfg.review_distance_m, 1)
    pairs: list[dict] = []
    for i, j in zip(*np.where(close)):
        if types[i] != types[j]:
            continue
        a, b, d = df.iloc[i], df.iloc[j], float(dist[i, j])
        same_source = present(a.source_program) and a.source_program == b.source_program
        op_equal = a.operator_name == b.operator_name
        op_generic = a.operator_name in GENERIC_OPERATORS or b.operator_name in GENERIC_OPERATORS
        op_ok = op_equal or op_generic
        plugs_known = not (pd.isna(a.number_of_plugs) or pd.isna(b.number_of_plugs))
        plugs_equal = plugs_known and int(a.number_of_plugs) == int(b.number_of_plugs)
        rebrand = d <= cfg.same_spot_m and plugs_equal          # same spot, same plugs, other operator name
        kw_known = not (pd.isna(a.rating_kw) or pd.isna(b.rating_kw))
        kw_ok = (not kw_known) or a.rating_kw == b.rating_kw
        # same DC unit listed with an updated nameplate rating (40 vs 50 kW, 175 vs 2x350 kW ...)
        dc_upgrade = a.charger_type == "DC" and plugs_equal and (op_equal or d <= cfg.same_spot_m)

        same_address = address_key(a.address) != "" and address_key(a.address) == address_key(b.address)
        near = d <= cfg.dup_distance_m or (same_address and d <= cfg.address_match_m)   # D1 / D2

        decision, reason = None, None
        if same_source:
            if near and op_ok and kw_ok:
                decision, reason = "kept_separate", "same_programme_close_pair"
        elif near and (op_ok or rebrand) and (kw_ok or dc_upgrade):
            decision, reason = "merged", "cross_programme_duplicate"
        elif not near and op_ok and kw_ok:
            decision, reason = "kept_separate", "beyond_merge_distance"
        elif near and not (op_ok or rebrand) and kw_ok:
            decision, reason = "kept_separate", "operator_conflict"
        elif near and (op_ok or rebrand) and not (kw_ok or dc_upgrade):
            decision, reason = "kept_separate", "rating_conflict"
        if decision is None:
            continue
        conflicts = []
        if decision == "merged":
            if not op_equal and not op_generic:
                conflicts.append("operator")
            if kw_known and a.rating_kw != b.rating_kw:
                conflicts.append("rating")
            if plugs_known and not plugs_equal:
                conflicts.append("plugs")
        pairs.append({
            "i": int(i), "j": int(j), "decision": decision, "reason": reason,
            "distance_m": round(d, 1), "conflicts": conflicts, "plugs_equal": bool(plugs_equal),
            "row_a": int(a.source_row), "row_b": int(b.source_row),
            "programme_a": a.source_program, "programme_b": b.source_program,
            "operator_a": a.operator_name, "operator_b": b.operator_name,
            "type": a.charger_type,
            "rating_a": a.rating_text, "rating_b": b.rating_text,
            "plugs_a": a.number_of_plugs, "plugs_b": b.number_of_plugs,
            "address_a": a.address, "address_b": b.address,
        })
    return guard_transitive_conflicts(df, pairs)


def guard_transitive_conflicts(df: pd.DataFrame, pairs: list[dict]) -> list[dict]:
    """Rule D3: a chain of pairwise-compatible AC records must not end up merging two different known kW.

    (Row A 22 kW ~ row B 'AC' ~ row C 7 kW: B is compatible with both, A and C are not.)
    Edges with equal plug counts are applied first, then by distance.
    """
    merged = sorted((p for p in pairs if p["decision"] == "merged"),
                    key=lambda p: (not p["plugs_equal"], p["distance_m"], p["i"], p["j"]))
    dsu = DSU(len(df))
    kw_sets = {k: ({float(df.iloc[k].rating_kw)} if present(df.iloc[k].rating_kw) else set()) for k in range(len(df))}
    for p in merged:
        ra, rb = dsu.find(p["i"]), dsu.find(p["j"])
        if ra == rb:
            continue
        union_kw = kw_sets[ra] | kw_sets[rb]
        if p["type"] == "AC" and len(union_kw) > 1:
            p["decision"], p["reason"], p["conflicts"] = "kept_separate", "transitive_rating_conflict", []
            continue
        dsu.union(ra, rb)
        kw_sets[dsu.find(ra)] = union_kw
    return pairs


def collapse_duplicates(df: pd.DataFrame, pairs: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Union merged pairs into groups; keep the best record, fill its gaps from the others."""
    dsu = DSU(len(df))
    conflicts_by_pair: dict[int, set] = {}
    for p in pairs:
        if p["decision"] == "merged":
            dsu.union(p["i"], p["j"])
    groups: dict[int, list[int]] = {}
    for i in range(len(df)):
        groups.setdefault(dsu.find(i), []).append(i)
    for p in pairs:
        if p["decision"] == "merged":
            conflicts_by_pair.setdefault(dsu.find(p["i"]), set()).update(p["conflicts"])

    def score(k: int):
        r = df.iloc[k]
        return (present(r.source_program), r.operator_name not in GENERIC_OPERATORS,
                present(r.station_name), present(r.rating_kw), -int(r.source_row))

    out_rows, dropped = [], []
    for root, members in groups.items():
        members = sorted(members, key=lambda k: int(df.iloc[k].source_row))
        best = max(members, key=score)
        rec = df.iloc[best].to_dict()
        rec["merged_record_count"] = len(members)
        rec["source_rows"] = ";".join(str(int(df.iloc[k].source_row)) for k in members)
        rec["operator_raw"] = "|".join(sorted({df.iloc[k].operator_raw for k in members if present(df.iloc[k].operator_raw)}))
        names = {str(df.iloc[k].station_name).lower() for k in members if present(df.iloc[k].station_name)}
        rec["dup_conflicts"] = sorted(conflicts_by_pair.get(root, set()) | ({"name"} if len(names) > 1 else set()))
        for col in ("station_name", "rating_kw", "rating_text", "postcode", "lga_name", "number_of_plugs", "objectid"):
            if not present(rec[col]):
                for k in members:
                    val = df.iloc[k][col]
                    if present(val):
                        rec[col] = val
                        break
        out_rows.append(rec)
        for k in members:
            if k != best:
                r = df.iloc[k]
                dropped.append({
                    "source_row": int(r.source_row), "source_program": r.source_program,
                    "operator_raw": r.operator_raw, "address": r.address, "latitude": r.latitude,
                    "longitude": r.longitude, "raw_type": r.raw_type, "raw_rating": r.raw_rating,
                    "raw_plugs": r.raw_plugs, "reason": "duplicate_merged",
                    "detail": f"kept_source_row={int(df.iloc[best].source_row)}",
                })
    out = pd.DataFrame(out_rows).sort_values("source_row").reset_index(drop=True)
    return out, pd.DataFrame(dropped, columns=EXCLUDED_COLUMNS)


def merged_removed(df_len: int, pairs: list[dict]) -> int:
    dsu = DSU(df_len)
    for p in pairs:
        if p["decision"] == "merged":
            dsu.union(p["i"], p["j"])
    return df_len - len({dsu.find(i) for i in range(df_len)})


# --------------------------------------------------------------------------- step 3: ids
def make_charger_ids(df: pd.DataFrame) -> list[str]:
    """Deterministic ID: hash of coordinates (6 dp), operator, current type and power."""
    seen: dict[str, int] = {}
    ids = []
    for r in df.itertuples():
        kw = "" if pd.isna(r.rating_kw) else f"{r.rating_kw:g}"
        key = f"{r.latitude:.6f}|{r.longitude:.6f}|{(r.operator_name or '').lower()}|{r.charger_type}|{kw}"
        base = "EV" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:10].upper()
        seen[base] = seen.get(base, 0) + 1
        ids.append(base if seen[base] == 1 else f"{base}-{seen[base]}")
    return ids


def make_station_ids(df: pd.DataFrame, radius_m: float) -> tuple[list[str], float]:
    """Charger records within ``radius_m`` (single linkage) share a station; also return max diameter."""
    dist = haversine_matrix(df["latitude"].to_numpy(), df["longitude"].to_numpy())
    dsu = DSU(len(df))
    for i, j in zip(*np.where(np.triu(dist <= radius_m, 1))):
        dsu.union(int(i), int(j))
    groups: dict[int, list[int]] = {}
    for i in range(len(df)):
        groups.setdefault(dsu.find(i), []).append(i)
    station_of = {}
    max_diameter = 0.0
    for members in groups.values():
        first = min(df.iloc[k].charger_id for k in members)
        sid = "ST" + first[2:12]
        for k in members:
            station_of[k] = sid
        if len(members) > 1:
            sub = dist[np.ix_(members, members)]
            max_diameter = max(max_diameter, float(sub.max()))
    return [station_of[i] for i in range(len(df))], max_diameter


# --------------------------------------------------------------------------- orchestration
@dataclass
class CleaningResult:
    clean: pd.DataFrame             # internal normalised table used for summaries and DC candidates
    dc_candidates: pd.DataFrame
    excluded: pd.DataFrame
    review: pd.DataFrame
    summary: pd.DataFrame
    clean_export: pd.DataFrame      # public 14-column cleaned dataset
    audit: pd.DataFrame             # per-record trace for clean_export


def load_raw(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")


def run_cleaning(raw: pd.DataFrame, cfg: Config = Config()) -> CleaningResult:
    rows: list[dict] = []

    def add(section: str, metric: str, value, note: str = "") -> None:
        rows.append({"section": section, "metric": metric, "value": value, "note": note})

    stats: dict[str, int] = {k: 0 for k in (
        "address_reformatted", "address_whitespace_or_newline", "operator_trailing_space",
        "operator_remapped_rows", "operator_truncation_repaired_rows", "rating_placeholder_or_missing",
        "rating_unit_added", "rating_mixed_text_rows", "postcode_prefix_stripped",
        "postcode_overridden_by_address", "postcode_filled_from_address", "station_name_trimmed",
        "station_name_equals_operator")}

    # ---- 0. raw profile
    n_raw = len(raw)
    add("raw", "rows", n_raw, "data rows in ev_20251216.csv")
    add("raw", "columns", raw.shape[1])
    for label, count in raw["Charger_Type"].str.strip().value_counts().items():
        add("raw", f"rows_type_{label}", int(count))
    for col in RAW_COLUMNS:
        blank = int((raw[col].str.strip() == "").sum())
        add("raw", f"missing_{col}", blank, f"{blank / n_raw:.1%} blank")
    add("raw", "distinct_operator_spellings", int(raw["Operator"].nunique()))

    # ---- 1. parse / normalise / exclude
    kept, excluded_parse = parse_raw(raw, cfg, stats)
    if kept.empty:
        raise ValueError("no usable records left after parsing")
    n_upcoming = int((excluded_parse["reason"] == "upcoming_not_installed").sum())
    n_bad_coord = int(excluded_parse["reason"].astype(str).str.startswith("coordinates").sum())
    add("exclusion", "upcoming_not_installed", n_upcoming, "rule R1: planned chargers are not part of the network yet")
    add("exclusion", "invalid_coordinates", n_bad_coord,
        f"rule R2: null / zero / swapped / outside NSW box {cfg.nsw_bbox}")
    for k, v in stats.items():
        add("transform", k, v, "counted over the raw rows examined (incl. upcoming rows)")
    add("transform", "distinct_operator_after_mapping", int(kept["operator_name"].nunique()),
        "rule T3 operator mapping (see OPERATOR_MAP)")

    # ---- 2. duplicates
    dist = haversine_matrix(kept["latitude"].to_numpy(), kept["longitude"].to_numpy())
    pairs = evaluate_pairs(kept, dist, cfg)
    add("duplicates", "candidate_pairs_within_review_distance", len(pairs))
    add("duplicates", "pairs_merged", sum(p["decision"] == "merged" for p in pairs))
    add("duplicates", "pairs_kept_separate_for_review", sum(p["decision"] == "kept_separate" for p in pairs))
    for t in cfg.sensitivity_m:
        p_t = evaluate_pairs(kept, dist, replace(cfg, dup_distance_m=t, address_match_m=t,
                                                 review_distance_m=max(t, cfg.review_distance_m)))
        add("duplicates", f"sensitivity_records_removed_at_{t:g}m", merged_removed(len(kept), p_t),
            "how many records rule D1-D3 would remove for this distance threshold")
    clean, dropped = collapse_duplicates(kept, pairs)
    add("duplicates", "records_removed_as_duplicates", len(dropped), f"rule D1-D3 at {cfg.dup_distance_m:g} m")
    add("duplicates", "groups_with_more_than_one_record", int((clean["merged_record_count"] > 1).sum()))
    add("duplicates", "largest_group_size", int(clean["merged_record_count"].max()))

    # ---- 3. fields that depend on the final record
    def final_flags(r) -> str:
        f = list(r.transform_flags)
        if pd.isna(r.rating_kw):
            f.append("rating_missing")
        if not pd.isna(r.rating_kw):
            if r.charger_type == "AC" and r.rating_kw >= cfg.ac_dc_split_kw:
                f.append("type_rating_mismatch")
            if r.charger_type == "DC" and r.rating_kw < cfg.ac_dc_split_kw:
                f.append("type_rating_mismatch")
        if not pd.isna(r.number_of_plugs) and int(r.number_of_plugs) >= cfg.plugs_outlier:
            f.append("plugs_very_high_check")
        if present(r.rating_plugs_in_text) and present(r.number_of_plugs) and int(r.rating_plugs_in_text) != int(r.number_of_plugs):
            f.append("rating_text_plugs_differ")
        if not present(r.postcode):
            f.append("postcode_missing")
        elif not str(r.postcode).startswith("2"):
            f.append("postcode_not_2xxx")
        for c in r.dup_conflicts:
            f.append(f"merge_conflict_{c}")
        if not present(r.source_program):
            f.append("source_program_missing")
        if not present(r.station_name):
            f.append("station_name_missing")
        if r.merged_record_count > 1:
            f.append("merged_duplicates")
        return ";".join(dict.fromkeys(f))

    clean["quality_flags"] = clean.apply(final_flags, axis=1)
    clean["charger_id"] = make_charger_ids(clean)
    clean["station_id"], diameter = make_station_ids(clean, cfg.station_radius_m)
    clean["number_of_plugs"] = pd.array(clean["number_of_plugs"], dtype="Int64")
    clean["rating_kw"] = clean["rating_kw"].astype("float64")

    # Keep the source field order while using one naming convention.
    order = clean.sort_values("source_row").index
    clean_export = pd.DataFrame({
        "charger_id": clean["charger_id"], "station_id": clean["station_id"],
        "source_objectid": clean["objectid"], "station_name": clean["station_name"],
        "address": clean["address"], "operator_name": clean["operator_name"],
        "number_of_plugs": clean["number_of_plugs"], "charger_type": clean["charger_type"],
        "rating_text": clean["rating_text"], "latitude": clean["latitude"],
        "longitude": clean["longitude"], "lga_name": clean["lga_name"], "postcode": clean["postcode"],
        "source_program": clean["source_program"]}).loc[order].reset_index(drop=True)
    audit = clean.loc[order, ["charger_id", "station_id", "source_rows", "merged_record_count", "operator_raw",
                              "station_name", "rating_kw", "quality_flags"]].reset_index(drop=True)

    columns = ["charger_id", "station_id", "station_name", "operator_name", "address", "postcode",
               "lga_name", "longitude", "latitude", "charger_type", "number_of_plugs", "rating_kw",
               "rating_text", "source_program", "operator_raw",
               "source_rows", "merged_record_count", "quality_flags"]
    clean = clean[columns].sort_values(["charger_type", "station_id", "charger_id"]).reset_index(drop=True)

    # ---- 4. self-checks (raise -> pipeline stops instead of shipping bad data)
    excluded_parts = [part for part in (excluded_parse, dropped) if not part.empty]
    excluded = (pd.concat(excluded_parts, ignore_index=True)
                if excluded_parts else pd.DataFrame(columns=EXCLUDED_COLUMNS))
    excluded = excluded.sort_values("source_row").reset_index(drop=True)
    problems = []
    if clean["charger_id"].duplicated().any():
        problems.append("charger_id not unique")
    for col in ("charger_id", "station_id", "operator_name", "address", "latitude", "longitude", "charger_type"):
        if clean[col].isna().any():
            problems.append(f"null values in required column {col}")
    if list(clean_export.columns) != BASE_CLEAN_COLUMNS or len(clean_export) != len(clean):
        problems.append("clean export has the wrong columns or row count")
    if not set(clean["charger_type"]) <= {"AC", "DC"}:
        problems.append("charger_type outside AC/DC")
    if len(clean) + len(excluded) != n_raw:
        problems.append(f"row reconciliation failed: {len(clean)} kept + {len(excluded)} excluded != {n_raw} raw")
    if problems:
        raise AssertionError("; ".join(problems))
    add("reconciliation", "raw_rows", n_raw)
    add("reconciliation", "excluded_rows", len(excluded), "upcoming + invalid + duplicates")
    add("reconciliation", "clean_rows", len(clean), "raw = excluded + clean (checked)")

    # ---- 5. after profile
    add("clean", "rows", len(clean))
    add("clean", "rows_AC", int((clean.charger_type == "AC").sum()))
    add("clean", "rows_DC", int((clean.charger_type == "DC").sum()))
    add("clean", "distinct_stations", int(clean.station_id.nunique()), f"charger records within {cfg.station_radius_m:g} m")
    add("clean", "stations_with_DC", int(clean.loc[clean.charger_type == "DC", "station_id"].nunique()))
    add("clean", "largest_station_diameter_m", round(diameter, 1), "check for chaining; should be small")
    add("clean", "plugs_total", int(clean.number_of_plugs.sum()))
    add("clean", "station_name_missing", int(clean.station_name.isna().sum()),
        "blank or operator-only names remain missing; no label is invented")
    add("clean", "rating_kw_missing", int(clean.rating_kw.isna().sum()), "rating was the placeholder 'AC' and no duplicate had a value")
    add("clean", "postcode_missing", int(clean.postcode.isna().sum()))
    add("clean", "lga_name_missing", int(clean.lga_name.isna().sum()), "LGA is only filled for the main programmes")
    add("clean", "rows_with_quality_flags", int((clean.quality_flags != "").sum()))
    flag_counts = clean["quality_flags"].str.split(";").explode()
    for flag, count in flag_counts[flag_counts != ""].value_counts().items():
        add("flags", str(flag), int(count))

    dc = clean[clean.charger_type == "DC"]
    dc_candidates = dc[BASE_DC_COLUMNS].reset_index(drop=True)
    add("dc_candidates", "dc_charger_records", len(dc_candidates), "denominator if coverage is counted per charger record")
    add("dc_candidates", "dc_stations", int(dc.station_id.nunique()), "denominator if coverage is counted per station (recommended)")
    add("dc_candidates", "min_records_for_50pct", math.ceil(0.5 * len(dc_candidates)))
    add("dc_candidates", "min_stations_for_50pct", math.ceil(0.5 * dc.station_id.nunique()))
    for op, count in dc.operator_name.value_counts().head(8).items():
        add("dc_candidates", f"operator_{op}", int(count), "DC records per operator (top 8)")

    review = pd.DataFrame([{k: v for k, v in p.items() if k not in ("i", "j")} | {"conflicts": ",".join(p["conflicts"])}
                           for p in pairs]).sort_values(["decision", "reason", "distance_m"]).reset_index(drop=True) \
        if pairs else pd.DataFrame()
    summary = pd.DataFrame(rows)
    summary["value"] = summary["value"].map(lambda v: f"{v:g}" if isinstance(v, float) else str(v))
    return CleaningResult(clean, dc_candidates, excluded, review, summary, clean_export, audit)


# --------------------------------------------------------------------------- Spatial join: add ABS SA4 region
def add_sa4_region(result: CleaningResult, shapefile_path: Path) -> CleaningResult:
    """Add the ABS SA4 region name to the full cleaned table and its DC subset."""
    if not shapefile_path.exists():
        raise FileNotFoundError(f"SA4 shapefile not found: {shapefile_path}")

    boundaries = gpd.read_file(shapefile_path)
    required = {"SA4_NAME26", "STE_CODE26", "geometry"}
    missing = sorted(required - set(boundaries.columns))
    if missing:
        raise ValueError(f"SA4 shapefile is missing columns: {', '.join(missing)}")
    if boundaries.crs is None:
        raise ValueError("SA4 shapefile has no coordinate reference system")

    # Only NSW SA4 polygons are needed. Charger coordinates start in WGS84.
    nsw = boundaries.loc[
        boundaries["STE_CODE26"].astype("string").eq("1"),
        ["SA4_NAME26", "geometry"],
    ].copy()
    if nsw.empty:
        raise ValueError("SA4 shapefile contains no NSW regions")

    points = gpd.GeoDataFrame(
        result.clean_export.copy(),
        geometry=gpd.points_from_xy(result.clean_export["longitude"], result.clean_export["latitude"]),
        crs="EPSG:4326",
    ).to_crs(nsw.crs)
    points["_row_order"] = np.arange(len(points))

    joined = gpd.sjoin(points, nsw, how="left", predicate="within")
    joined = joined.sort_values("_row_order", kind="stable").reset_index(drop=True)
    if len(joined) != len(result.clean_export) or joined["charger_id"].duplicated().any():
        raise ValueError("Spatial join changed the number or uniqueness of charger records")

    # A coastal coordinate can sit just outside the ABS land polygon because of
    # rounding. For unmatched points only, accept the nearest NSW SA4 within 100 m.
    unmatched_rows = joined.index[joined["SA4_NAME26"].isna()]
    nearest_fallback_count = 0
    if len(unmatched_rows):
        metric_crs = "EPSG:3577"  # Australian Albers, suitable for metre distances.
        unmatched_points = points.loc[unmatched_rows, ["_row_order", "geometry"]].to_crs(metric_crs)
        nearest = gpd.sjoin_nearest(
            unmatched_points,
            nsw.to_crs(metric_crs),
            how="left",
            max_distance=100,
            distance_col="distance_m",
        )
        nearest = nearest.sort_values("distance_m", kind="stable").drop_duplicates("_row_order")
        fallback = nearest.dropna(subset=["SA4_NAME26"]).set_index("_row_order")["SA4_NAME26"]
        missing_mask = joined["SA4_NAME26"].isna()
        joined.loc[missing_mask, "SA4_NAME26"] = joined.loc[missing_mask, "_row_order"].map(fallback)
        nearest_fallback_count = int(joined.loc[unmatched_rows, "SA4_NAME26"].notna().sum())

    clean_export = result.clean_export.copy()
    clean_export["sa4_region"] = pd.array(joined["SA4_NAME26"], dtype="string")
    clean_export = clean_export[CLEAN_COLUMNS]

    region_by_charger = clean_export.set_index("charger_id")["sa4_region"]
    dc_candidates = result.dc_candidates.copy()
    dc_candidates["sa4_region"] = dc_candidates["charger_id"].map(region_by_charger).astype("string")
    dc_candidates = dc_candidates[DC_COLUMNS]

    matched = int(clean_export["sa4_region"].notna().sum())
    spatial_summary = pd.DataFrame(
        [
            {"section": "spatial_join", "metric": "input_chargers", "value": str(len(clean_export)), "note": "cleaned charger records"},
            {"section": "spatial_join", "metric": "matched_chargers", "value": str(matched), "note": "point-in-polygon match or documented coastal fallback"},
            {"section": "spatial_join", "metric": "unmatched_chargers", "value": str(len(clean_export) - matched), "note": "retained with null sa4_region"},
            {"section": "spatial_join", "metric": "nearest_fallback_chargers", "value": str(nearest_fallback_count), "note": "unmatched coastal points assigned to the nearest SA4 within 100 m"},
            {"section": "spatial_join", "metric": "distinct_sa4_regions", "value": str(clean_export["sa4_region"].nunique()), "note": "SA4_NAME26 values"},
        ]
    )
    summary = pd.concat([result.summary, spatial_summary], ignore_index=True)
    return replace(result, clean_export=clean_export, dc_candidates=dc_candidates, summary=summary)


# --------------------------------------------------------------------------- CLI
def write_outputs(root: Path, result: CleaningResult) -> None:
    def target(rel: str) -> Path:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def save_table_pair(table: pd.DataFrame, parquet_path: str, csv_path: str) -> None:
        """Save the same table in Parquet and readable CSV formats."""
        table.to_parquet(target(parquet_path), index=False)
        table.to_csv(target(csv_path), index=False, encoding="utf-8")

    if list(result.clean_export.columns) != CLEAN_COLUMNS:
        raise ValueError("Clean output must include the final sa4_region column")
    if list(result.dc_candidates.columns) != DC_COLUMNS:
        raise ValueError("DC output must include the final sa4_region column")

    try:
        save_table_pair(result.clean_export, OUT_CLEAN, OUT_CLEAN_CSV)
        save_table_pair(result.dc_candidates, OUT_DC, OUT_DC_CSV)
    except ImportError as exc:                       # pragma: no cover
        raise SystemExit("pyarrow is required to write parquet: python -m pip install pyarrow") from exc
    result.summary.to_csv(target(OUT_SUMMARY), index=False, encoding="utf-8")
    result.excluded.to_csv(target(OUT_EXCLUDED), index=False, encoding="utf-8")
    result.review.to_csv(target(OUT_REVIEW), index=False, encoding="utf-8")
    result.audit.to_csv(target(OUT_AUDIT), index=False, encoding="utf-8")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Clean the raw TfNSW EV charger CSV (member B).")
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--dup-distance-m", type=float, default=Config.dup_distance_m,
                        help="cross-programme duplicate distance threshold in metres (default 30)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    root = args.project_root.resolve()
    raw_path = root / RAW_NSW_CSV
    sa4_path = root / RAW_SA4_SHP
    if not raw_path.exists():
        LOG.error("raw file not found: %s (run member A's acquisition first)", raw_path)
        return 1
    cfg = replace(Config(), dup_distance_m=args.dup_distance_m)
    raw = load_raw(raw_path)
    result = run_cleaning(raw, cfg)
    result = add_sa4_region(result, sa4_path)
    write_outputs(root, result)
    LOG.info("raw %s rows -> clean %s rows (AC %s / DC %s), %s stations, DC candidates %s, SA4 matched %s",
             len(raw), len(result.clean), (result.clean.charger_type == "AC").sum(),
             (result.clean.charger_type == "DC").sum(), result.clean.station_id.nunique(),
             len(result.dc_candidates), result.clean_export.sa4_region.notna().sum())
    return 0


if __name__ == "__main__":
    sys.exit(main())
