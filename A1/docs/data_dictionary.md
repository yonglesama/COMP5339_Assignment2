# Data dictionary and cleaning rules (member B)

Sources: `data/raw/nsw/ev_20251216.csv` (Transport for NSW, 1,958 rows,
12 columns) and the ABS ASGS Edition 4 SA4 Shapefile (checksums in
`data/source_manifest.csv`).
Produced by `src/cleaning/run.py`; statistics of every run are in `data/quality/cleaning_summary.csv`.
Numbers quoted below are from the December 2025 file with default thresholds.

## 1. Grain and keys

| Concept | Definition |
| --- | --- |
| charger record (`charger_id`) | One raw row after cleaning: `number_of_plugs` plugs of one operator, one current type (AC/DC) and one power rating at one coordinate. D loads this charger-level unit. |
| station (`station_id`) | All charger records within 30 m of each other (single linkage, any operator or type). The raw file has no station identifier. 1,803 charger records fall into 1,772 stations. |
| `charger_id` | `EV` + first 10 hex characters of SHA-1 of `latitude(6 dp) | longitude(6 dp) | operator | type | kW`. Deterministic: the same raw file always gives the same IDs. A `-2` suffix is added on the (so far never observed) hash collision. |
| `station_id` | `ST` + suffix of the smallest `charger_id` in the station. |

## 2. Primary cleaned outputs (existing AC + DC chargers)

`data/processed/chargers_clean.parquet` and
`data/processed/chargers_clean.csv` contain the same 1,803 records, the same
row order and the same 15 columns. The Parquet file is the typed representation;
the CSV is the readable representation.

| Column | Meaning / cleaning |
| --- | --- |
| `charger_id` | Unique cleaned-record key, defined in section 1. |
| `station_id` | Groups charger records at the same site, defined in section 1. |
| `source_objectid` | Original `OBJECTID` value retained. |
| `station_name` | Trimmed; a value that only repeats the operator becomes null. Missing names are not invented. |
| `address` | One line; trailing `Australia` removed; `New South Wales` becomes `NSW`. |
| `operator_name` | Canonical spelling from rule T3. Original spelling remains in the audit file. |
| `number_of_plugs` | Positive integer plug count. |
| `charger_type` | `AC` or `DC`; raw `Upcoming` rows are excluded. |
| `rating_text` | Normalised rating text; a bare number gains `kW`; the placeholder `AC` becomes null. |
| `latitude`, `longitude` | Numeric WGS84 coordinates inside the configured NSW bounding box. |
| `lga_name` | Source-provided LGA name, trimmed only. |
| `postcode` | Four-digit postcode; the address value wins when it conflicts with the raw field. |
| `source_program` | Original TfNSW source programme, trimmed. |
| `sa4_region` | ABS `SA4_NAME26` polygon containing the charger point; added by the spatial join. |

The first two columns are the generated IDs. The following 12 fields retain the
raw file's order but use lower-case `snake_case` names. `sa4_region` is appended
as the one assignment-required spatial field. Missing values are stored as nulls
in Parquet and as empty fields in CSV.

## 3. DC outputs (input of member C)

`data/processed/dc_candidates.parquet` and `dc_candidates.csv` contain the same
419 DC charger records (414 stations), in this order:
`charger_id, station_id, station_name, address, operator_name, number_of_plugs,
rating_text, rating_kw, latitude, longitude, lga_name, postcode, source_program,
sa4_region`.
Every DC record has a rating. **Coverage denominator:** the assignment asks for at least 50 % of "fast charger locations", so coverage should be counted per station (>= 207 of 414 DC stations) and, as a stricter check, per record (>= 210 of 419). Both numbers are in `cleaning_summary.csv` (section `dc_candidates`).

## 3b. Spatial integration and audit information

After all existing cleaning and de-duplication rules run, `src/cleaning/run.py`
creates WGS84 points from `longitude` and `latitude`, transforms them to the ABS
boundary CRS (GDA2020 / EPSG:7844), keeps NSW polygons (`STE_CODE26 = 1`) and
performs a point-within-polygon spatial join. Only `SA4_NAME26` is appended, under
the processed name `sa4_region`. The join must preserve all 1,803 charger rows
and the uniqueness of `charger_id`. Match counts are recorded in
`cleaning_summary.csv` under section `spatial_join`. If a coastal coordinate falls
just outside the land polygon because of rounding, an unmatched point may use the
nearest NSW SA4 within 100 m; this fallback count is reported separately. In the
current snapshot all 1,803 records matched 28 SA4 regions, including one Clontarf
coordinate assigned by this fallback at a distance of 1.28 m.

The primary Parquet and CSV use the source field order but standardise every
processed column name to lower-case `snake_case`.

| Column | Difference from the raw file |
| --- | --- |
| `source_objectid`, `number_of_plugs`, `charger_type`, `latitude`, `longitude`, `source_program` | renamed source fields; values are otherwise retained (`Upcoming` rows are excluded) |
| `station_name` | trimmed; a name that only repeats the operator becomes null; missing names remain missing |
| `address` | one line, "Australia" removed, "New South Wales" -> "NSW" (rule T2) |
| `operator_name` | canonical spelling (rule T3); the raw spelling is in the audit file |
| `rating_text` | `22` -> `22 kW`; the placeholder `AC` becomes blank (rule T4) |
| `lga_name` | trimmed only |
| `postcode` | valid 4-digit postcode, address wins over a wrong raw value (rule T6) |
| `charger_id`, `station_id` (first two columns) | keys defined in section 1 |

Rows: 1,803 (raw 1,958 minus 98 upcoming minus 57 merged duplicates). Cells changed against the raw row, for the 1,753 records that were not merged: Station_address 771, Charger_rating 496, Operator 41, PCODE 39, Station_name 19, all other columns 0.

`data/quality/chargers_clean_audit.csv` has the same row order and joins on
`charger_id`. Its columns are `charger_id, station_id, source_rows,
merged_record_count, operator_raw, station_name, rating_kw, quality_flags`.
`rating_kw` is the numeric power extracted from `rating_text`; missing station
names stay null and receive the `station_name_missing` quality flag.

`dc_candidates.parquet` remains the standardised DC-only interface for member C.
Member D should read the standardised names directly, for example `address`,
`longitude`, `latitude` and `charger_type`.

## 4. Cleaning rules

| Rule | What is done | Rows affected |
| --- | --- | --- |
| R1 | Raw `Charger_Type = Upcoming` (planned, not installed) is excluded, written to `excluded_records.csv` | 98 |
| R2 | Coordinates that are non-numeric, zero, swapped or outside the NSW box are excluded | 0 |
| T1 | Trim, turn newline/NBSP into one space, blank -> null (all text columns) | 733 addresses had newlines; 20 names, 55 operators had stray spaces |
| T2 | Address on one line; `\n` -> `, `; drop trailing "Australia"; "New South Wales" -> "NSW"; fix `" ,"`; drop leading comma | 927 |
| T3 | Operator mapping (Tesla Motors -> Tesla, BP Australia -> BP, Evie Networks -> Evie, NRMA Electric -> NRMA, Non-Networked -> Non-networked, Charge Hub -> ChargeHub, Porsche ... -> Porsche) and repair of values cut at 13 characters (`Viva Energy A`, `Energy Austra`, `Fast Cities A`, `PLUS ES Manag`, `University of`) | 153 remapped, 28 truncated |
| T4 | Rating text -> `rating_kw` (max per plug) and `rating_text`; a bare number gets the unit `kW`; the placeholder `AC` becomes null | 522 placeholders, 18 units added, 5 mixed ratings |
| T5 | Plug count -> integer, must be > 0 | 0 invalid |
| T6 | Postcode: `NSW 2500` -> `2500`; if address and raw `PCODE` disagree the address wins (checked by hand on all 24 cases: raw `PCODE` was wrong, e.g. Wilcannia 2836 vs 2350); missing -> from address | 10 / 24 / 22 |
| T7 | Station name: trim text; blank or operator-only values remain null, because no official name should be invented | 1,304 kept records missing a useful name |
| D1 | Records of **different** `Source` programmes, same type, <= 30 m, compatible operator (equal, or one is the placeholder `Non-networked`, or same spot <= 2 m with equal plug count) and compatible rating (equal or one missing, or DC with equal plug count) are one charger -> merged | see summary |
| D2 | Same normalised street address, <= 100 m, otherwise compatible -> merged | 6 pairs |
| D3 | A chain A~B~C must not merge two different known AC ratings (22 kW ~ `AC` ~ 7 kW) | 1 pair blocked |

Merging keeps the record with (programme present, named operator, real station name, rating present, lowest line number) and fills its empty fields from the others. Merge conflicts stay visible as flags. Records of the **same** programme are never merged (e.g. kerbside posts 20 m apart are separate chargers); such close pairs are listed for review.
Duplicates in total: 57 raw rows removed (50 merge groups, largest group 3). Sensitivity of the distance threshold (rows removed): 5 m 31, 10 m 33, **30 m 51 (+6 by D2)**, 50 m 62, 100 m 80.

Flags in `quality_flags`: `station_name_missing`, `rating_missing`, `merged_duplicates`, `merge_conflict_plugs|rating|operator|name`, `postcode_overridden_by_address`, `postcode_filled_from_address`, `postcode_not_2xxx` (2 rows: Barooga NSW uses postcode 3644), `source_program_missing`, `operator_truncation_repaired`, `plugs_very_high_check` (>= 25 plugs, kept), `type_rating_mismatch` (AC >= 24 kW or DC < 24 kW; none found), `rating_text_plugs_differ`, `address_no_street`, `plugs_missing_or_invalid`, `postcode_missing`.

## 5. Audit files (`data/quality/`)

| File | Content |
| --- | --- |
| `cleaning_summary.csv` | `section, metric, value, note`: raw profile, effect of every rule, duplicates, before/after missing rates, reconciliation `raw = excluded + clean` |
| `excluded_records.csv` | every raw row that is not in `chargers_clean` with `reason` (`upcoming_not_installed`, `duplicate_merged` + kept line number, invalid coordinate reasons) |
| `duplicate_review.csv` | every candidate pair within 100 m with `decision` (`merged` / `kept_separate`) and `reason` (`cross_programme_duplicate`, `same_programme_close_pair`, `beyond_merge_distance`, `operator_conflict`, `rating_conflict`, `transitive_rating_conflict`) - use it to spot-check and to write the report |
| `chargers_clean_audit.csv` | per-record source rows, original operator text, retained station name, numeric rating and quality flags |

## 6. Known limitations / assumptions to state in the report

* The raw file has no station key and no connector level; `Number_of_plugs` is the only plug information. Plug types (CCS2, Type 2, CHAdeMO) come from augmentation (member C).
* 495 AC records have no rating in the source (`AC` placeholder); they are kept with null `rating_kw`.
* Operator mapping of truncated values is an inference from the other rows (e.g. `Fast Cities A` -> `Fast Cities Australia`); the original text stays in `operator_raw`.
* `Upcoming` rows are not installed chargers and are outside the coverage analysis; they are listed in `excluded_records.csv` if Assignment 2 needs them.
* 23 pairs 31-100 m apart with different addresses were left separate (`beyond_merge_distance`); re-run with `--dup-distance-m 50` to see the effect.
* Five surviving rows come from the 121-row block of the raw file that has no `Source`, `LGANAME` or `PCODE` (they are flagged `source_program_missing`). One of them (line 1077, Tesla Campbelltown, 12 plugs) carries an upgrade-style rating (`2x350 kW & 2x175 kW`) although its raw type is DC; it is kept because the raw type says DC.
* The initial coordinate rule uses a loose NSW bounding box. The later point-in-polygon join in the same cleaning program assigns the exact ABS SA4 region; unmatched points remain present with null `sa4_region` and are counted in the summary.

## 7. Member C augmentation data dictionary

Source: OpenStreetMap `amenity=charging_station` objects obtained through the
Overpass API. The raw JSON, flattened CSV, exact query, retrieval timestamp,
OpenStreetMap data timestamp, attribution and ODbL details are stored in
`data/raw/augmentation/`.

Grain: one row per DC charger record in
`data/processed/charger_enrichment.parquet`. The file contains the same 419 rows
and original 14 columns as `dc_candidates.parquet`, in the same order, followed by
three new columns. Several charger records at one `station_id` therefore share the
same location-level OSM values. Unmatched rows remain present with null new fields.

| Column | Meaning |
| --- | --- |
| Original 14 columns | Copied unchanged from `dc_candidates.parquet`: `charger_id`, `station_id`, `station_name`, `address`, `operator_name`, `number_of_plugs`, `rating_text`, `rating_kw`, `latitude`, `longitude`, `lga_name`, `postcode`, `source_program`, `sa4_region`. |
| `plug_types` | Human-readable connector types derived from positive `socket:<type>` tags, for example `CCS Type 2; CHAdeMO`. |
| `charging_bays` | Positive integer OSM `capacity`; OSM defines this as vehicles that can charge simultaneously. |
| `pricing_information` | OSM `charge` value, or a conservative interpretation of `fee` (`free` or `fee required; rate not specified`). |

Matching columns such as `match_status`, `match_method`, distance, confidence and
OSM object ID are not part of the 17-column final dataset. They remain in
`data/quality/matching_review.csv`, which has one row for each of the 414 unique
DC locations and provides the audit trail required to explain the matching strategy.

### Matching rules and quality evidence

1. Collapse the 419 DC charger rows to 414 unique locations by `station_id`.
2. Search OSM candidates within 100 m using Haversine distance.
3. Prefer the nearest candidate whose `operator`, `brand` or `network` is a
   canonical match to the TfNSW operator (including Evie/Evie Networks,
   Ampol/AmpCharge and BP/bp pulse aliases).
4. If no operator-compatible candidate exists, accept a coordinate-only match only
   within 25 m and only when OSM has no operator, brand or network tag. Explicit
   operator conflicts remain unmatched.
5. Keep every decision in `data/quality/matching_review.csv`, including unmatched
   reasons and cases where one OSM object is shared by multiple station IDs.

`data/quality/augmentation_coverage.csv` records the assignment's primary
station-level 50% check and a stricter charger-record check. A location counts as
augmented only when at least one of the three final new columns is populated. OSM
is community-maintained and attributes can be incomplete or stale; this is a
report limitation, not a reason to impute missing values.
