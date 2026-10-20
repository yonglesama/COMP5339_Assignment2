# COMP5339 Assignment 1 — EV Charger Data Integration

## 1. Project structure

This folder contains the Member A acquisition, Member B cleaning and spatial
integration, Member C data augmentation, Member D database, and final validation
stages.
Use `--stage ab --offline` to verify the cached raw inputs and run cleaning in one
command. Member C can be rerun independently from its saved OpenStreetMap cache.
All stages listed in `config/pipeline.json` can be run through the shared pipeline.

```text
src/acquisition/
    __init__.py                   Acquisition package
    retrieve.py                   Download, validation and extraction functions
src/cleaning/
    __init__.py                   Cleaning package
    run.py                        Cleaning, de-duplication and ID assignment
src/augmentation/
    __init__.py                   Augmentation package
    run.py                        Overpass retrieval, matching and enrichment
src/database/
    __init__.py                   Database package
    run.py                        DuckDB loading and validation
sql/schema.sql                   Relational DDL and spatial extension setup
config/
    sources.json                  Dataset URLs, versions, checksums and paths
    pipeline.json                 Stage entry points, inputs and outputs
data/
    source_manifest.csv           Source and checksum inventory
    raw/
        nsw/
            ev_20251216.csv
        abs/
            SA4_2026_AUST_SHP_GDA2020.zip
            SA4_2026_AUST_SHP_GDA2020/
                SA4_2026_AUST_GDA2020.shp
                SA4_2026_AUST_GDA2020.shx
                SA4_2026_AUST_GDA2020.dbf
                SA4_2026_AUST_GDA2020.prj
                SA4_2026_AUST_GDA2020.cpg
                SA4_2026_AUST_GDA2020 - Shapefile ISO Metadata.xml
        metadata/
            nsw_ev_catalogue.json
            abs_edition4_boundaries.html
        augmentation/
            osm_nsw_charging_stations.json   Complete Overpass response cache
            osm_nsw_charging_stations.csv    Flattened readable cache
            source_metadata.json             Query, timestamps, licence and attribution
    processed/
        chargers_clean.parquet
        chargers_clean.csv
        dc_candidates.parquet
        dc_candidates.csv
        charger_enrichment.parquet
    quality/
        cleaning_summary.csv
        excluded_records.csv
        duplicate_review.csv
        chargers_clean_audit.csv
        augmentation_coverage.csv
        matching_review.csv
        database_validation.csv          Database row, key, relation and spatial checks
        final_quality_report.csv         Independent final cross-stage validation
    final/
        ev_chargers.duckdb         Loaded relational database with spatial geometry
docs/data_dictionary.md           Cleaning and augmentation fields, rules and limitations
docs/data_augmentation_methodology.md  Report-ready Member C method and limitations
tests/                            Acquisition, cleaning and augmentation tests
run_pipeline.py                   Shared command-line entry point
validate_pipeline.py              Independent final cross-stage validation
requirements.txt                  Project dependencies
README.md                         Setup and usage instructions
```

## 2. Setup instructions

Use Python 3.10 or later. From the project directory, install the dependencies:

Create and activate a clean virtual environment first (Windows PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If activation is unavailable, use `.\.venv\Scripts\python.exe` in place of
`python`. Member A's code uses the standard library. The requirements file also
contains Member B's dataframe and Parquet dependencies.

```shell
python -m pip install -r requirements.txt
```

## 3. Execution instructions

### Download the required datasets

```shell
python run_pipeline.py
```

This downloads the two datasets, extracts the ABS archive and records their sources in `data/source_manifest.csv`. Existing files are checked and reused.

Data is saved under the current directory. A bare `--project-root` also uses the current directory; provide a path to save elsewhere:

```shell
python run_pipeline.py --project-root ./fresh-acquisition
```

### Verify or reuse local data without downloading

```shell
python run_pipeline.py --stage verify-raw
python run_pipeline.py --stage acquire --offline
```

Both commands avoid downloading. `verify-raw` is strictly read-only: it checks the
manifest, both original downloads, metadata, and every required extracted ZIP member.
It fails on missing or altered files without repairing them or rewriting the manifest.
`acquire --offline` is a recovery/reuse operation: it can recreate missing extracted
files from the verified local ZIP and update the manifest, but cannot fetch missing downloads.

### Execute configured pipeline stages

```shell
python run_pipeline.py --stage cleaning
python run_pipeline.py --stage ab --offline
python run_pipeline.py --stage augmentation
python run_pipeline.py --stage database
python run_pipeline.py --stage validate
python run_pipeline.py --stage all
```

The `ab` stage verifies the cached A inputs and then runs B cleaning. Without
`--offline`, it performs the normal acquisition/reuse checks before cleaning.
The `all` stage runs acquisition → cleaning (including the SA4 spatial join) →
augmentation → database → validate, using the scripts listed in
`config/pipeline.json`. All
configured scripts must be present. The `--offline` option is supported for
acquisition, raw verification and the combined AB stage.

The augmentation stage downloads OpenStreetMap charging-station objects only if
`data/raw/augmentation/osm_nsw_charging_stations.json` is absent. It otherwise
uses the local cache, so an ordinary rerun is reproducible and makes no API call.
To deliberately refresh the external snapshot, run:

```shell
python src/augmentation/run.py --project-root . --refresh
```

The database stage reads the cleaned and enriched Parquet files, the cleaning
audit's numeric ratings, and the ABS SA4 shapefile. It creates the seven tables in
`sql/schema.sql` and writes `data/final/ev_chargers.duckdb`. It installs and loads
DuckDB's `spatial` extension, so first use may require network access. The 28
NSW SA4 polygons with charger records are stored with WGS84 geometry. Two ABS
non-spatial NSW categories have no polygons and are omitted. One coastal charger
was assigned its nearest SA4 during cleaning and therefore lies just outside
that polygon; 1,802 of 1,803 charger points intersect their assigned polygon.
The central `chargers` table links to `charger_locations` and the one-to-one
`charger_characteristics` table. Exact coordinates are stored once, so the 1,803
charger records use 1,799 location rows. This keeps spatial and technical fields
out of the central entity table without losing any processed-data columns.
The `charger_enrichment` table has one row per DC charger and uses `charger_id`
as both its primary key and a foreign key to `chargers`. This preserves all 419
rows from C without duplicating the main charger table; AC chargers have no
enrichment row, and externally unmatched DC chargers retain null added fields.
Run the stage directly with:

```shell
python src/database/run.py --project-root .
```

After opening the database in another DuckDB connection, run `LOAD spatial;`
before calling spatial functions.

For example, inspect stored charger points and count chargers whose point
intersects its assigned SA4 polygon:

```sql
LOAD spatial;

SELECT c.charger_id,
       ST_AsText(l.location_geom) AS point_wkt,
       ST_X(l.location_geom) AS longitude,
       ST_Y(l.location_geom) AS latitude
FROM chargers AS c
JOIN charger_locations AS l USING (location_id)
LIMIT 10;

SELECT COUNT(*) AS intersecting_chargers
FROM chargers AS c
JOIN charger_locations AS l USING (location_id)
JOIN stations AS s USING (station_id)
JOIN regions AS r USING (sa4_code)
WHERE ST_Intersects(l.location_geom, r.boundary_geom);
```

Each member script must accept `--project-root PATH`, return a nonzero exit code
on failure, and produce the configured nonempty output files. Other members' imports
must be included in the final merged requirements file. Capture a persistent run log
in PowerShell with `python run_pipeline.py --stage verify-raw 2>&1 | Tee-Object -FilePath verification.log`.

For a fresh-download test, use an empty output directory with
`python run_pipeline.py --project-root ./fresh-acquisition`. This directory receives
data only, not a copy of the team code; run later member stages from the complete project.

## 4. Assumptions made

- The required EV snapshot is identified by the official resource filename `ev_20251216.csv`. Catalogue update times and HTTP modification dates are recorded separately from the dataset version.
- Raw files are preserved unchanged and set to read-only. SHA-256 checksums are used to verify their integrity.
- Raw record counts precede deduplication and do not define the number of distinct stations or the final augmentation denominator. Missing station names remain null rather than being invented.
- The ABS archive covers Australia and uses GDA2020 / EPSG:7844. During cleaning,
  WGS84 charger points are transformed to the boundary CRS and joined to NSW SA4
  polygons. The single output field `sa4_region` contains `SA4_NAME26`. A point
  just outside a coastal polygon because of coordinate rounding may use the
  nearest SA4 within 100 m; the summary reports these fallback cases separately.
- Member C counts augmentation coverage by unique DC `station_id`, as requested for
  charging locations. Charger-row coverage is also reported as a stricter secondary check.
- An OSM object is matched within 100 m only when its operator, brand or network is
  compatible with the TfNSW operator. Coordinate-only matching is limited to 25 m
  and only when the OSM object has no operator, brand or network tag.
- Unmatched locations are retained with null external fields. OSM values are not
  used to overwrite TfNSW values.
- Member D uses `charger_id` as the one-to-one key between the clean charger table
  and C's DC-only enrichment; `station_id` is not unique in the enrichment input.
- Exact duplicate coordinate pairs share one `charger_locations` row. Address,
  station name and LGA remain charger attributes because they can differ at the
  same coordinate. Point and SA4 geometry are stored in WGS84 for direct queries.
- Both `rating_text` and the derived nullable `rating_kw` are retained as a
  documented analytical convenience rather than fully normalising rating values.

## 5. Outputs and included data files

The two datasets are the NSW EV CSV and the ABS SA4 shapefile. The ZIP and extracted shapefile components represent the same boundary dataset; the manifest and catalogue snapshots document their sources.

### Data directories

| Directory | Purpose |
| --- | --- |
| `data/` | Data files and their source manifest. |
| `data/raw/` | Original downloads, exact archive extracts and source evidence. |
| `data/raw/nsw/` | NSW charging-location CSV. |
| `data/raw/abs/` | Original ABS ZIP and its extracted boundary files. |
| `data/raw/abs/SA4_2026_AUST_SHP_GDA2020/` | All six shapefile components, kept together for spatial software. |
| `data/raw/metadata/` | Saved official catalogue and download-page responses. |
| `data/processed/` | Cleaned Parquet/CSV data and the C enrichment output. |
| `data/quality/` | Cleaning, matching, coverage and database-validation evidence. |
| `data/final/` | Final DuckDB database used for analysis and visualisation. |

### Downloaded files and manifest

| File | Description |
| --- | --- |
| `data/raw/nsw/ev_20251216.csv` | December 2025 TfNSW charging-location data: 1,958 raw records and 12 fields, including location, operator, plug count, charger type and rating. Raw types comprise 1,427 AC, 433 DC and 98 Upcoming records. |
| `data/raw/abs/SA4_2026_AUST_SHP_GDA2020.zip` | Original ASGS Edition 4 SA4 archive for 2026, containing six files and 108 national DBF records. |
| `data/raw/metadata/nsw_ev_catalogue.json` | CKAN catalogue response identifying the EV dataset and its resource URL. |
| `data/raw/metadata/abs_edition4_boundaries.html` | ABS download-page snapshot identifying the selected Edition 4 archive. |
| `data/source_manifest.csv` | Records each file's source URL, publisher, version, UTC retrieval time, local path, size and SHA-256, with supporting download metadata. |

### Extracted shapefile components

The following files are inside `data/raw/abs/SA4_2026_AUST_SHP_GDA2020/`. Keep their filenames and directory placement together.

| File | Description |
| --- | --- |
| `SA4_2026_AUST_GDA2020.shp` | SA4 boundary geometries used for spatial queries and joins. |
| `SA4_2026_AUST_GDA2020.shx` | Index locating geometry records within the `.shp` file. |
| `SA4_2026_AUST_GDA2020.dbf` | Regional attributes, including `SA4_CODE26`, `SA4_NAME26` and `STE_CODE26`. |
| `SA4_2026_AUST_GDA2020.prj` | Coordinate reference system definition: GDA2020 / EPSG:7844. |
| `SA4_2026_AUST_GDA2020.cpg` | Character encoding declaration for attribute text. |
| `SA4_2026_AUST_GDA2020 - Shapefile ISO Metadata.xml` | ABS documentation of the boundary dataset and its provenance. |

Official sources: [TfNSW EV Charging Locations](https://opendata.transport.nsw.gov.au/data/dataset/ev-charging-locations), [data.gov.au catalogue](https://data.gov.au/data/dataset/nsw-2-ev-charging-locations), and [ABS Edition 4 digital boundaries](https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs/edition-4-july-2026-june-2031/access-and-downloads/digital-boundary-files).

## 6. Member B cleaning outputs

Run `python run_pipeline.py --stage cleaning` when the A inputs have already
been verified, or use `python run_pipeline.py --stage ab --offline` to run both
completed stages together. The cleaning stage produces 1,803 existing charger
records from the 1,958-row raw snapshot: 1,384 AC and 419 DC records across
1,772 station groups. Detailed rules and assumptions are documented in
`docs/data_dictionary.md`.

The main cleaned dataset is available as `chargers_clean.parquet` and
`chargers_clean.csv`; both contain the same 1,803 rows and 15 standardised
lower-case columns. The DC-only interface is available as
`dc_candidates.parquet` and `dc_candidates.csv`; both contain the same 419 rows
and 14 columns. The last field in both datasets is `sa4_region`, added by the
ABS SA4 spatial join. CSV copies are included for inspection, while Parquet preserves
data types for downstream processing.

All 1,803 cleaned records matched one of 28 NSW SA4 regions. One Clontarf coastal
coordinate was 1.28 m outside the land polygon and used the documented nearest-SA4
fallback; `cleaning_summary.csv` records this separately.

Blank or operator-only station names remain null: the cleaning code does not
invent an official-sounding name. `Upcoming` rows are excluded because they are
not installed chargers. Cross-programme duplicates are merged within 30 m, or
within 100 m when the normalised street address is identical. When the address
postcode conflicts with the source `PCODE`, the address postcode is used. The raw
input is never modified and the generated IDs are deterministic.

Cleaning can also be run directly with
`python src/cleaning/run.py --project-root .`. The optional
`--dup-distance-m 50` argument changes the cross-programme duplicate threshold.
The quality files `cleaning_summary.csv`, `excluded_records.csv`,
`duplicate_review.csv` and `chargers_clean_audit.csv` retain the audit trail.
Run all automated checks with `python -m unittest discover -s tests -v`.

## 7. Member C data-augmentation outputs

Member C queries the read-only OpenStreetMap Overpass API for
`amenity=charging_station` objects in a NSW bounding box and stores the complete
response locally. It matches the external objects to the 414 unique DC locations
from `dc_candidates.parquet` using compatible operator names and Haversine distance.
The maximum operator-assisted distance is 100 m. A stricter 25 m rule is used for
objects with no external operator label.

`data/processed/charger_enrichment.parquet` is the final, easy-to-use enhanced
dataset. It keeps all 419 rows and all 14 columns from the spatially joined
`dc_candidates.parquet`
unchanged, then appends exactly three columns: `plug_types`, `charging_bays` and
`pricing_information`. Unmatched rows are retained with null values in these new
columns. Match method, distance and OSM identifiers are kept separately in the
quality-review file rather than making the final table unnecessarily wide.

| File | Description |
| --- | --- |
| `data/raw/augmentation/osm_nsw_charging_stations.json` | Complete Overpass JSON response retained for reproducibility. |
| `data/raw/augmentation/osm_nsw_charging_stations.csv` | Flattened readable copy of the external OSM data. |
| `data/raw/augmentation/source_metadata.json` | Exact query, endpoint, retrieval/data timestamps, bounding box and licence. |
| `data/processed/charger_enrichment.parquet` | 419 DC charger rows and 17 columns: 14 cleaned/spatial columns plus three OSM attributes. |
| `data/quality/augmentation_coverage.csv` | Coverage based only on the three added attributes, including the 50% checks. |
| `data/quality/matching_review.csv` | All 414 locations with candidates, match decision, distance, confidence and review reason. |

API used: [Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API).
Supporting references: [OSM charging-station tag definitions](https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dcharging_station)
and [OpenStreetMap copyright/licence](https://www.openstreetmap.org/copyright).
The external data is © OpenStreetMap contributors and is available under the ODbL.

## 8. Member D database outputs

Run `python run_pipeline.py --stage database` or
`python src/database/run.py --project-root .` to rebuild the database. The stage
executes `sql/schema.sql`, loads the cleaned and enriched data, installs and loads
DuckDB's spatial extension, creates point and SA4 polygon geometry, and validates
the closed and reopened database.

| File | Description |
| --- | --- |
| `sql/schema.sql` | Recreates the seven-table relational schema, constraints and spatial-extension setup. |
| `src/database/run.py` | Validates the inputs, loads all tables, creates geometry and runs database checks. |
| `data/final/ev_chargers.duckdb` | Final DuckDB file containing 1,803 chargers, their related entities and spatial fields. |
| `data/quality/database_validation.csv` | Persistent results of 30 row-count, PK, FK, enrichment, coordinate, SA4, spatial and reopen checks. |

The schema separates regions, stations, operators, exact locations, charger core
records, technical characteristics and DC-only enrichment. This reduces repeated
operator, region and coordinate values. It intentionally retains both
`rating_text` and `rating_kw`: the former preserves the readable source value and
the latter supports numerical analysis. New DuckDB connections must execute
`LOAD spatial;` before using spatial functions.

## 9. Final validation output

Run `python run_pipeline.py --stage validate` or
`python validate_pipeline.py --project-root .` to validate existing final outputs
without rebuilding them. The validator opens the database read-only, compares the
reconstructed charger and enrichment rows with both Parquet inputs, checks table
counts, PK and FK integrity, completeness and spatial queries, closes the database,
then reopens it for a final spatial query. Results are written to
`data/quality/final_quality_report.csv`; any failed check returns a nonzero exit
code. `python run_pipeline.py --stage all` runs this validation after A, B, C and D.
