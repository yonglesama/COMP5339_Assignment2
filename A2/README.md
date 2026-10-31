# COMP5339 Assignment 2 pipeline

This directory contains the shared Assignment 2 pipeline modelled on Assignment 1. The
A, B, C and D implementations are integrated. D's Flask/Leaflet dashboard has
private-copy demo/MQTT modes and read-only historical browsing.
The A1 database remains read-only.

## What is provided

- a configuration-driven runner with declared stage inputs and outputs;
- the professor-provided generator, retained unchanged;
- A's deterministic generation, event preparation and MQTT publishing pipeline;
- B's MQTT subscription, validation, recoverable live state and D-facing interface;
- C's DuckDB schema, event repository, analytical queries and validation;
- D's Flask/Leaflet map, history browser and live MQTT charging curves;
- development and assessment profiles, fixtures, tests and team contracts.

## Ownership and structure

```text
A2/
  config/                  Pipeline, generator profiles and MQTT settings
  external/data_generator/ Professor-provided files
  data/examples/            Shared message-contract fixtures
  src/generation/           A: station export and generator wrapper
  src/events/               A: ordered event preparation
  src/publishing/           A: MQTT publisher
  src/subscription/         B: subscriber and recoverable live state
  src/database/             C: event repository, full-stream import and analytical queries
  sql/a2_schema.sql         C: A2 event tables, indexes and analytical views
  src/dashboard/            D: Flask API/runtime and Leaflet presentation
  tests/                    Unit and integration tests
  docs/team_contracts.md    Stable hand-off contracts
  run_pipeline.py           Working shared orchestrator
  validate_pipeline.py      C/shared final reconciliation CLI
```

The authoritative task split and method signatures are in
`docs/team_contracts.md`.

## Member responsibilities and implementation status

### Member A — data generation and MQTT publishing

Member A's implementation is complete and owns:

```text
A2/src/generation/export_stations.py
A2/src/generation/run.py
A2/src/events/prepare.py
A2/src/publishing/run.py
```

Member A maintains generation/publishing tests under `A2/tests/` and the JSON
fixtures in `A2/data/examples/` when the whole group approves a message-contract change.
Member A may propose changes to the generator profiles, but should not edit the
professor-provided files under `A2/external/data_generator/`.

Expected outputs:

```text
A2/data/input/generator_stations.csv
A2/data/generated/stations.csv
A2/data/generated/prices_hourly.csv
A2/data/generated/sessions.csv
A2/data/generated/session_metering.csv
A2/data/events/event_stream.jsonl
A2/data/demo/event_stream.jsonl
A2/data/quality/generation_summary.csv
A2/data/quality/generation_checksums.csv
A2/data/quality/event_preparation_summary.csv
A2/data/quality/demo_summary.csv
```

### Member B — MQTT subscription and live state

Member B's implementation is complete and merged. It owns:

```text
A2/src/subscription/run.py
A2/src/subscription/event_processor.py
A2/src/subscription/state_store.py
```

B validates JSON envelopes and MQTT topics, persists each new event through C's
`EventRepository`, and only then updates its thread-safe live projection. The projection
provides latest prices, active sessions, connector occupancy, restart recovery and active
session meter curves to D. The subscription module contains no DuckDB SQL. Operational
details and the D integration contract are recorded in `docs/member_b_handoff.md`.

### Member C — DuckDB storage and analytics

Member C's implementation is complete and owns:

```text
A2/src/database/initialise.py
A2/src/database/import_events.py
A2/src/database/repository.py
A2/sql/a2_schema.sql
A2/validate_pipeline.py
```

Member C maintains storage/analytics tests under `A2/tests/`. C owns the final integrated
DuckDB, DDL, idempotent event-write interface, current/historical queries and final
reconciliation report. C must create a separate A2 database and must not write to the
A1 database.

Expected outputs:

```text
A2/data/final/ev_chargers_a2.duckdb
A2/data/quality/database_initialisation.csv
A2/data/quality/database_import.csv
A2/data/quality/final_validation.csv
```

### Member D — map dashboard and live curve

Member D's implementation is complete and owns:

```text
A2/src/dashboard/runtime.py
A2/src/dashboard/run.py
A2/src/dashboard/assets/
A2/src/database/replay_snapshot.py
A2/tests/test_member_d.py
A2/tests/test_station_history_dashboard.py
A2/tests/test_unified_dashboard.py
```

D composes B's `StateStore` and subscription service with C's repository. History mode
opens the formal database read-only; demo, unified and external MQTT modes write only to
UUID-owned private copies under `A2/data/runtime/`.

### Shared files — change only after group agreement

```text
A2/config/pipeline.json
A2/config/mqtt.json
A2/config/generator.dev.json
A2/config/generator.assessment.json
A2/docs/team_contracts.md
A2/data/examples/*
```

`A2/run_pipeline.py` should normally remain stable. Change the orchestrator only when a
new shared stage or command-line contract has been approved by the group.

### Files nobody should modify

```text
A1/*
A2/external/data_generator/*
```

Treat A1 as the read-only upstream dataset/database and the external generator as
professor-provided source. If a wrapper or adapter is needed, implement it in the
member-owned A2 directories instead of editing either source.

## Setup

From the repository root with Python 3.11 to 3.13 recommended:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

The root requirements file contains the complete A1/A2 environment, including the local
MQTT broker used by the unified dashboard and real transport tests.

Python 3.14 cannot currently install the pinned `amqtt`/`PyYAML` development combination
from a pre-built Windows wheel; use Python 3.11 to 3.13 for the broker test environment.

The populated A2 DuckDB, empty replay seed and complete one-day event stream are tracked
with Git LFS. Install Git LFS before cloning or run the following after installation to
replace the pointers with the actual runtime data files:

```powershell
git lfs install
git lfs pull
```

Each member must add any dependency introduced by their implementation to the root
`requirements.txt`. Member A owns the final merged dependency check.

## Inspect the pipeline

This command works now and shows every stage and owner:

```powershell
python A2/run_pipeline.py --list-stages
```

The intended development pipeline is:

```powershell
python A2/run_pipeline.py --stage prepare-dev
```

The assessment preparation pipeline is:

```powershell
python A2/run_pipeline.py --stage prepare
```

`prepare` regenerates A's assessment CSV, full JSONL and representative MQTT demo,
rebuilds and populates C's DuckDB, and finishes with full reconciliation. It is destructive
to the existing A2 DuckDB because the `database` stage intentionally rebuilds it from the
read-only A1 source. The equivalent `prepare-dev` alias uses the development profile.
It writes to the same working output paths and final A2 database, so rerun `prepare`
before submission after using `prepare-dev`.

After `prepare`, run the A+B+C transport verification with the development requirements
installed:

```powershell
python A2/run_pipeline.py --stage verify-abc
```

This first validates the populated assessment database, then runs the representative demo
through A's real QoS 1 publisher, B's subscriber/restart state and C's repository using an
isolated temporary DuckDB. The complete 172,803-event stream is not paced over MQTT because
the required 0.1-second delay would take at least 4.8 hours. The standalone publisher,
subscriber and dashboard remain long-running services and are started separately for live
operation.

## Member entry points

### A — generation, event preparation and publishing

```powershell
python A2/run_pipeline.py --stage station-export-dev
python A2/run_pipeline.py --stage synthetic-generation-dev
python A2/run_pipeline.py --stage event-preparation
python A2/run_pipeline.py --stage demo-preparation
python A2/run_pipeline.py --stage publisher
```

### B — subscription and live state

```powershell
python A2/run_pipeline.py --stage subscriber
```

The subscriber runs in the foreground until Ctrl+C or a fatal repository error. Start a
broker first and do not run the standalone subscriber while D owns the same writable
DuckDB. B's implementation restores latest prices, active sessions and meter curves from
C before subscribing.

Verify B without modifying the tracked assessment database:

```powershell
python -m unittest discover -s A2/tests -p "test_member_b.py" -v
python A2/run_pipeline.py --stage mqtt-integration
```

All 40 A/B/C tests and all 17 D tests pass. The real loopback MQTT demo
stores 36 new events, ignores 36 duplicate deliveries, rejects one malformed message, restores an
active curve after subscriber restart and completes one historical session. Evidence is
recorded in `data/quality/member_b_mqtt_summary.json` and `docs/member_b_report.md`.

The complete one-day stream contains 172,803 events. A paced MQTT replay would require at
least 4.8 hours, so the full stream is imported through the same validated C repository
boundary while MQTT transport is verified with the representative demo. Broker-outage recovery
and sustained long-running throughput are also outside the demo validation scope. The
complete stream is validated offline, while the representative demo covers the end-to-end
MQTT, B state and C persistence path. D uses the same interfaces for map state, station
history and live curves.

### C — DuckDB storage and analytics

```powershell
python A2/run_pipeline.py --stage database
python A2/run_pipeline.py --stage database-import
python A2/run_pipeline.py --stage validate
```

The database stage rebuilds an empty A2 schema from A1 and therefore clears event history.
The import stage is idempotent and loads the complete prepared stream through
`EventRepository`, then verifies and atomically installs a physically compacted copy; the
validation stage reconciles the stream, raw receipts, normalised tables, views and
queries. Keep A1 unchanged.

### D — map dashboard and live curve

```powershell
python A2/run_pipeline.py --stage dashboard
```

The recommended Windows entry is `Start_Dashboard.cmd`, which opens one unified
history/live page at http://127.0.0.1:8050. It reads C's formal database, creates a
private working copy, holds out three completed sessions, and restores their 87 unchanged
A events through A's publisher, B's subscriber and C's repository. The baseline moves
from 5,081 to the original 5,084 completed sessions without modifying the source database.

The unified replay requires `A2/data/events/event_stream.jsonl`. The complete stream is
tracked with Git LFS together with the formal databases. After a clone, run `git lfs pull`
once; the unified dashboard can then start without regenerating the assessment dataset.

```powershell
python -m pip install -r requirements.txt
python A2/src/dashboard/run.py --project-root . --mode unified --replay-delay 0.5 --open-browser
```

The pipeline command and a bare `run.py` default to read-only history. To open all 5,084
completed sessions without replay/reset controls:

```powershell
python A2/src/dashboard/run.py --project-root . --mode history --port 8052 --open-browser
```

The optional single-session demo uses C's empty reference database and A's unchanged
36-event demo stream:

```powershell
python A2/src/dashboard/run.py --project-root . --mode demo --initial-station ST4D056616A3 --port 8051 --open-browser
```

For an external MQTT broker, explicitly select `--mode mqtt --database PATH`. See
`docs/member_d_handoff.md` and the root `D_QUICKSTART.md` for the full operating and
data-protection contract.

## Shared fixture

`data/examples/short_event_stream.jsonl` provides one agreed scenario:

```text
price update -> session start -> meter -> meter -> session end
```

A maintains the contract; B, C and D can use it immediately without waiting for full
generation or MQTT deployment.

## Configuration profiles

- `generator.dev.json`: proposed small offline settings for integration development.
- `generator.assessment.json`: all 414 eligible DC stations, one day, AEMO prices and
  one-minute meters.

The one-day assessment scope was selected by the group because the assignment does not
mandate the generator's 14-day example. It retains the full eligible A1 station/connector
scope while keeping the complete database and submission package practical.

## Generated files and Git LFS delivery

The repository does not track the generated assessment CSV files. They are derived outputs
and are reproducible from the committed code and inputs. In the current one-day assessment
run, `session_metering.csv` is approximately 14.7 MiB before compression. The ordered
67.6 MiB JSONL stream is a direct dashboard input, so it is delivered through Git LFS.

The following generated output paths are therefore excluded by `.gitignore`:

```text
A2/data/generated/*
A2/data/events/* except event_stream.jsonl
A2/data/dev/
```

The small source cache
`A2/data/generated/aemo_cache/PRICE_AND_DEMAND_202610_NSW1.csv` is the only exception
under `A2/data/generated/`. It is tracked so that every group member uses the same AEMO
price input and can reproduce the same assessment CSV and JSONL outputs without a new
download. The generator automatically reuses this file when it is present.

Small integration fixtures, connector mappings and quality summaries remain in ordinary
Git. The 140.3 MiB populated A2 DuckDB, empty replay seed and complete event stream are
tracked with Git LFS. The generated CSV files must still be included in the final course
submission package. Their reproducibility hashes are recorded in
`A2/data/quality/generation_checksums.csv`; the event-stream hash is recorded in
`A2/data/quality/event_preparation_summary.csv`. The ignored
`A2/data/generated/generation_manifest.json` contains the effective generator configuration
and the generated-output hashes.

### Regenerating the assessment JSONL stream

Run A's complete deterministic batch workflow from the repository root:

```powershell
python A2/run_member_a.py
```

This calls the station export, synthetic generator, event preparation and demo projection
in order. Its main JSONL outputs are:

```text
A2/data/events/event_stream.jsonl       # complete ordered assessment stream
A2/data/demo/event_stream.jsonl         # one short complete real session
```

If the generated CSV files already exist, rebuild only the complete ordered stream with:

```powershell
python A2/src/events/prepare.py --project-root .
```

`A2/src/events/prepare.py` reads `prices_hourly.csv`, `sessions.csv` and
`session_metering.csv`, validates their relationships, orders the four event types and
writes `A2/data/events/event_stream.jsonl`.

Rebuild only the short demo from an existing complete stream with:

```powershell
python A2/src/events/make_demo.py --project-root .
```

`A2/src/events/make_demo.py` projects one complete generated session and the applicable
station price events into `A2/data/demo/event_stream.jsonl`.

### Regenerating development JSONL files

Use the small development profile without overwriting the assessment output tree:

```powershell
python A2/run_member_a.py --profile dev
```

This writes:

```text
A2/data/dev/events/event_stream.jsonl
A2/data/dev/demo/event_stream.jsonl
```

### MQTT received JSONL evidence

Files such as `mqtt_smoke_received.jsonl` and `mqtt_demo_received.jsonl` are transport
evidence rather than prepared event streams. They are created by
`A2/tests/mqtt_smoke.py` and contain both the MQTT topic and received event:

```powershell
python A2/tests/mqtt_smoke.py --stream A2/data/demo/event_stream.jsonl --report-prefix mqtt_demo
```

`A2/data/examples/short_event_stream.jsonl` is a manually maintained shared fixture; it
is not generated by the assessment pipeline. `A2/src/publishing/run.py` also does not
generate JSONL files: it reads an existing stream and publishes it to MQTT.
