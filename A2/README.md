# COMP5339 Assignment 2 pipeline scaffold

This directory is a collaboration scaffold modelled on Assignment 1. It defines stages,
ownership, paths and cross-member interfaces without supplying the four members'
assignment implementations. The A1 database remains read-only.

## What is provided

- a configuration-driven runner and declared stage inputs/outputs;
- the professor-provided generator, retained unchanged;
- development and assessment configuration profiles;
- versioned JSON message examples for parallel work;
- CLI stubs for every team-owned stage;
- B/C/D interface signatures, without implementation logic;
- team contracts and expected test ownership.

The stubs deliberately return a non-zero exit status until their owner implements them.
This prevents an unimplemented stage from appearing successful.

## Ownership and structure

```text
A2/
  config/                  Pipeline, generator profiles and MQTT settings
  external/data_generator/ Professor-provided files
  data/examples/            Shared message-contract fixtures
  src/generation/           A: station export and generator wrapper TODOs
  src/events/               A: ordered event preparation TODO
  src/publishing/           A: MQTT publisher TODO
  src/subscription/         B: subscriber/state interface and TODO
  src/database/             C: repository interface and database TODO
  sql/a2_schema.sql         C: documented DDL placeholder, no solution SQL
  src/dashboard/            D: dashboard/runtime interfaces and TODO
  tests/                    Per-owner test checklist
  docs/team_contracts.md    Stable hand-off contracts
  run_pipeline.py           Working shared orchestrator
  validate_pipeline.py      C/shared validation CLI stub
```

The authoritative task split and method signatures are in
`docs/team_contracts.md`.

## Files each member should implement

### Member A — data generation and MQTT publishing

Member A should modify:

```text
A2/src/generation/export_stations.py
A2/src/generation/run.py
A2/src/events/prepare.py
A2/src/publishing/run.py
```

Member A may add generation/publishing tests under `A2/tests/` and maintains the JSON
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
A2/data/quality/generation_summary.csv
A2/data/quality/event_preparation_summary.csv
```

### Member B — MQTT subscription and live state

Member B should modify:

```text
A2/src/subscription/run.py
A2/src/subscription/event_processor.py
A2/src/subscription/state_store.py
```

Member B may add subscriber/state tests under `A2/tests/`. B should use C's
`EventRepository` interface and must not place DuckDB SQL inside the subscription
module. B owns message validation, price/session state transitions, occupancy and the
live meter-curve data supplied to D.

### Member C — DuckDB storage and analytics

Member C should modify:

```text
A2/src/database/initialise.py
A2/src/database/repository.py
A2/sql/a2_schema.sql
A2/validate_pipeline.py
```

Member C may add storage/analytics tests under `A2/tests/`. C owns the final integrated
DuckDB, DDL, idempotent event-write interface, current/historical queries and final
reconciliation report. C must create a separate A2 database and must not write to the
A1 database.

Expected outputs:

```text
A2/data/final/ev_chargers_a2.duckdb
A2/data/quality/database_initialisation.csv
A2/data/quality/final_validation.csv
```

### Member D — map dashboard and live curve

Member D should modify:

```text
A2/src/dashboard/runtime.py
A2/src/dashboard/run.py
A2/requirements.txt              # add the selected dashboard framework
```

Member D may add dashboard tests and static UI assets under `A2/tests/` and a new
`A2/src/dashboard/assets/` directory. D should consume B's `StateStore` methods and C's
`EventRepository` queries instead of duplicating message parsing or SQL.

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

From the repository root with Python 3.10 or newer:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r A1/requirements.txt
python -m pip install -r A2/requirements.txt
```

Each member must add any dependency introduced by their implementation. Member A owns
the final merged `requirements.txt` check.

## Inspect the pipeline

This command works now and shows every stage and owner:

```powershell
python A2/run_pipeline.py --list-stages
```

The intended development pipeline is:

```powershell
python A2/run_pipeline.py --stage prepare-dev
```

It will become runnable as A and C complete the declared stages. The intended assessment
pipeline is:

```powershell
python A2/run_pipeline.py --stage prepare
```

Both aliases currently stop at the first team-owned TODO, by design.

## Member entry points

### A — generation, event preparation and publishing

```powershell
python A2/run_pipeline.py --stage station-export-dev
python A2/run_pipeline.py --stage synthetic-generation-dev
python A2/run_pipeline.py --stage event-preparation
python A2/run_pipeline.py --stage publisher
```

### B — subscription and live state

```powershell
python A2/run_pipeline.py --stage subscriber
```

Implement the signatures in `subscription/state_store.py` and
`subscription/event_processor.py` without embedding C's SQL.

### C — DuckDB storage and analytics

```powershell
python A2/run_pipeline.py --stage database
python A2/run_pipeline.py --stage validate
```

Implement `database/repository.py`, `sql/a2_schema.sql` and validation. Keep A1 unchanged.

### D — map dashboard and live curve

```powershell
python A2/run_pipeline.py --stage dashboard
```

Select a Python framework, implement `dashboard/runtime.py` and `dashboard/run.py`, and
consume B's state interface plus C's repository interface.

## Shared fixture

`data/examples/short_event_stream.jsonl` provides one agreed scenario:

```text
price update -> session start -> meter -> meter -> session end
```

A maintains the contract; B, C and D can use it immediately without waiting for full
generation or MQTT deployment.

## Configuration profiles

- `generator.dev.json`: proposed small offline settings for integration development.
- `generator.assessment.json`: proposed full settings requiring AEMO input/cache.

The group must confirm the assessed station scope and runtime before producing final
data. Full-scale generation may result in millions of meter messages, while publishing
must retain at least a 0.1-second delay.

## Large generated files intentionally not tracked

The repository does not track the full generated assessment dataset. These files are
derived outputs, are reproducible from the committed code and inputs, and are too large
for an ordinary GitHub workflow. In the current assessment run, the ordered JSONL stream
is approximately 973 MiB and `session_metering.csv` is approximately 211 MiB before
compression.

The following generated output paths are therefore excluded by `.gitignore`:

```text
A2/data/generated/*
A2/data/events/
A2/data/dev/
```

The small source cache
`A2/data/generated/aemo_cache/PRICE_AND_DEMAND_202610_NSW1.csv` is the only exception
under `A2/data/generated/`. It is tracked so that every group member uses the same AEMO
price input and can reproduce the same assessment CSV and JSONL outputs without a new
download. The generator automatically reuses this file when it is present.

Small integration fixtures, connector mappings, quality summaries and the initialised
A2 DuckDB remain tracked. If the full generated dataset or populated DuckDB is required
for final assessment, package it separately through the course submission system or Git
LFS and record its SHA-256 digest. Do not add these large files to ordinary Git history.

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
