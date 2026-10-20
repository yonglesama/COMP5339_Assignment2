# A2 team contracts

These interfaces implement the agreed four-person task split. A contract change must
be reviewed by every downstream owner before it is merged.

## Ownership

| Member | Owned code | Required output |
|---|---|---|
| A | `src/generation`, `src/events`, `src/publishing` | Generated data, JSON contract/examples, ordered stream, MQTT publisher |
| B | `src/subscription` | MQTT parsing, validation, live station/session state and curve interface |
| C | `src/database`, `sql`, `validate_pipeline.py` | DDL, event repository, metrics/queries, final DuckDB and validation |
| D | `src/dashboard` | Map, marker details, live curve, current/historical metrics and screenshot |
| Shared | `run_pipeline.py`, `config/pipeline.json`, integration tests | End-to-end assembly and demo |

Every member owns their tests, report section and demo explanation. Ownership does not
remove the requirement that everyone understand the full submission.

## Common executable contract

Every executable accepts `--project-root PATH`. Paths in `config/pipeline.json` are
relative to the repository root containing `A1/` and `A2/`. Batch stages return non-zero
on failure and create all declared outputs. MQTT and dashboard services run until stopped.

## A: generator and event contract

`data/input/generator_stations.csv` has exactly:

```text
station_id,name,lat,lon,site_type,connectors,max_kw
```

The event entity is A1 `station_id`; `connector_id` identifies capacity within a station.
Member A owns envelope version 1.0 and the fixtures in `data/examples/`:

```json
{
  "schema_version": "1.0",
  "event_id": "evt_...",
  "sequence_number": 1,
  "event_type": "price_update",
  "event_time": "2026-10-01T00:00:00",
  "event_timezone": "Australia/Sydney",
  "station_id": "ST...",
  "payload": {}
}
```

Rules:

- Events are ordered by time, then price update, session start, meter and session end.
- `event_id` is stable and globally unique; `sequence_number` strictly increases.
- Session payloads contain `session_id` and `connector_id`.
- Only the first price and actual price changes are published.
- MQTT topic: `comp5339/ev/{station_id}/{event_type}`.
- Published messages are separated by at least 0.1 seconds.

## B: subscription and live-state contract

Member B owns:

- `EventProcessor`: validates messages, controls transitions and calls C's sink;
- `StateStore`: thread-safe latest price, occupancy, active sessions and meter curves;
- MQTT callbacks and headless subscriber runner.

Stable interface for D:

```python
state_store.get_station_state(station_id)
state_store.get_all_station_states()
state_store.get_active_sessions()
state_store.get_session_curve(session_id)
state_store.get_current_metrics()
```

B does not issue SQL. It receives an injected event sink implementing `has_event()` and
`store_event()`. A valid new event is committed by C first and then applied to B's memory
state. Duplicate `event_id` values update neither database nor state.

## C: storage and analytics contract

Member C owns `database.repository.EventRepository`, the only write/query interface:

```python
repository.has_event(event_id)
repository.store_event(topic, event)
repository.get_dashboard_stations()
repository.get_latest_prices()
repository.get_active_sessions()
repository.get_session_meter_events(session_id)
repository.get_current_metrics()
repository.get_historical_metrics()
```

`data/final/ev_chargers_a2.duckdb` is rebuilt from a copy of A1; A1 remains read-only.
C maintains `mqtt_events`, `price_events`, `charging_sessions`, `meter_events` and their
analytical views. Raw and normalised writes use one transaction. `event_id` is the
idempotency key.

`meter_events.session_id` is checked by repository logic and final validation rather
than a physical foreign key. DuckDB otherwise prevents updating an active session after
referencing meter rows exist.

## D: dashboard contract

D selects the Python framework and implements `src/dashboard/run.py`. Create exactly one
`dashboard.runtime.DashboardRuntime`, which exposes:

```python
runtime.state_store   # B's live state and curves
runtime.repository    # C's static details and metrics
runtime.start()
runtime.stop()
```

The dashboard process owns the MQTT background loop and DuckDB connection. Do not run
the standalone subscriber at the same time. D displays the A1 name/address/operators/
plug types, latest price, occupancy, active-session curve, one current metric and one
historical metric. D does not parse MQTT payloads or duplicate C's SQL.

## Parallel-development fixture

`data/examples/short_event_stream.jsonl` is the shared scenario:

```text
price change -> session start -> meter -> meter -> session end
```

A maintains it; B uses it for transitions, C for storage/analytics, and D for mock UI.
No member needs to wait for a full generator or broker run to begin development.

## Configuration profiles

- `generator.dev.json`: 10 stations, one day, static prices, two-minute meters.
- `generator.assessment.json`: full configured scope, 14 days, AEMO prices, one-minute meters.

The full export currently has 414 stations and 1,610 connectors. It may produce roughly
two million meter messages and require more than 60 hours at 0.1 seconds per message.
The group must confirm whether the assessed run uses all stations or a justified subset.

## Decisions still requiring the whole group

- Assessed station scope and replay duration.
- Acceptance of the configured SA4 urban/regional classification.
- MQTT broker deployment and credentials.
- Dashboard framework and refresh interval.
- Exact current and historical metrics shown in the report.
