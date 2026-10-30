# Member B: subscription and live state

Originally implemented against main snapshot 7bb38f2 (2026-10-29). The B implementation
and tests were refreshed for the team-approved one-day stream and populated C database;
B tests and MQTT evidence continue to use disposable databases and do not change the
assessment database.

## Setup and standalone run

From the repository root, use Python 3.11+ and install `A2/requirements.txt`. The optional local broker and transport test also require `A2/requirements-dev.txt`.

1. Ensure C's initialised database exists at `A2/data/final/ev_chargers_a2.duckdb`. If it does not, use `python A2/run_pipeline.py --stage database`. **That command rebuilds the database and removes existing event history; do not use it to resume.**
2. Terminal 1: `python A2/src/publishing/local_broker.py`
3. Terminal 2: `python A2/src/subscription/run.py --project-root .`
4. Wait for `Subscribed to comp5339/ev/#; ready for publisher`.
5. Terminal 3: `python A2/src/publishing/run.py --project-root . --stream A2/data/demo/event_stream.jsonl --delay 1`
6. Ctrl+C stops B and closes its repository. Restarting B restores latest prices, active sessions and their meter curves from C before subscribing.

CLI overrides: `--database PATH`, `--config PATH`, `--host HOST`, `--port PORT`. Database overrides allow disposable integration databases. B never automatically creates, rebuilds or clears a database. Host defaults to the shared MQTT configuration. Optional TLS and MQTT_USERNAME/MQTT_PASSWORD follow A's publisher conventions. C currently requires the prefix `comp5339/ev`, so B rejects a different prefix rather than accepting messages C cannot store.

## Event processing

JSON decode (reject duplicate keys) -> shared v1 event validation -> exact topic/envelope match -> repository.has_event -> repository.store_event -> StateStore.apply_event only on True. Calls are serialised to avoid duplicate delivery races. C validates station/connector references, session chronology and cumulative meter progression transactionally; B contains no SQL. Sparse sequence numbers are accepted. Event IDs, rather than consecutive sequence numbers, drive deduplication.

Price updates replace the price (an older price cannot regress the display). Session starts create session-keyed records. Meter energy is cumulative and replaces the latest reading; points are appended to the session curve. Session ends remove only that session, so the old end cannot clear a new same-second session on the same connector. Occupancy counts distinct connectors, separately from active session count.

The service requests persistent MQTT 3.1.1 sessions, resubscribes on reconnect, and uses manual acknowledgements. Valid events and duplicates are acknowledged after processing. Malformed JSON/envelopes/topics are logged, counted and deliberately acknowledged as rejected. Repository failures (including invalid references/transitions) or unexpected state failures stop the service without acknowledging that message. Check service.error, correct the cause and restart. No automatic dead-letter store is implemented. Delivery while disconnected depends on broker persistence/QoS; the local development broker is not a durable production broker.

## D integration: one owner process

Do not run the standalone B subscriber while D holds the same writable DuckDB. D should compose these objects:

```python
from database.repository import EventRepository
from subscription.run import SubscriptionService
from subscription.state_store import StateStore

repository = EventRepository(database_path)
state_store = StateStore()
subscriber = SubscriptionService(mqtt_config, repository, state_store)
subscriber.start()  # restores state, starts MQTT loop, waits for SUBACK
# D reads state_store and repository; display failure if subscriber.failed.is_set().
# On shutdown, in this order:
subscriber.stop()
repository.close()
```

State methods return independent snapshots under a lock. Time values are ISO seconds under A's v1 naive Australia/Sydney simulation convention; B does not reinterpret DST or convert to UTC.

- `get_station_state(station_id)`: station_id, price_per_kwh (None if not received), price_event_time, currency, active_sessions, active_session_count, occupied_connector_ids, occupied_connector_count, occupancy (bool).
- `get_all_station_states()`: snapshots for stations observed/restored, sorted by ID. For all 414 map locations, use C's `get_dashboard_stations()` and join by station_id. A station with no events has an empty snapshot.
- `get_active_sessions()`: records with session_id, station_id, connector_id, start_time, site_type, vehicle_model, locked price_per_kwh and latest_meter_time. Voltage/current/power/energy/SoC fields appear after telemetry; restored records may contain None before telemetry.
- `get_session_curve(session_id)`: chronological point dictionaries with event_time, event_id, station_id, session_id, connector_id, voltage_v, current_a, power_kw, energy_kwh, soc_pct.
- `get_current_metrics()`: active_session_count, active_station_count, occupied_connector_count. Use C for historical metrics and current_power_kw.

Only active-session curves live in memory. An ended session's curve becomes `[]`; retrieve completed curves via C's `get_session_meter_events()`. No unbounded in-memory event-ID set is retained; C is the durable deduplication authority. `StateStore.apply_event()` is an internal committed-event boundary; D must not call it directly. Create/start the service before using EventProcessor against a populated repository so the live state is restored first.

## Verification

```bash
python -m unittest discover -s A2/tests -v
python A2/run_pipeline.py --stage verify-abc
```

B's 12 tests create a fresh initialised database in a temporary directory and use the real C repository. They cover complete demo processing, duplicates/races, malformed messages, topic mismatch, write failure, orphan-meter rollback, restarts, copied snapshots, multiple connectors, same-second handover and ACK behaviour. The populated assessment database is never changed by these tests.

The separate real loopback MQTT test stores 36 events, restarts mid-session and restores 7 curve points, ignores 36 redeliveries, rejects one malformed message and verifies one completed historical session (22.60 kWh, AUD 16.72), with zero final active sessions. Tests use disposable database copies. The complete 172,803-event one-day stream is imported separately through C's repository and reconciled with the final database; broker outage recovery and sustained full-scale throughput are not covered by the MQTT demo.
