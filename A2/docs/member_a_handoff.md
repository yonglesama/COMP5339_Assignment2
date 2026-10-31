# MQTT Message Handoff

## Files for Member B

Paths are relative to the project root that contains `A1/` and `A2/`.

| File | Contents |
|---|---|
| `A2/data/examples/` | Examples of all four JSON message types |
| `A2/data/demo/event_stream.jsonl` | 36 messages containing price changes and one complete charging session for subscription and state-processing tests |
| `A2/data/events/event_stream.jsonl` | A complete one-day stream of 172,803 messages ordered by event time |

## MQTT connection

The default broker is `localhost:1883`, and the subscriber listens to
`comp5339/ev/#`. For testing across computers, both publisher and subscriber
must use the same reachable broker address.

Topic format: `comp5339/ev/{station_id}/{event_type}`.

The message body is JSON. Common fields are `schema_version`, `event_id`,
`sequence_number`, `event_type`, `event_time`, `event_timezone`, `station_id`,
and `payload`.

| Message | Meaning | JSON example |
|---|---|---|
| `price_update` | `payload.price_per_kwh`: current station price in AUD/kWh | [price_update.json](../data/examples/price_update.json) |
| `session_start` | `payload.session_id`, `connector_id`, and `price_per_kwh`: starts a session and locks its price | [session_start.json](../data/examples/session_start.json) |
| `meter` | `voltage_v`, `current_a`, `power_kw`, `energy_kwh`, and `soc_pct` for the same `session_id`: updates the meter state | [meter.json](../data/examples/meter.json) |
| `session_end` | `charge_end_time` stops charging; `end_time` records unplugging and releases occupancy | [session_end.json](../data/examples/session_end.json) |

`event_time` is an ISO timestamp and `event_timezone` is `Australia/Sydney`.
The generator models a fixed 24-hour period without daylight-saving conversion.
Meter units are V, A, kW, kWh, and percent. `energy_kwh` is the session's
cumulative energy.

The demo stream has non-contiguous `sequence_number` values and must be
processed in file order. Use `event_id` to deduplicate redelivered messages.
Clear test state before replaying the sample if the complete state transition
must be observed again. Consecutive sessions on the same connector can hand
over in the same second; on completion, remove the old session by `session_id`.

## Expected result for the 36-message demonstration

Start with an empty subscriber state, subscribe first, and then publish
`A2/data/demo/event_stream.jsonl`.

| Item | Expected value |
|---|---|
| Station / session | `ST4D056616A3` / `ST4D056616A3-4-202610010201` |
| Message count | 36: 2 `price_update`, 1 `session_start`, 32 `meter`, and 1 `session_end` |
| Latest price | 0.75 -> 0.74 AUD/kWh |
| Active session count | 0 -> 1 -> 0 |
| Session meter data | 32 sample points received in timestamp order |
| Energy / cost in the end event | 22.60 kWh / AUD 16.72 |

## Join keys for Member C

A1 database: `A1/data/final/ev_chargers.duckdb`.

- `station_id` matches `A1.stations.station_id`; station records are in `A2/data/input/generator_stations.csv`.
- `connector_id` is a simulated connector number within a station. Use `(station_id, connector_id)` in `A2/data/input/station_connector_mapping.csv` to find A1's `charger_id` and `plug_number`.

The dataset includes 414 DC stations with known positive power and 1,610
connectors. AC chargers and records with missing power are excluded. Each
station is modelled using its maximum DC power; two stations contain chargers
with different power ratings.
