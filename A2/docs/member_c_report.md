# Event Storage and Integration with Assignment 1 Database

Member C created a separate Assignment 2 DuckDB by copying the read-only Assignment 1
database, verifying its SHA-256 fingerprint before and after initialisation, and adding
five streaming tables. `station_connector_mapping` links each simulated station-local
connector to its original A1 charger and plug. `mqtt_events` retains every raw receipt for
audit and event-ID deduplication. `price_events` and `meter_events` provide normalised
time-series data, while one `charging_sessions` row represents the complete start-to-end
lifecycle of a session.

Primary keys, unique constraints, reference checks and secondary indexes protect event
identity and support station/time and session/time access. Five analytical views expose
latest prices, active sessions, latest meter readings, current metrics and historical
metrics. Repository transactions commit the raw event and its normalised change together;
invalid references, regressing telemetry or failed normalisation roll back the complete
event. A documented 0.011 kWh tolerance accommodates the supplied generator's difference
between three-decimal meter readings and two-decimal session summaries.

The group selected a one-day assessment window covering all 414 eligible DC stations and
1,610 connectors. The complete 172,803-event stream was imported through the same
`EventRepository` validation boundary used by the MQTT subscriber, without the 0.1-second
network pacing. It produced 7,038 price rows, 5,084 completed sessions and 155,597 meter
rows. A separate real MQTT demo verified ordered delivery, acknowledgements, duplicate
handling, restart recovery and persistence for 36 messages.

Final reconciliation found zero duplicate event IDs or sequence numbers, unknown stations,
orphan meters, active sessions, identity mismatches, price mismatches or session-result
mismatches. All five views and the dashboard queries executed successfully. The populated
database reports 129,422.20 kWh total energy, AUD 88,908.69 revenue, a 30.61-minute average
charge and 183.4 kW maximum peak. The compacted DuckDB is approximately 140.3 MiB.

The complete paced MQTT replay would require at least 4.8 hours and was not used to build
the final database; instead, full repository reconciliation and a representative real MQTT
test provide complementary storage and transport evidence. Broker-outage persistence and
sustained long-running throughput remain future improvements.

## Member C contribution

Member C designed and initialised the A2 DuckDB schema, implemented transactional and
idempotent event persistence, added full-stream import and dashboard query interfaces,
created analytical views, reconciled the populated database with the prepared stream, and
documented the database design, validation evidence and operational limitations.
