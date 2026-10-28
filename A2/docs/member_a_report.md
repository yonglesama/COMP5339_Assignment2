# Member A report sections

## Synthetic Data Generation

We exported 414 stations with known positive DC power from the Assignment 1 DuckDB database, covering 1,610 plugs. The original station_id is retained for database joins. DC plug counts are summed at each station, and the highest-rated charger supplies its representative location and maximum power. A separate mapping links each simulated connector to its A1 charger and plug number. Urban and regional labels use the configured SA4 grouping.

The supplied generator was kept unchanged. We generated 14 days of data starting on 1 October 2026, using AEMO NSW1 wholesale prices, seed 5339 and one-minute meter readings. The output contains 139,104 hourly prices, 73,860 sessions and 2,234,930 meter readings. Wholesale prices are observed inputs; retail tariffs, sessions and meter readings are simulated. Sessions starting near the boundary retain their later end events.

## MQTT Publishing

We convert the CSV records into price_update, session_start, meter and session_end messages. Each JSON message includes its event ID, sequence number, event time and A1 station_id. Session messages include session_id and the station-local connector_id. Prices use AUD/kWh, and meter energy is cumulative kWh within a session. The session-end payload distinguishes charging completion from unplugging so that occupancy can remain active during the idle period.

The preparation script sorts messages by event time. Ties follow price update, session start, meter and session end order. It emits the initial price and subsequent retail price changes, removing 42,642 unchanged hourly records. The final stream contains 2,479,112 messages. The publisher sends them to comp5339/ev/{station_id}/{event_type} with QoS 1, waits for acknowledgement and leaves at least 0.1 seconds between messages. Downstream consumers use event_id to reject duplicate deliveries.

## Validation, limitations and improvements

All 17 unit tests passed, and the complete event file was checked for ordering, references and complete sessions. Real MQTT tests delivered a five-message fixture and a 32-message generated demonstration in order; the measured minimum publishing interval was about 0.109 seconds. Full MQTT replay was not performed: it requires at least 68.864 hours. The short stream supports live integration but does not represent full historical activity. A station-wide maximum power may overestimate lower-rated plugs at mixed-power sites. The generator uses naive simulation timestamps without daylight-saving conversion. Per-connector power settings and resumable publishing would improve the model and long runs.

## Member A contribution

Member A implemented station export, synthetic data generation, event preparation and MQTT publishing, tested the outputs, and prepared the data, interfaces and operating instructions.

## References

- [AEMO NSW1 price and demand, October 2026](https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_202610_NSW1.csv).
