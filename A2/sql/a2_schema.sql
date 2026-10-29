-- COMP5339 Assignment 2, Member C: streaming-event storage extension.
--
-- Run this schema only against A2/data/final/ev_chargers_a2.duckdb after it
-- has been rebuilt from the read-only A1 DuckDB. The copied database already
-- contains stations, chargers and the other A1 reference tables used below.

-- A's station-local connector identifiers are mapped back to the A1 charger
-- and physical plug from which each simulated connector was derived.
CREATE TABLE station_connector_mapping (
    station_id VARCHAR NOT NULL,
    connector_id INTEGER NOT NULL CHECK (connector_id > 0),
    charger_id VARCHAR NOT NULL,
    plug_number INTEGER NOT NULL CHECK (plug_number > 0),
    source_rating_kw DOUBLE NOT NULL CHECK (source_rating_kw > 0),
    PRIMARY KEY (station_id, connector_id),
    UNIQUE (charger_id, plug_number),
    FOREIGN KEY (station_id) REFERENCES stations (station_id),
    FOREIGN KEY (charger_id) REFERENCES chargers (charger_id)
);

-- Immutable raw receipt of each unique MQTT event. event_id is the
-- idempotency key used to reject QoS 1 redelivery before normalised state is
-- changed. The original payload is retained for audit and reconciliation.
CREATE TABLE mqtt_events (
    event_id VARCHAR PRIMARY KEY,
    topic VARCHAR NOT NULL CHECK (length(topic) > 0),
    schema_version VARCHAR NOT NULL CHECK (schema_version = '1.0'),
    sequence_number BIGINT NOT NULL CHECK (sequence_number > 0),
    event_type VARCHAR NOT NULL CHECK (
        event_type IN ('price_update', 'session_start', 'meter', 'session_end')
    ),
    event_time TIMESTAMP NOT NULL,
    event_timezone VARCHAR NOT NULL CHECK (event_timezone = 'Australia/Sydney'),
    station_id VARCHAR NOT NULL,
    payload JSON NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES stations (station_id)
);

-- Normalised station tariff history. Wholesale prices may be negative in the
-- electricity market, so only the retail tariff is constrained non-negative.
CREATE TABLE price_events (
    event_id VARCHAR PRIMARY KEY,
    station_id VARCHAR NOT NULL,
    event_time TIMESTAMP NOT NULL,
    price_per_kwh DOUBLE NOT NULL CHECK (price_per_kwh >= 0),
    wholesale_per_kwh DOUBLE NOT NULL,
    currency VARCHAR NOT NULL CHECK (currency = 'AUD'),
    UNIQUE (station_id, event_time),
    FOREIGN KEY (event_id) REFERENCES mqtt_events (event_id),
    FOREIGN KEY (station_id) REFERENCES stations (station_id)
);

-- One mutable row represents the lifecycle of a charging session. A start
-- event inserts the row; its matching end event fills every nullable result
-- column in one update. A NULL end_event_id therefore means the session is
-- active. price_per_kwh is the tariff locked at session start.
CREATE TABLE charging_sessions (
    session_id VARCHAR PRIMARY KEY,
    start_event_id VARCHAR NOT NULL UNIQUE,
    end_event_id VARCHAR UNIQUE,
    station_id VARCHAR NOT NULL,
    connector_id INTEGER NOT NULL CHECK (connector_id > 0),
    start_time TIMESTAMP NOT NULL,
    charge_end_time TIMESTAMP,
    end_time TIMESTAMP,
    site_type VARCHAR NOT NULL CHECK (site_type IN ('urban', 'regional')),
    vehicle_model VARCHAR NOT NULL CHECK (length(vehicle_model) > 0),
    price_per_kwh DOUBLE NOT NULL CHECK (price_per_kwh >= 0),
    charge_minutes DOUBLE CHECK (charge_minutes >= 0),
    energy_kwh DOUBLE CHECK (energy_kwh >= 0),
    avg_kw DOUBLE CHECK (avg_kw >= 0),
    peak_kw DOUBLE CHECK (peak_kw >= 0),
    cost_aud DOUBLE CHECK (cost_aud >= 0),
    FOREIGN KEY (start_event_id) REFERENCES mqtt_events (event_id),
    FOREIGN KEY (end_event_id) REFERENCES mqtt_events (event_id),
    FOREIGN KEY (station_id, connector_id)
        REFERENCES station_connector_mapping (station_id, connector_id),
    CHECK (
        (
            end_event_id IS NULL
            AND charge_end_time IS NULL
            AND end_time IS NULL
            AND charge_minutes IS NULL
            AND energy_kwh IS NULL
            AND avg_kw IS NULL
            AND peak_kw IS NULL
            AND cost_aud IS NULL
        )
        OR
        (
            end_event_id IS NOT NULL
            AND charge_end_time IS NOT NULL
            AND end_time IS NOT NULL
            AND charge_minutes IS NOT NULL
            AND energy_kwh IS NOT NULL
            AND avg_kw IS NOT NULL
            AND peak_kw IS NOT NULL
            AND cost_aud IS NOT NULL
            AND start_time < charge_end_time
            AND charge_end_time <= end_time
        )
    )
);

-- Meter telemetry for live and historical charging curves. session_id is
-- deliberately a logical relationship rather than a physical foreign key:
-- DuckDB otherwise prevents updating an active charging_sessions row after
-- meter rows reference it. EventRepository and final validation enforce that
-- the session exists and that station/connector values agree.
CREATE TABLE meter_events (
    event_id VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    station_id VARCHAR NOT NULL,
    connector_id INTEGER NOT NULL CHECK (connector_id > 0),
    event_time TIMESTAMP NOT NULL,
    voltage_v DOUBLE NOT NULL CHECK (voltage_v >= 0),
    current_a DOUBLE NOT NULL CHECK (current_a >= 0),
    power_kw DOUBLE NOT NULL CHECK (power_kw >= 0),
    energy_kwh DOUBLE NOT NULL CHECK (energy_kwh >= 0),
    soc_pct DOUBLE NOT NULL CHECK (soc_pct BETWEEN 0 AND 100),
    UNIQUE (session_id, event_time),
    FOREIGN KEY (event_id) REFERENCES mqtt_events (event_id),
    FOREIGN KEY (station_id, connector_id)
        REFERENCES station_connector_mapping (station_id, connector_id)
);

-- Secondary indexes support event reconciliation and the repository's main
-- station/session time-series access paths. Primary, UNIQUE and foreign-key
-- constraints create their own supporting ART indexes in DuckDB.
CREATE INDEX idx_mqtt_events_sequence
    ON mqtt_events (sequence_number);
CREATE INDEX idx_mqtt_events_station_time
    ON mqtt_events (station_id, event_time);
CREATE INDEX idx_price_events_station_time
    ON price_events (station_id, event_time);
CREATE INDEX idx_charging_sessions_station_time
    ON charging_sessions (station_id, start_time);
CREATE INDEX idx_meter_events_session_time
    ON meter_events (session_id, event_time);

-- Latest tariff for each station. event_id breaks ties deterministically even
-- though the table also prevents two prices for one station at the same time.
CREATE VIEW latest_station_prices AS
SELECT
    event_id,
    station_id,
    event_time,
    price_per_kwh,
    wholesale_per_kwh,
    currency
FROM price_events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY station_id
    ORDER BY event_time DESC, event_id DESC
) = 1;

CREATE VIEW active_charging_sessions AS
SELECT *
FROM charging_sessions
WHERE end_event_id IS NULL;

-- Most recent sample in each session is used for live power and SoC displays.
CREATE VIEW latest_session_meter_events AS
SELECT
    event_id,
    session_id,
    station_id,
    connector_id,
    event_time,
    voltage_v,
    current_a,
    power_kw,
    energy_kwh,
    soc_pct
FROM meter_events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_id
    ORDER BY event_time DESC, event_id DESC
) = 1;

-- Single-row current metrics for the repository/dashboard. Sessions without a
-- meter sample remain active but contribute zero to current power.
CREATE VIEW current_charging_metrics AS
SELECT
    COUNT(*) AS active_session_count,
    COUNT(DISTINCT s.station_id) AS active_station_count,
    COALESCE(SUM(m.power_kw), 0.0) AS current_power_kw
FROM active_charging_sessions AS s
LEFT JOIN latest_session_meter_events AS m
    ON m.session_id = s.session_id;

-- Single-row completed-session metrics for historical reporting.
CREATE VIEW historical_charging_metrics AS
SELECT
    COUNT(*) AS completed_session_count,
    COUNT(DISTINCT station_id) AS serviced_station_count,
    COALESCE(SUM(energy_kwh), 0.0) AS total_energy_kwh,
    COALESCE(SUM(cost_aud), 0.0) AS total_revenue_aud,
    COALESCE(AVG(charge_minutes), 0.0) AS average_charge_minutes,
    COALESCE(MAX(peak_kw), 0.0) AS maximum_peak_kw
FROM charging_sessions
WHERE end_event_id IS NOT NULL;
