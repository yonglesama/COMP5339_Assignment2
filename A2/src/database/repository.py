from __future__ import annotations

from datetime import datetime
import json
import math
from pathlib import Path
from threading import RLock
from typing import Any

import duckdb

from events.contract import validate_event


TOPIC_PREFIX = "comp5339/ev"


class EventRepository:
    """Transactional DuckDB event sink used by B and query boundary used by D."""

    def __init__(self, database_path: Path, *, read_only: bool = False) -> None:
        self.database_path = Path(database_path).resolve()
        self.read_only = read_only
        if not self.database_path.is_file():
            raise FileNotFoundError(f"A2 database does not exist: {self.database_path}")
        self._lock = RLock()
        self._connection: duckdb.DuckDBPyConnection | None = duckdb.connect(
            str(self.database_path), read_only=read_only
        )

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    def _require_connection(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            raise RuntimeError("EventRepository is closed")
        return self._connection

    @staticmethod
    def _require_event_id(event_id: str) -> None:
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("event_id must be a non-empty string")

    @staticmethod
    def _expected_topic(event: dict[str, Any]) -> str:
        return f"{TOPIC_PREFIX}/{event['station_id']}/{event['event_type']}"

    @staticmethod
    def _event_exists(connection: duckdb.DuckDBPyConnection, event_id: str) -> bool:
        return (
            connection.execute(
                "SELECT 1 FROM mqtt_events WHERE event_id = ? LIMIT 1", [event_id]
            ).fetchone()
            is not None
        )

    def has_event(self, event_id: str) -> bool:
        self._require_event_id(event_id)
        with self._lock:
            return self._event_exists(self._require_connection(), event_id)

    def store_event(self, topic: str, event: dict[str, Any]) -> bool:
        if self.read_only:
            raise PermissionError("Cannot store events through a read-only repository")

        event_time, _ = validate_event(event)
        expected_topic = self._expected_topic(event)
        if topic != expected_topic:
            raise ValueError(f"Topic/event mismatch: expected {expected_topic!r}, received {topic!r}")

        with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN TRANSACTION")
            try:
                if self._event_exists(connection, event["event_id"]):
                    connection.execute("ROLLBACK")
                    return False

                self._insert_raw_event(connection, topic, event)
                kind = event["event_type"]
                if kind == "price_update":
                    self._store_price_event(connection, event, event_time)
                elif kind == "session_start":
                    self._store_session_start(connection, event, event_time)
                elif kind == "meter":
                    self._store_meter_event(connection, event, event_time)
                elif kind == "session_end":
                    self._store_session_end(connection, event, event_time)
                else:  # validate_event currently makes this unreachable.
                    raise ValueError(f"Unsupported event type: {kind}")

                connection.execute("COMMIT")
                return True
            except Exception:
                try:
                    connection.execute("ROLLBACK")
                except duckdb.Error:
                    pass
                raise

    @staticmethod
    def _insert_raw_event(
        connection: duckdb.DuckDBPyConnection,
        topic: str,
        event: dict[str, Any],
    ) -> None:
        connection.execute(
            """INSERT INTO mqtt_events (
                   event_id, topic, schema_version, sequence_number, event_type,
                   event_time, event_timezone, station_id, payload
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                event["event_id"],
                topic,
                event["schema_version"],
                event["sequence_number"],
                event["event_type"],
                event["event_time"],
                event["event_timezone"],
                event["station_id"],
                json.dumps(
                    event["payload"],
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ),
            ],
        )

    @staticmethod
    def _store_price_event(
        connection: duckdb.DuckDBPyConnection,
        event: dict[str, Any],
        event_time: str,
    ) -> None:
        payload = event["payload"]
        connection.execute(
            """INSERT INTO price_events (
                   event_id, station_id, event_time, price_per_kwh,
                   wholesale_per_kwh, currency
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            [
                event["event_id"],
                event["station_id"],
                event_time,
                payload["price_per_kwh"],
                payload["wholesale_per_kwh"],
                payload["currency"],
            ],
        )

    @staticmethod
    def _store_session_start(
        connection: duckdb.DuckDBPyConnection,
        event: dict[str, Any],
        event_time: str,
    ) -> None:
        payload = event["payload"]
        connection.execute(
            """INSERT INTO charging_sessions (
                   session_id, start_event_id, station_id, connector_id,
                   start_time, site_type, vehicle_model, price_per_kwh
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                payload["session_id"],
                event["event_id"],
                event["station_id"],
                payload["connector_id"],
                event_time,
                payload["site_type"],
                payload["vehicle_model"],
                payload["price_per_kwh"],
            ],
        )

    @staticmethod
    def _session_row(
        connection: duckdb.DuckDBPyConnection, session_id: str
    ) -> tuple[Any, ...]:
        row = connection.execute(
            """SELECT station_id, connector_id, start_time, end_event_id, price_per_kwh
               FROM charging_sessions
               WHERE session_id = ?""",
            [session_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"Event references unknown session_id: {session_id}")
        return row

    @staticmethod
    def _require_session_identity(
        event: dict[str, Any], station_id: str, connector_id: int
    ) -> None:
        payload = event["payload"]
        if event["station_id"] != station_id or payload["connector_id"] != connector_id:
            raise ValueError(
                f"Session identity mismatch for {payload['session_id']}: "
                f"expected ({station_id}, {connector_id}), received "
                f"({event['station_id']}, {payload['connector_id']})"
            )

    @classmethod
    def _store_meter_event(
        cls,
        connection: duckdb.DuckDBPyConnection,
        event: dict[str, Any],
        event_time: str,
    ) -> None:
        payload = event["payload"]
        station_id, connector_id, start_time, end_event_id, _ = cls._session_row(
            connection, payload["session_id"]
        )
        cls._require_session_identity(event, station_id, connector_id)
        parsed_event_time = datetime.fromisoformat(event_time)
        if end_event_id is not None:
            raise ValueError(f"Meter event received for ended session: {payload['session_id']}")
        if parsed_event_time <= start_time:
            raise ValueError(f"Meter event is not after session start: {payload['session_id']}")

        previous = connection.execute(
            """SELECT event_time, energy_kwh, soc_pct
               FROM meter_events
               WHERE session_id = ?
               ORDER BY event_time DESC
               LIMIT 1""",
            [payload["session_id"]],
        ).fetchone()
        if previous is not None:
            previous_time, previous_energy, previous_soc = previous
            if parsed_event_time <= previous_time:
                raise ValueError(f"Meter event time did not increase: {payload['session_id']}")
            if payload["energy_kwh"] < previous_energy:
                raise ValueError(f"Meter energy regressed: {payload['session_id']}")
            if payload["soc_pct"] < previous_soc:
                raise ValueError(f"Meter SoC regressed: {payload['session_id']}")

        connection.execute(
            """INSERT INTO meter_events (
                   event_id, session_id, station_id, connector_id, event_time,
                   voltage_v, current_a, power_kw, energy_kwh, soc_pct
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                event["event_id"],
                payload["session_id"],
                event["station_id"],
                payload["connector_id"],
                event_time,
                payload["voltage_v"],
                payload["current_a"],
                payload["power_kw"],
                payload["energy_kwh"],
                payload["soc_pct"],
            ],
        )

    @classmethod
    def _store_session_end(
        cls,
        connection: duckdb.DuckDBPyConnection,
        event: dict[str, Any],
        event_time: str,
    ) -> None:
        payload = event["payload"]
        station_id, connector_id, start_time, end_event_id, locked_price = cls._session_row(
            connection, payload["session_id"]
        )
        cls._require_session_identity(event, station_id, connector_id)
        if end_event_id is not None:
            raise ValueError(f"Session already ended: {payload['session_id']}")
        if not math.isclose(payload["price_per_kwh"], locked_price, rel_tol=0, abs_tol=1e-9):
            raise ValueError(f"Session end tariff differs from locked price: {payload['session_id']}")

        charge_end_time = datetime.fromisoformat(payload["charge_end_time"])
        parsed_event_time = datetime.fromisoformat(event_time)
        if charge_end_time <= start_time or parsed_event_time < charge_end_time:
            raise ValueError(f"Invalid session end chronology: {payload['session_id']}")

        latest_meter = connection.execute(
            """SELECT event_time, energy_kwh
               FROM meter_events
               WHERE session_id = ?
               ORDER BY event_time DESC
               LIMIT 1""",
            [payload["session_id"]],
        ).fetchone()
        if latest_meter is None:
            raise ValueError(f"Session ended without meter telemetry: {payload['session_id']}")
        if latest_meter[0] > charge_end_time:
            raise ValueError(f"Meter event occurs after charging ended: {payload['session_id']}")
        if payload["energy_kwh"] < latest_meter[1]:
            raise ValueError(f"Session end energy is below latest meter: {payload['session_id']}")

        connection.execute(
            """UPDATE charging_sessions
               SET end_event_id = ?, charge_end_time = ?, end_time = ?,
                   charge_minutes = ?, energy_kwh = ?, avg_kw = ?, peak_kw = ?,
                   cost_aud = ?
               WHERE session_id = ?""",
            [
                event["event_id"],
                payload["charge_end_time"],
                payload["end_time"],
                payload["charge_minutes"],
                payload["energy_kwh"],
                payload["avg_kw"],
                payload["peak_kw"],
                payload["cost_aud"],
                payload["session_id"],
            ],
        )

    @staticmethod
    def _fetch_dicts(
        connection: duckdb.DuckDBPyConnection,
        sql: str,
        parameters: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        cursor = connection.execute(sql, parameters or [])
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @classmethod
    def _fetch_one_dict(
        cls,
        connection: duckdb.DuckDBPyConnection,
        sql: str,
        parameters: list[Any] | None = None,
    ) -> dict[str, Any]:
        rows = cls._fetch_dicts(connection, sql, parameters)
        if len(rows) != 1:
            raise RuntimeError(f"Expected one metric row, received {len(rows)}")
        return rows[0]

    def get_dashboard_stations(self) -> list[dict[str, Any]]:
        with self._lock:
            return self._fetch_dicts(
                self._require_connection(),
                """WITH mapped_chargers AS (
                       SELECT DISTINCT station_id, charger_id, source_rating_kw
                       FROM station_connector_mapping
                   ),
                   representatives AS (
                       SELECT
                           m.station_id,
                           m.charger_id AS representative_charger_id,
                           c.station_name,
                           c.address,
                           l.latitude,
                           l.longitude,
                           ROW_NUMBER() OVER (
                               PARTITION BY m.station_id
                               ORDER BY m.source_rating_kw DESC, m.charger_id
                           ) AS station_rank
                       FROM mapped_chargers AS m
                       JOIN chargers AS c USING (charger_id)
                       JOIN charger_locations AS l USING (location_id)
                   ),
                   summaries AS (
                       SELECT
                           m.station_id,
                           COUNT(*) AS connector_count,
                           COUNT(DISTINCT m.charger_id) AS charger_count,
                           MAX(m.source_rating_kw) AS max_rating_kw,
                           STRING_AGG(
                               DISTINCT o.operator_name,
                               ', ' ORDER BY o.operator_name
                           ) AS operator_names,
                           STRING_AGG(
                               DISTINCT ce.plug_types,
                               ', ' ORDER BY ce.plug_types
                           ) FILTER (WHERE ce.plug_types IS NOT NULL) AS plug_types
                       FROM station_connector_mapping AS m
                       JOIN chargers AS c USING (charger_id)
                       JOIN operators AS o USING (operator_id)
                       LEFT JOIN charger_enrichment AS ce USING (charger_id)
                       GROUP BY m.station_id
                   )
                   SELECT
                       r.station_id,
                       r.representative_charger_id,
                       r.station_name,
                       r.address,
                       r.latitude,
                       r.longitude,
                       s.postcode,
                       rg.sa4_name,
                       x.operator_names,
                       x.plug_types,
                       x.connector_count,
                       x.charger_count,
                       x.max_rating_kw
                   FROM representatives AS r
                   JOIN summaries AS x USING (station_id)
                   JOIN stations AS s USING (station_id)
                   LEFT JOIN regions AS rg USING (sa4_code)
                   WHERE r.station_rank = 1
                   ORDER BY r.station_id""",
            )

    def get_latest_prices(self) -> list[dict[str, Any]]:
        with self._lock:
            return self._fetch_dicts(
                self._require_connection(),
                """SELECT
                       event_id,
                       station_id,
                       event_time,
                       price_per_kwh,
                       wholesale_per_kwh,
                       currency
                   FROM latest_station_prices
                   ORDER BY station_id""",
            )

    def get_active_sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            return self._fetch_dicts(
                self._require_connection(),
                """SELECT
                       s.session_id,
                       s.station_id,
                       s.connector_id,
                       s.start_time,
                       s.site_type,
                       s.vehicle_model,
                       s.price_per_kwh,
                       m.event_time AS latest_meter_time,
                       m.voltage_v,
                       m.current_a,
                       m.power_kw,
                       m.energy_kwh,
                       m.soc_pct
                   FROM active_charging_sessions AS s
                   LEFT JOIN latest_session_meter_events AS m USING (session_id)
                   ORDER BY s.start_time, s.session_id""",
            )

    def get_session_meter_events(self, session_id: str) -> list[dict[str, Any]]:
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("session_id must be a non-empty string")
        with self._lock:
            return self._fetch_dicts(
                self._require_connection(),
                """SELECT
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
                   WHERE session_id = ?
                   ORDER BY event_time, event_id""",
                [session_id],
            )

    def get_current_metrics(self) -> dict[str, Any]:
        with self._lock:
            return self._fetch_one_dict(
                self._require_connection(),
                "SELECT * FROM current_charging_metrics",
            )

    def get_historical_metrics(self) -> dict[str, Any]:
        with self._lock:
            return self._fetch_one_dict(
                self._require_connection(),
                "SELECT * FROM historical_charging_metrics",
            )
