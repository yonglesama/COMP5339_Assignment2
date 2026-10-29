"""Thread-safe live projection; durable history remains in C's repository."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from threading import RLock
from typing import Any


class StateStore:
    """Expose JSON-friendly snapshots to D. Only active-session curves are retained."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._stations: dict[str, dict[str, Any]] = {}
        self._sessions: dict[str, dict[str, Any]] = {}
        self._curves: dict[str, list[dict[str, Any]]] = {}

    def _station(self, station_id: str) -> dict[str, Any]:
        return self._stations.setdefault(station_id, {
            "station_id": station_id, "price_per_kwh": None,
            "price_event_time": None, "currency": "AUD",
        })

    def apply_event(self, event: dict[str, Any]) -> None:
        """Apply a validated, committed event; EventProcessor serialises this boundary."""
        with self._lock:
            station = self._station(event["station_id"])
            kind, payload = event["event_type"], event["payload"]
            if kind == "price_update":
                if station["price_event_time"] is None or event["event_time"] >= station["price_event_time"]:
                    station.update(price_per_kwh=payload["price_per_kwh"],
                                   price_event_time=event["event_time"], currency=payload["currency"])
            elif kind == "session_start":
                sid = payload["session_id"]
                self._sessions[sid] = {**deepcopy(payload), "station_id": event["station_id"],
                                       "start_time": event["event_time"], "latest_meter_time": None}
                self._curves[sid] = []
            elif kind == "meter":
                sid = payload["session_id"]
                point = {**deepcopy(payload), "event_time": event["event_time"],
                         "event_id": event["event_id"], "station_id": event["station_id"]}
                self._curves[sid].append(point)
                self._sessions[sid].update(deepcopy(payload), latest_meter_time=event["event_time"])
            elif kind == "session_end":
                # Same-second handover can start the new session before the old end.
                sid = payload["session_id"]
                self._sessions.pop(sid, None)
                self._curves.pop(sid, None)

    @staticmethod
    def _json_value(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat(timespec="seconds")
        if isinstance(value, dict):
            return {k: StateStore._json_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [StateStore._json_value(v) for v in value]
        return value

    def restore(self, repository: Any) -> None:
        """Call before subscription starts, while this process owns the repository."""
        prices = self._json_value(repository.get_latest_prices())
        sessions = self._json_value(repository.get_active_sessions())
        curves = {s["session_id"]: self._json_value(repository.get_session_meter_events(s["session_id"]))
                  for s in sessions}
        with self._lock:
            self._stations.clear()
            self._sessions = {s["session_id"]: deepcopy(s) for s in sessions}
            self._curves = deepcopy(curves)
            for price in prices:
                self._station(price["station_id"]).update(
                    price_per_kwh=price["price_per_kwh"], price_event_time=price["event_time"],
                    currency=price["currency"])
            for session in sessions:
                self._station(session["station_id"])

    def _snapshot(self, station_id: str) -> dict[str, Any]:
        station = deepcopy(self._stations.get(station_id, {
            "station_id": station_id, "price_per_kwh": None,
            "price_event_time": None, "currency": "AUD"}))
        sessions = [deepcopy(s) for s in self._sessions.values() if s["station_id"] == station_id]
        connectors = sorted({s["connector_id"] for s in sessions})
        station.update(active_sessions=sorted(sessions, key=lambda s: s["session_id"]),
                       active_session_count=len(sessions), occupied_connector_ids=connectors,
                       occupied_connector_count=len(connectors), occupancy=bool(connectors))
        return station

    def get_station_state(self, station_id: str) -> dict[str, Any]:
        with self._lock:
            return self._snapshot(station_id)

    def get_all_station_states(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._snapshot(sid) for sid in sorted(self._stations)]

    def get_active_sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            return deepcopy(sorted(self._sessions.values(), key=lambda s: s["session_id"]))

    def get_session_curve(self, session_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return deepcopy(self._curves.get(session_id, []))

    def get_current_metrics(self) -> dict[str, int]:
        with self._lock:
            return {"active_session_count": len(self._sessions),
                    "active_station_count": len({s["station_id"] for s in self._sessions.values()}),
                    "occupied_connector_count": len({(s["station_id"], s["connector_id"])
                                                      for s in self._sessions.values()})}
