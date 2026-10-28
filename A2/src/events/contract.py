"""Version 1 envelope helpers shared by A's preparation and publishing code."""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math

PRIORITY = {"price_update": 0, "session_start": 1, "meter": 2, "session_end": 3}
FIELDS = {
    "price_update": {"price_per_kwh", "wholesale_per_kwh", "currency"},
    "session_start": {"session_id", "connector_id", "site_type", "vehicle_model", "price_per_kwh"},
    "meter": {"session_id", "connector_id", "voltage_v", "current_a", "power_kw", "energy_kwh", "soc_pct"},
    "session_end": {"session_id", "connector_id", "charge_end_time", "end_time", "charge_minutes",
                    "energy_kwh", "avg_kw", "peak_kw", "price_per_kwh", "cost_aud"},
}


def timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        raise ValueError("v1 uses naive local timestamps with event_timezone=Australia/Sydney")
    return parsed.isoformat(timespec="seconds")


def number(value: object, *, minimum: float | None = 0) -> float:
    if isinstance(value, bool):
        raise ValueError("A boolean is not a numeric measurement")
    result = float(value)
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise ValueError(f"Invalid numeric value: {value}")
    return result


def envelope(kind: str, time: str, station: str, payload: dict) -> dict:
    event = {"schema_version": "1.0", "event_type": kind, "event_time": timestamp(time),
             "event_timezone": "Australia/Sydney", "station_id": station, "payload": payload}
    canonical = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    event["event_id"] = "evt_" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return event


def validate_event(event: dict) -> tuple:
    if not isinstance(event, dict):
        raise ValueError("Each event must be a JSON object")
    if event.get("schema_version") != "1.0" or event.get("event_timezone") != "Australia/Sydney":
        raise ValueError("Unsupported schema or timezone")
    kind = event.get("event_type")
    if kind not in PRIORITY:
        raise ValueError(f"Unknown event type: {kind}")
    for key in ("station_id", "event_id"):
        if not isinstance(event.get(key), str) or not event[key] or any(c in event[key] for c in "/+#"):
            raise ValueError(f"Invalid {key}")
    sequence = event.get("sequence_number")
    if type(sequence) is not int or sequence < 1:
        raise ValueError("sequence_number must be a positive integer")
    time = timestamp(event["event_time"])
    if time != event["event_time"]:
        raise ValueError("Event timestamps must use ISO seconds")
    payload = event.get("payload")
    if not isinstance(payload, dict) or not FIELDS[kind] <= payload.keys():
        raise ValueError(f"Missing payload fields for {kind}")
    if kind != "price_update":
        if not isinstance(payload["session_id"], str) or not payload["session_id"]:
            raise ValueError("Invalid session_id")
        if type(payload["connector_id"]) is not int or payload["connector_id"] < 1:
            raise ValueError("Invalid connector_id")
    for key in FIELDS[kind] - {"currency", "session_id", "connector_id", "site_type", "vehicle_model", "charge_end_time", "end_time"}:
        if type(payload[key]) not in (int, float):
            raise ValueError(f"JSON number required: {key}")
        number(payload[key], minimum=None if key == "wholesale_per_kwh" else 0)
    if kind == "price_update" and payload["currency"] != "AUD":
        raise ValueError("Expected AUD")
    if kind == "meter" and payload["soc_pct"] > 100:
        raise ValueError("SoC exceeds 100%")
    if kind == "session_start" and payload["site_type"] not in {"urban", "regional"}:
        raise ValueError("Invalid site_type")
    if kind == "session_end":
        if timestamp(payload["end_time"]) != time or timestamp(payload["charge_end_time"]) > time:
            raise ValueError("Invalid session end times")
    return time, PRIORITY[kind]
