"""Validate MQTT envelopes, persist once, then update the live projection."""
from __future__ import annotations

from dataclasses import dataclass
import json
from threading import RLock
from typing import Any, Protocol

from events.contract import validate_event
from subscription.state_store import StateStore


class EventSink(Protocol):
    def has_event(self, event_id: str) -> bool: ...
    def store_event(self, topic: str, event: dict[str, Any]) -> bool: ...


@dataclass(frozen=True)
class ProcessResult:
    event_id: str
    stored: bool
    state_updated: bool


class InvalidMessage(ValueError):
    """Malformed input may be logged and rejected without stopping the consumer."""


class EventProcessor:
    def __init__(self, topic_prefix: str, state_store: StateStore, event_sink: EventSink) -> None:
        self.topic_prefix = topic_prefix.strip("/")
        if not self.topic_prefix or any(c in self.topic_prefix for c in "+#"):
            raise ValueError("Invalid topic prefix")
        self.state_store = state_store
        self.event_sink = event_sink
        self._lock = RLock()

    @staticmethod
    def _object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise InvalidMessage(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    def process_message(self, topic: str, payload: bytes | str) -> ProcessResult:
        try:
            event = json.loads(payload, object_pairs_hook=self._object)
        except (ValueError, UnicodeError, TypeError) as exc:
            raise InvalidMessage(f"Invalid JSON: {exc}") from exc
        return self.process(topic, event)

    def process(self, topic: str, event: dict[str, Any]) -> ProcessResult:
        try:
            validate_event(event)
            expected = f"{self.topic_prefix}/{event['station_id']}/{event['event_type']}"
            if topic != expected:
                raise ValueError(f"Topic/event mismatch: expected {expected}")
        except (ValueError, KeyError, TypeError, OverflowError) as exc:
            raise InvalidMessage(str(exc)) from exc
        with self._lock:
            if self.event_sink.has_event(event["event_id"]):
                return ProcessResult(event["event_id"], False, False)
            # C checks references, chronology and cumulative measurements transactionally.
            # Exceptions propagate so the service cannot silently acknowledge lost writes.
            stored = self.event_sink.store_event(topic, event)
            if stored:
                self.state_store.apply_event(event)
            return ProcessResult(event["event_id"], stored, stored)
