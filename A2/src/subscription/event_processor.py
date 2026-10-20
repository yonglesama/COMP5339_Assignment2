from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from subscription.state_store import StateStore


class EventSink(Protocol):
    """Member C interface injected into member B's processor."""

    def has_event(self, event_id: str) -> bool: ...

    def store_event(self, topic: str, event: dict[str, Any]) -> bool: ...


@dataclass(frozen=True)
class ProcessResult:
    event_id: str
    stored: bool
    state_updated: bool


class EventProcessor:
    """Member B must validate, persist through EventSink and update StateStore."""

    def __init__(self, topic_prefix: str, state_store: StateStore, event_sink: EventSink) -> None:
        self.topic_prefix = topic_prefix
        self.state_store = state_store
        self.event_sink = event_sink

    def process(self, topic: str, event: dict[str, Any]) -> ProcessResult:
        raise NotImplementedError
