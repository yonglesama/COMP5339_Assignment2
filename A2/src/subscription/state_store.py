from __future__ import annotations

from typing import Any


class StateStore:
    """Member B interface consumed by member D; implementation intentionally omitted."""

    def apply_event(self, event: dict[str, Any]) -> None:
        raise NotImplementedError

    def get_station_state(self, station_id: str) -> dict[str, Any]:
        raise NotImplementedError

    def get_all_station_states(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_active_sessions(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_session_curve(self, session_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_current_metrics(self) -> dict[str, int]:
        raise NotImplementedError
