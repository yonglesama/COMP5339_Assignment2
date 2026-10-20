from __future__ import annotations

from pathlib import Path
from typing import Any


class EventRepository:
    """Member C interface used by B and D; DuckDB implementation intentionally omitted."""

    def __init__(self, database_path: Path, *, read_only: bool = False) -> None:
        self.database_path = Path(database_path)
        self.read_only = read_only

    def close(self) -> None:
        raise NotImplementedError

    def has_event(self, event_id: str) -> bool:
        raise NotImplementedError

    def store_event(self, topic: str, event: dict[str, Any]) -> bool:
        raise NotImplementedError

    def get_dashboard_stations(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_latest_prices(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_active_sessions(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_session_meter_events(self, session_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_current_metrics(self) -> dict[str, Any]:
        raise NotImplementedError

    def get_historical_metrics(self) -> dict[str, Any]:
        raise NotImplementedError
