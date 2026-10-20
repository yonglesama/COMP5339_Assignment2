from __future__ import annotations

from pathlib import Path
from typing import Any


class DashboardRuntime:
    """Member D composition interface for B's state and C's repository."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.state_store: Any = None
        self.repository: Any = None

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError
