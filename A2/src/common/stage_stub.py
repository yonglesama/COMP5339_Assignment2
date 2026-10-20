from __future__ import annotations

import sys


def todo_stage(owner: str, responsibility: str) -> int:
    print(
        f"TODO member {owner}: {responsibility}. "
        "Implement this stage according to A2/docs/team_contracts.md.",
        file=sys.stderr,
    )
    return 2
