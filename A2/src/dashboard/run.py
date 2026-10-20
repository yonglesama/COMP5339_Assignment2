from __future__ import annotations

import argparse
from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.stage_stub import todo_stage


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Member D: map dashboard and live charging curve.")
    parser.add_argument("--project-root", type=Path, required=True)
    parser.parse_args(argv)
    return todo_stage("D", "select a Python UI framework and compose B's state with C's query interface")


if __name__ == "__main__":
    raise SystemExit(main())
