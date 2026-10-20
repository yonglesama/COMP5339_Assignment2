from __future__ import annotations

import argparse
from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.stage_stub import todo_stage


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Member A: publish the ordered event stream to MQTT.")
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--delay", type=float, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.parse_args(argv)
    return todo_stage("A", "publish in event-time order with at least 0.1 seconds between messages")


if __name__ == "__main__":
    raise SystemExit(main())
