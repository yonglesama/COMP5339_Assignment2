"""Run A's completed batch stages without invoking the B/C/D placeholders."""
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "A2/src"))
from events.make_demo import make_demo
from events.prepare import prepare
from generation.export_stations import export_stations
from generation.run import generate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=ROOT)
    parser.add_argument("--profile", choices=["assessment", "dev"], default="assessment")
    parser.add_argument("--start", help="Override the start date from the generator profile")
    parser.add_argument("--days", type=int)
    args = parser.parse_args()
    root = args.project_root.resolve()
    data = root / ("A2/data/dev" if args.profile == "dev" else "A2/data")
    export_stations(root, args.profile, data)
    generate(root, args.profile, data, args.start, args.days)
    prepare(root, data)
    make_demo(data)
    print("Member A batch stages completed. Start a subscriber/dashboard before publishing.")


if __name__ == "__main__":
    main()
