from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import subprocess
import sys

# Keep runtime caches out of the submission directory.
sys.dont_write_bytecode = True
from src.acquisition.retrieve import project_path, run as acquire, verify_raw

CODE_ROOT = Path(__file__).resolve().parent
LOG = logging.getLogger("pipeline")


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def require_paths(root, paths, label):
    missing = [p for p in paths if not project_path(root, p).exists()
               or (project_path(root, p).is_file() and project_path(root, p).stat().st_size == 0)]
    if missing:
        raise FileNotFoundError(f"{label}: missing/empty {', '.join(missing)}")


def run_member_stage(root, stage):
    require_paths(root, [stage["script"]] + stage["inputs"], f"{stage['name']} (member {stage['owner']})")
    LOG.info("Starting %s (member %s)", stage["name"], stage["owner"])
    # Contract: each script accepts --project-root; same Python interpreter/venv.
    command = [sys.executable, str(project_path(root, stage["script"])), "--project-root", str(root)]
    with subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, encoding="utf-8", errors="replace") as process:
        for line in process.stdout:
            LOG.info("[%s] %s", stage["name"], line.rstrip())
        code = process.wait()
    if code:
        raise RuntimeError(f"{stage['name']} failed with exit code {code}")
    require_paths(root, stage["outputs"], f"{stage['name']} outputs")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Acquire raw data or run configured team modules.")
    parser.add_argument("--project-root", type=Path, nargs="?", const=Path("."), default=Path("."),
                        help="Output/project root; omitted flag or omitted PATH uses the current working directory")
    parser.add_argument(
        "--stage",
        default="acquire",
        choices=["acquire", "verify-raw", "ab", "cleaning", "augmentation", "database", "validate", "all"],
        help="Use 'ab' to verify/acquire member A's inputs and then run member B's cleaning stage",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Disallow network acquisition; with --stage ab, verify cached A inputs before running B",
    )
    args = parser.parse_args(argv)
    root = args.project_root.resolve()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s",
                        handlers=[logging.StreamHandler()], force=True)
    local_config = root / "config"
    config_dir = local_config if (local_config / "sources.json").exists() else CODE_ROOT / "config"
    try:
        sources = load_json(config_dir / "sources.json")
        pipeline = load_json(config_dir / "pipeline.json")
        if args.stage not in ("acquire", "verify-raw", "ab", "all"):
            matches = [s for s in pipeline["stages"] if s["name"] == args.stage]
            if len(matches) != 1:
                raise ValueError(f"Expected exactly one configured stage named {args.stage}")
        if args.stage == "ab":
            cleaning = [s for s in pipeline["stages"] if s["name"] == "cleaning"]
            if len(cleaning) != 1:
                raise ValueError("Expected exactly one configured cleaning stage for the AB pipeline")
            require_paths(root, [cleaning[0]["script"]], "AB pipeline preflight")
        if args.offline and args.stage not in ("acquire", "verify-raw", "ab"):
            raise ValueError("--offline covers A's acquisition only; use cached acquisition then run member stages with their documented settings")
        if args.stage == "all":
            require_paths(root, [s["script"] for s in pipeline["stages"]], "Full pipeline preflight")
        if args.stage in ("acquire", "verify-raw", "ab", "all"):
            read_only_verification = args.stage == "verify-raw" or (args.stage == "ab" and args.offline)
            summary = (verify_raw(root, sources) if read_only_verification
                       else acquire(root, sources, offline=args.offline))
            for source_id, info in summary["sources"].items():
                LOG.info("%s: %s raw records", source_id, info.get("raw_record_count", info.get("dbf_record_count")))
        for stage in pipeline["stages"]:
            if args.stage in (stage["name"], "all") or (args.stage == "ab" and stage["name"] == "cleaning"):
                run_member_stage(root, stage)
        LOG.info("Requested stage completed: %s", args.stage)
        return 0
    except Exception as exc:
        LOG.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
