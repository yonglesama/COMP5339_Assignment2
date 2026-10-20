from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True

CODE_ROOT = Path(__file__).resolve().parent
DEFAULT_PROJECT_ROOT = CODE_ROOT.parent
LOG = logging.getLogger("a2.pipeline")


def load_config() -> dict:
    path = CODE_ROOT / "config" / "pipeline.json"
    return json.loads(path.read_text(encoding="utf-8-sig"))


def resolve_path(project_root: Path, relative_path: str) -> Path:
    return project_root / Path(relative_path)


def require_paths(project_root: Path, paths: list[str], label: str) -> None:
    missing: list[str] = []
    for relative_path in paths:
        path = resolve_path(project_root, relative_path)
        if not path.exists() or (path.is_file() and path.stat().st_size == 0):
            missing.append(relative_path)
    if missing:
        raise FileNotFoundError(f"{label}: missing/empty {', '.join(missing)}")


def run_stage(project_root: Path, stage: dict, check_outputs: bool) -> None:
    label = f"{stage['name']} (owner {stage['owner']})"
    require_paths(project_root, [stage["script"], *stage.get("inputs", [])], label)
    command = [
        sys.executable,
        str(resolve_path(project_root, stage["script"])),
        "--project-root",
        str(project_root),
        *stage.get("args", []),
    ]
    LOG.info("Starting %s", label)
    with subprocess.Popen(
        command,
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            LOG.info("[%s] %s", stage["name"], line.rstrip())
        return_code = process.wait()
    if return_code:
        raise RuntimeError(f"{label} failed with exit code {return_code}")
    if check_outputs:
        require_paths(project_root, stage.get("outputs", []), f"{label} outputs")


def describe(config: dict) -> None:
    print("Batch stages (finish and validate declared output files):")
    for stage in config["batch_stages"]:
        print(f"  {stage['name']:<22} owner={stage['owner']:<8} {stage['description']}")
    print("\nServices (long-running foreground processes):")
    for stage in config["services"]:
        print(f"  {stage['name']:<22} owner={stage['owner']:<8} {stage['description']}")
    print("\nAliases:")
    for name, stages in config["aliases"].items():
        print(f"  {name:<22} {' -> '.join(stages)}")


def main(argv: list[str] | None = None) -> int:
    config = load_config()
    stages = {
        stage["name"]: (stage, True)
        for stage in config["batch_stages"]
    }
    stages.update({stage["name"]: (stage, False) for stage in config["services"]})
    choices = sorted([*stages, *config["aliases"]])

    parser = argparse.ArgumentParser(
        description="Run the configured COMP5339 Assignment 2 batch stages or streaming services."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help="Repository root containing A1/ and A2/ (defaults to the parent of this script).",
    )
    parser.add_argument("--stage", choices=choices, help="Configured stage or alias to run.")
    parser.add_argument("--list-stages", action="store_true", help="Print stage ownership and contracts.")
    args = parser.parse_args(argv)

    if args.list_stages:
        describe(config)
        return 0
    if not args.stage:
        parser.error("one of --stage or --list-stages is required")

    project_root = args.project_root.resolve()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler()],
        force=True,
    )
    requested = config["aliases"].get(args.stage, [args.stage])
    try:
        for name in requested:
            if name not in stages:
                raise ValueError(f"Alias {args.stage!r} references unknown stage {name!r}")
            stage, check_outputs = stages[name]
            run_stage(project_root, stage, check_outputs=check_outputs)
        LOG.info("Requested stage completed: %s", args.stage)
        return 0
    except Exception as exc:
        LOG.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
