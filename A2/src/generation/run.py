"""Validate inputs and invoke the unmodified professor-provided generator."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import urllib.request

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.paths import load_json, write_metric_csv
from generation.export_stations import sha256

AEMO_URL = "https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{month}_{region}.csv"
OUTPUT_FILES = ["stations.csv", "prices_hourly.csv", "sessions.csv", "session_metering.csv"]


def validate_wholesale(paths: list[Path], start: datetime, days: int, region: str) -> dict:
    buckets: dict[datetime, list[datetime]] = {}
    end = start + timedelta(days=days)
    for path in paths:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                if row["REGION"] != region:
                    raise ValueError(f"Unexpected AEMO region in {path}")
                ts = datetime.strptime(row["SETTLEMENTDATE"], "%Y/%m/%d %H:%M:%S")
                hour = (ts - timedelta(minutes=5)).replace(minute=0, second=0)
                if start <= hour < end:
                    if not math.isfinite(float(row["RRP"])):
                        raise ValueError("Non-finite AEMO wholesale price")
                    buckets.setdefault(hour, []).append(ts)
    expected = [start + timedelta(hours=i) for i in range(days * 24)]
    missing = [h.isoformat() for h in expected if len(set(buckets.get(h, []))) != 12 or len(buckets.get(h, [])) != 12]
    if missing:
        raise ValueError(f"Incomplete AEMO coverage; no mean-price fallback allowed: {missing[:5]}")
    return {"hours": len(expected), "intervals": sum(map(len, buckets.values()))}


def generate(root: Path, profile: str, data_root: Path | None = None,
             start_override: str | None = None, days_override: int | None = None) -> dict:
    data_root = data_root or root / "A2/data"
    config = load_json(root / f"A2/config/generator.{profile}.json")
    if start_override is not None:
        config["start"] = start_override
    if days_override is not None:
        config["days"] = days_override
    start = datetime.fromisoformat(config["start"])
    if start.time() != datetime.min.time() or start.tzinfo or config["days"] <= 0:
        raise ValueError("start must be a naive midnight; days must be positive")
    station_path = data_root / "input/generator_stations.csv"
    with (data_root / "quality/station_export_summary.csv").open(newline="", encoding="utf-8") as handle:
        summary = {r["metric"]: r["value"] for r in csv.DictReader(handle)}
    if summary["profile"] != profile or summary["station_csv_sha256"] != sha256(station_path):
        raise ValueError("Station export/profile mismatch; rerun station export with the same profile")
    generated = data_root / "generated"
    generated.mkdir(parents=True, exist_ok=True)
    cache = generated / "aemo_cache"
    cache.mkdir(exist_ok=True)
    end = start + timedelta(days=config["days"])
    months = sorted({(start + timedelta(hours=h)).strftime("%Y%m")
                     for h in range(config["days"] * 24 + 1)})
    sources = []
    if not config["static_prices"]:
        for month in months:
            path = cache / f"PRICE_AND_DEMAND_{month}_{config['region']}.csv"
            url = AEMO_URL.format(month=month, region=config["region"])
            if not path.exists():
                request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(request, timeout=30) as response:
                    contents = response.read()
                partial = path.with_suffix(".download")
                partial.write_bytes(contents)
                partial.replace(path)
            sources.append({"file": path.name, "url": url, "sha256": sha256(path)})
        coverage = validate_wholesale([cache / s["file"] for s in sources], start, config["days"], config["region"])
    else:
        coverage = {"hours": 0, "intervals": 0}
    script = root / "A2/external/data_generator/generate_dataset.py"
    with tempfile.TemporaryDirectory(prefix=".generation-", dir=generated) as directory:
        staging = Path(directory)
        if sources:
            (staging / "aemo_cache").mkdir()
            for source in sources:
                shutil.copy2(cache / source["file"], staging / "aemo_cache" / source["file"])
        command = [sys.executable, str(script), "--stations", str(station_path.resolve()),
                   "--start", config["start"], "--days", str(config["days"]),
                   "--region", config["region"], "--seed", str(config["seed"]),
                   "--meter-every", str(config["meter_every_minutes"]), "--out", str(staging.resolve())]
        if config["static_prices"]:
            command.append("--static-prices")
        env = dict(os.environ, PYTHONUTF8="1", PYTHONDONTWRITEBYTECODE="1")
        subprocess.run(command, cwd=root, env=env, check=True)
        counts = {}
        for filename in OUTPUT_FILES:
            with (staging / filename).open(newline="", encoding="utf-8") as handle:
                counts[filename] = sum(1 for _ in csv.DictReader(handle))
        if counts["prices_hourly.csv"] != int(summary["exported_stations"]) * config["days"] * 24:
            raise ValueError("Unexpected hourly price count")
        for filename in OUTPUT_FILES:
            (staging / filename).replace(generated / filename)
    provenance = {"profile": profile, "effective_config": config,
                  "period_end_exclusive": end.isoformat(), "event_timezone": "Australia/Sydney",
                  "aemo_time_basis": "AEMO interval labels; generator uses naive simulated timestamps without DST conversion",
                  "generator_sha256": sha256(script), "station_csv_sha256": sha256(station_path),
                  "aemo_sources": sources, "wholesale_coverage": coverage,
                  "row_counts": counts,
                  "outputs": {f: sha256(generated / f) for f in OUTPUT_FILES}}
    (generated / "generation_manifest.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    write_metric_csv(data_root / "quality/generation_summary.csv", [
        ("profile", profile, "Effective overrides recorded in generation_manifest.json"),
        ("start", config["start"], "Supplied generator's simulated timestamp labels"),
        ("days", config["days"], "Session starts in window; ends can extend beyond it"),
        ("seed", config["seed"], "Unmodified supplied generator"),
        ("static_prices", config["static_prices"], "True is development only"),
        ("wholesale_hours_validated", coverage["hours"], "12 distinct five-minute samples per hour"),
        *[(name.removesuffix(".csv") + "_rows", count, name) for name, count in counts.items()],
    ])
    checksum_rows = [
        ("generator", provenance["generator_sha256"], str(script.relative_to(root))),
        ("station_input", provenance["station_csv_sha256"], str(station_path.relative_to(root))),
        *[(f"aemo_{source['file']}", source["sha256"], source["url"])
          for source in sources],
        *[(f"output_{filename}", provenance["outputs"][filename],
           str((generated / filename).relative_to(root)))
          for filename in OUTPUT_FILES],
    ]
    write_metric_csv(data_root / "quality/generation_checksums.csv", checksum_rows)
    return provenance


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--profile", choices=["dev", "assessment"], default="assessment")
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--start", help="Override the start date from the generator profile")
    parser.add_argument("--days", type=int)
    args = parser.parse_args(argv)
    generate(args.project_root.resolve(), args.profile, args.data_root, args.start, args.days)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
