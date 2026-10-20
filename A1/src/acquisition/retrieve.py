from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
from pathlib import Path, PurePosixPath
import re
import stat
import struct
import tempfile
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen
import zipfile

LOG = logging.getLogger(__name__)
FIELDS = ["source_id", "role", "publisher", "version", "requested_url", "resolved_url",
          "retrieved_at_utc", "filename", "local_path", "size_bytes", "sha256",
          "content_type", "etag", "last_modified", "version_evidence"]


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def project_path(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root.resolve()) or candidate == root.resolve():
        raise ValueError(f"Path escapes project root: {relative}")
    return candidate


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write(path: Path, content: bytes, readonly=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temp = Path(stream.name)
        stream.write(content)
    try:
        os.replace(temp, path)
        if readonly:
            path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    finally:
        temp.unlink(missing_ok=True)


def load_manifest(root):
    path = root / "data/source_manifest.csv"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8", newline="") as stream:
        return {row["local_path"]: row for row in csv.DictReader(stream)}


def save_manifest(root, manifest):
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(manifest[key] for key in sorted(manifest))
    atomic_write(root / "data/source_manifest.csv", out.getvalue().encode("utf-8"))


def download(url, destination, max_bytes, expected_hash=None, attempts=3):
    """Return provenance only after the entire response passes size/hash checks."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(attempts):
        temp = None
        try:
            request = Request(url, headers={"User-Agent": "COMP5339-Acquisition/1.0", "Accept-Encoding": "identity"})
            with urlopen(request, timeout=45) as response:
                if response.status != 200:
                    raise ValueError(f"Expected HTTP 200; got {response.status}")
                headers = response.headers
                with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
                    temp = Path(stream.name)
                    size = 0
                    while chunk := response.read(1024 * 1024):
                        size += len(chunk)
                        if size > max_bytes:
                            raise ValueError("Response exceeds configured size limit")
                        stream.write(chunk)
                if size == 0:
                    raise ValueError("Empty download")
                length = headers.get("Content-Length")
                if length and size != int(length):
                    raise ValueError(f"Truncated response: expected {length}, got {size}")
                digest = sha256(temp)
                if expected_hash and digest != expected_hash:
                    raise ValueError("SHA-256 differs from pinned source; raw file not replaced")
                info = dict(resolved_url=response.url, retrieved_at_utc=utc_now(),
                            size_bytes=size, sha256=digest,
                            content_type=headers.get("Content-Type", ""),
                            etag=headers.get("ETag", ""), last_modified=headers.get("Last-Modified", ""))
            # Validation by caller happens before committing to the final raw path.
            return temp, info
        except (HTTPError, URLError, TimeoutError, ConnectionError, OSError) as exc:
            if temp:
                temp.unlink(missing_ok=True)
            if isinstance(exc, HTTPError) and exc.code not in (408, 429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {exc.code} retrieving {url}; check official access requirements") from exc
            if attempt + 1 == attempts:
                raise RuntimeError(f"Download failed after {attempts} attempts: {url}") from exc
            delay = 2 ** attempt
            if isinstance(exc, HTTPError) and (exc.headers.get("Retry-After", "").isdigit()):
                delay = min(30, int(exc.headers["Retry-After"]))
            LOG.warning("Transient download failure; retry %s/%s in %ss", attempt + 1, attempts, delay)
            time.sleep(delay)
        except Exception:
            if temp:
                temp.unlink(missing_ok=True)
            raise


def inspect_csv(path):
    # newline='' correctly handles the source's unusual CRCRLF line endings.
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        columns = reader.fieldnames or []
        required = {"Station_name", "Station_address", "Operator", "Charger_Type", "Latitude", "Longitude"}
        if not required.issubset(columns):
            raise ValueError("Expected charger CSV fields missing (possibly an HTML login/error page)")
        rows = [row for row in reader if any((value or "").strip() for value in row.values() if isinstance(value, str))]
    if not rows or any(None in row or any(v is None for v in row.values()) for row in rows):
        raise ValueError("Empty or malformed charger CSV")
    counts = {}
    for row in rows:
        key = row["Charger_Type"].strip()
        counts[key] = counts.get(key, 0) + 1
    return {"raw_record_count": len(rows), "columns": columns, "raw_charger_type_counts": counts,
            "empty_objectid_count": sum(not row.get("OBJECTID", "").strip() for row in rows),
            "empty_station_name_count": sum(not row["Station_name"].strip() for row in rows),
            "note": "Raw rows are not deduplicated stations, chargers or connectors; B determines grain."}


def safe_members(archive):
    seen = set()
    total = 0
    for item in archive.infolist():
        name = item.filename
        path = PurePosixPath(name)
        original = item.orig_filename  # ZipInfo normalizes backslashes on Windows.
        if ("\\" in original or "\x00" in original or ":" in name or path.is_absolute() or ".." in path.parts
                or stat.S_ISLNK(item.external_attr >> 16)):
            raise ValueError(f"Unsafe ZIP member: {name}")
        key = name.casefold()
        if key in seen:
            raise ValueError(f"Duplicate ZIP member: {name}")
        seen.add(key)
        total += item.file_size
        if total > 500_000_000:
            raise ValueError("Uncompressed archive exceeds limit")
    return [item for item in archive.infolist() if not item.is_dir()]


def inspect_zip(path):
    with zipfile.ZipFile(path) as archive:
        members = safe_members(archive)
        names = {item.filename for item in members}
        shapes = [n for n in names if n.lower().endswith(".shp")]
        if len(shapes) != 1 or not re.search(r"SA4_2026", shapes[0], re.I):
            raise ValueError("Expected exactly one 2026 SA4 shapefile")
        stem = shapes[0][:-4]
        for ext in (".shp", ".shx", ".dbf", ".prj"):
            if stem + ext not in names:
                raise ValueError(f"Missing shapefile companion: {stem + ext}")
        bad = archive.testzip()
        if bad:
            raise ValueError(f"ZIP CRC failed: {bad}")
        prj = archive.read(stem + ".prj").decode("utf-8-sig")
        if "GDA2020" not in prj:
            raise ValueError("Boundary CRS is not GDA2020")
        shape = archive.read(stem + ".shp")
        if len(shape) < 100 or struct.unpack(">i", shape[:4])[0] != 9994:
            raise ValueError("Invalid shapefile header")
        if struct.unpack(">i", shape[24:28])[0] * 2 != len(shape):
            raise ValueError("Invalid shapefile byte length")
        dbf = archive.read(stem + ".dbf")
        row_count = struct.unpack("<I", dbf[4:8])[0]
        header_size = struct.unpack("<H", dbf[8:10])[0]
        columns = [dbf[i:i+11].split(b"\0")[0].decode("ascii") for i in range(32, header_size-1, 32)]
        if not {"SA4_CODE26", "SA4_NAME26", "STE_CODE26"}.issubset(columns):
            raise ValueError("Expected ASGS Edition 4 SA4 attributes missing")
        return {"shapefile": shapes[0], "dbf_record_count": row_count, "columns": columns,
                "prj_wkt": prj, "archive_member_count": len(members),
                "note": "National raw boundary records; D handles NSW filtering, null geometry and spatial validation."}


def validate_metadata(source, path):
    if source["format"] == "csv":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not payload.get("success"):
            raise ValueError("CKAN metadata request unsuccessful")
        resources = payload["result"]["resources"]
        matches = [r for r in resources if r.get("url") == source["url"] and r.get("format", "").upper() == "CSV"]
        if not matches or "20251216" not in source["url"]:
            raise ValueError("Catalogue no longer confirms the pinned December 2025 CSV; review version explicitly")
        return "CKAN resource URL explicitly names ev_20251216.csv; catalogue modified date is not dataset vintage"
    text = unquote(path.read_text(encoding="utf-8"))
    if Path(urlparse(source["url"]).path).name not in text or "Edition 4" not in text:
        raise ValueError("ABS page does not confirm the pinned Edition 4 boundary archive")
    return "ABS Edition 4 download page links SA4_2026_AUST_SHP_GDA2020.zip; archive schema and PRJ checked"


def acquire_file(root, source, manifest, relative, url, role, offline, validator, expected_hash=None):
    path = project_path(root, relative)
    existing = manifest.get(relative)
    if path.exists():
        if not existing:
            raise ValueError(f"Untracked raw file: {relative}; use an empty project root")
        actual = sha256(path)
        if actual != existing["sha256"] or (expected_hash and actual != expected_hash):
            raise ValueError(f"Raw integrity failure: {relative}; restore original bytes in a new root")
        if existing["requested_url"] != url or existing["version"] != source["version"]:
            raise ValueError(f"Manifest/config mismatch: {relative}")
        result = validator(path)
        path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        LOG.info("Verified cached %s", relative)
        return result
    if offline:
        raise FileNotFoundError(f"Offline raw input missing: {relative}")
    temp, info = download(url, path, source["max_bytes"], expected_hash)
    try:
        result = validator(temp)
        evidence = result if isinstance(result, str) else source.get("version_evidence", "")
        os.replace(temp, path)
        path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        manifest[relative] = dict(source_id=source["id"], role=role, publisher=source["publisher"],
                                  version=source["version"], requested_url=url, filename=path.name,
                                  local_path=relative, version_evidence=evidence, **info)
        save_manifest(root, manifest)
        LOG.info("Downloaded %s (%s bytes)", relative, info["size_bytes"])
        return result
    finally:
        temp.unlink(missing_ok=True)


def extract(root, source, manifest):
    zip_path = project_path(root, source["path"])
    parent_row = manifest[source["path"]]
    with zipfile.ZipFile(zip_path) as archive:
        for item in safe_members(archive):
            relative = (PurePosixPath(source["extract_dir"]) / item.filename).as_posix()
            path = project_path(root, relative)
            content = archive.read(item)
            digest = hashlib.sha256(content).hexdigest()
            if path.exists():
                if sha256(path) != digest:
                    raise ValueError(f"Extracted file changed: {relative}")
                path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            else:
                atomic_write(path, content, readonly=True)
            manifest[relative] = dict(parent_row, role="extracted", filename=path.name,
                                      local_path=relative, size_bytes=len(content), sha256=digest,
                                      content_type="", version_evidence=f"Exact ZIP member {item.filename}; parent SHA-256 {parent_row['sha256']}")
    save_manifest(root, manifest)


def verify_raw(root: Path, config: dict):
    """Check existing inputs without downloading, extracting or changing any file."""
    manifest = load_manifest(root)
    summary = {"checked_at_utc": utc_now(), "offline": True, "sources": {}}

    def checked(relative, source, url, expected_hash=None):
        path = project_path(root, relative)
        row = manifest.get(relative)
        if not path.is_file() or not row:
            raise FileNotFoundError(f"Required tracked raw file missing: {relative}")
        digest = sha256(path)
        if digest != row["sha256"] or (expected_hash and digest != expected_hash):
            raise ValueError(f"Raw integrity failure: {relative}")
        if path.stat().st_size != int(row["size_bytes"]):
            raise ValueError(f"Manifest size mismatch: {relative}")
        if row["requested_url"] != url or row["version"] != source["version"]:
            raise ValueError(f"Manifest/config mismatch: {relative}")
        return path

    for source in config["sources"]:
        if not re.fullmatch(r"[0-9a-f]{64}", source.get("sha256") or ""):
            raise ValueError(f"Missing or invalid pinned SHA-256 for {source['id']}")
        metadata = checked(source["metadata_path"], source, source["metadata_url"])
        validate_metadata(source, metadata)
        path = checked(source["path"], source, source["url"], source["sha256"])
        result = inspect_csv(path) if source["format"] == "csv" else inspect_zip(path)
        if source["format"] == "zip":
            with zipfile.ZipFile(path) as archive:
                for item in safe_members(archive):
                    relative = (PurePosixPath(source["extract_dir"]) / item.filename).as_posix()
                    digest = hashlib.sha256(archive.read(item)).hexdigest()
                    checked(relative, source, source["url"], digest)
        summary["sources"][source["id"]] = result
    for relative, row in manifest.items():
        path = project_path(root, relative)
        if not path.is_file() or sha256(path) != row["sha256"]:
            raise ValueError(f"Manifest integrity failure: {relative}")
    return summary


def run(root: Path, config: dict, offline=False):
    manifest = load_manifest(root)
    summary = {"checked_at_utc": utc_now(), "offline": offline, "sources": {}}
    for source in config["sources"]:
        source = dict(source)
        if not re.fullmatch(r"[0-9a-f]{64}", source.get("sha256") or ""):
            raise ValueError(f"Missing or invalid pinned SHA-256 for {source['id']}")
        source["version_evidence"] = acquire_file(
            root, source, manifest, source["metadata_path"], source["metadata_url"], "metadata", offline,
            lambda path: validate_metadata(source, path))
        result = acquire_file(root, source, manifest, source["path"], source["url"], "download", offline,
                              inspect_csv if source["format"] == "csv" else inspect_zip, source["sha256"])
        if source["format"] == "zip":
            extract(root, source, manifest)
        summary["sources"][source["id"]] = result
    # Also verify every tracked byte, including metadata and extracted companions.
    for relative, row in manifest.items():
        if sha256(project_path(root, relative)) != row["sha256"]:
            raise ValueError(f"Manifest integrity failure: {relative}")
    return summary
