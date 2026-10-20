"""Standard-library regression checks. Run from the project root."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.dont_write_bytecode = True
from src.acquisition.retrieve import verify_raw


class AcquisitionChecks(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / 'config/sources.json').read_text())

    def test_verification_is_read_only(self):
        def snapshot():
            return {str(p): (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
                    for p in (ROOT / 'data').rglob('*') if p.is_file()}
        before = snapshot()
        result = verify_raw(ROOT, self.config)
        self.assertEqual(before, snapshot())
        self.assertEqual(result['sources']['nsw_ev_20251216']['raw_record_count'], 1958)

    def test_missing_root_does_not_get_created(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'not-created'
            proc = subprocess.run([sys.executable, str(ROOT / 'run_pipeline.py'),
                                   '--project-root', str(root), '--stage', 'verify-raw'],
                                  capture_output=True, text=True)
            self.assertNotEqual(proc.returncode, 0)
            self.assertFalse(root.exists())

    def test_missing_stage_cannot_report_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'config').mkdir()
            (root / 'config/sources.json').write_text(json.dumps(self.config))
            (root / 'config/pipeline.json').write_text('{"stages": []}')
            proc = subprocess.run([sys.executable, str(ROOT / 'run_pipeline.py'),
                                   '--project-root', str(root), '--stage', 'cleaning'],
                                  capture_output=True, text=True)
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn('exactly one configured stage', proc.stderr)


if __name__ == '__main__':
    unittest.main()
