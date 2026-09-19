"""Verify public artifacts contain only relocatable source, not local state."""
import hashlib
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class PackageTests(unittest.TestCase):
    def test_reproducible_archive_and_no_runtime_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ('one', 'two'):
                subprocess.run([sys.executable, str(ROOT / 'scripts/package.py'), '--output', str(root / name)],
                               check=True, capture_output=True)
            first = next((root / 'one').glob('*.tar.gz'))
            second = next((root / 'two').glob('*.tar.gz'))
            self.assertEqual(first.read_bytes(), second.read_bytes())
            checksum = first.with_suffix(first.suffix + '.sha256').read_text().split()[0]
            self.assertEqual(checksum, hashlib.sha256(first.read_bytes()).hexdigest())
            with tarfile.open(first) as tar:
                names = tar.getnames()
                self.assertTrue(all(m.isfile() and '..' not in Path(m.name).parts for m in tar.getmembers()))
                self.assertTrue(any(n.endswith('/scripts/install.py') for n in names))
                self.assertTrue(any(n.endswith('/codex_notify/transport.py') for n in names))
                self.assertFalse(any(n.endswith('deployment.json') or '.sqlite3' in n or '/.git/' in n for n in names))


if __name__ == '__main__':
    unittest.main()
