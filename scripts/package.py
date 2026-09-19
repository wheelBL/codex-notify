#!/usr/bin/env python3
"""Build a reproducible allowlisted archive; never package runtime state."""
import argparse
import gzip
import hashlib
import io
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, default=ROOT / 'dist')
    args = p.parse_args()
    import sys
    sys.path.insert(0, str(ROOT))
    from codex_notify import __version__
    files = [ROOT / name for name in ('README.md', 'LICENSE', 'pyproject.toml')]
    for folder in ('codex_notify', 'scripts', 'skills', 'tests'):
        files += [p for p in (ROOT / folder).rglob('*') if p.is_file()
                  and p.suffix in ('.py', '.md', '.yaml') and '__pycache__' not in p.parts]
    args.output.mkdir(parents=True, exist_ok=True)
    target = args.output / f'codex-notify-{__version__}.tar.gz'
    with target.open('wb') as raw, gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0) as gz, tarfile.open(fileobj=gz, mode='w') as tar:
        for path in sorted(files):
            data = path.read_bytes()
            info = tarfile.TarInfo('codex-notify-' + __version__ + '/' + str(path.relative_to(ROOT)))
            info.mode = 0o644
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    target.with_suffix(target.suffix + '.sha256').write_text(hashlib.sha256(target.read_bytes()).hexdigest() + '  ' + target.name + '\n')
    print(target)


if __name__ == '__main__':
    main()
