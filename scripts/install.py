#!/usr/bin/env python3
"""Install the service and global Skill from a checkout or release archive."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def run(*argv, **kwargs):
    return subprocess.run([str(x) for x in argv], check=True, **kwargs)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runtime-dir', type=Path)
    p.add_argument('--codex-home', type=Path, default=Path(os.environ.get('CODEX_HOME', str(Path.home() / '.codex'))))
    p.add_argument('--workflow-adapter', action='store_true', help='Also supervise the optional workflow producer')
    p.add_argument('--no-start', action='store_true')
    args = p.parse_args()
    if sys.platform != 'linux' or sys.version_info < (3, 10):
        p.error('This installer supports Linux with Python 3.10+')
    home = args.codex_home.expanduser().resolve()
    skill = home / 'skills/codex-notify'
    receipt = skill / 'deployment.json'
    previous = json.loads(receipt.read_text()) if receipt.exists() else {}
    if skill.exists() and not previous:
        p.error('Existing unmanaged codex-notify Skill; refusing to overwrite')
    runtime = (args.runtime_dir or Path(previous.get('runtime_dir', str(Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'codex-notify')))).expanduser().resolve()
    # Supervisor configuration uses INI and quoted environment values. Reject
    # control characters rather than emitting ambiguous service configuration.
    for path in (home, runtime):
        if any(c in str(path) for c in '\n\r%'):
            p.error('Installation paths cannot contain newlines or percent signs')
    runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
    python = runtime / 'venv/bin/python'
    config = runtime / 'supervisord.conf'
    ctl = [runtime / 'venv/bin/supervisorctl', '-c', config]
    if not python.exists():
        run(sys.executable, '-m', 'venv', runtime / 'venv')
    # Build from a staging copy so packaging never dirties the caller's checkout.
    import tempfile
    with tempfile.TemporaryDirectory(prefix='codex-notify-install-') as temporary:
        stage = Path(temporary) / 'source'
        shutil.copytree(ROOT, stage, ignore=shutil.ignore_patterns('.git', '__pycache__', '.venv', 'build', 'dist', '*.egg-info'))
        run(python, '-m', 'pip', 'install', '--upgrade', stage)
    skill.mkdir(parents=True, exist_ok=True)
    for name in ('SKILL.md', 'agents/openai.yaml'):
        target = skill / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / 'skills/codex-notify' / name, target)
    use_workflow = args.workflow_adapter or previous.get('workflow_adapter', False)
    receipt.write_text(json.dumps({'runtime_dir': str(runtime), 'codex_home': str(home),
                                   'workflow_adapter': use_workflow}, indent=2) + '\n')
    command = [str(python), '-m', 'codex_notify', '--state-dir', str(runtime / 'state')]
    content = f'''[unix_http_server]
file={runtime}/supervisor.sock
chmod=0700
[supervisord]
pidfile={runtime}/supervisord.pid
logfile={runtime}/supervisord.log
logfile_maxbytes=5MB
logfile_backups=3
[rpcinterface:supervisor]
supervisor.rpcinterface_factory=supervisor.rpcinterface:make_main_rpcinterface
[supervisorctl]
serverurl=unix://{runtime}/supervisor.sock
'''
    programs = {'delivery': command + ['serve', '--socket', str(home / 'app-server-control/app-server-control.sock')]}
    if use_workflow:
        programs['workflow'] = command + ['workflow-watch']
    for role, argv in programs.items():
        content += f'''
[program:codex-notify-{role}]
command={shlex.join(argv)}
directory={runtime}
autostart=true
autorestart=true
startsecs=0
stopasgroup=true
killasgroup=true
stdout_logfile={runtime}/{role}.log
stdout_logfile_maxbytes=5MB
stdout_logfile_backups=3
redirect_stderr=true
'''
    config.write_text(content)
    if not args.no_start:
        active = subprocess.run([str(x) for x in ctl + ['pid']], capture_output=True)
        if active.returncode:
            run(runtime / 'venv/bin/supervisord', '-c', config)
        else:
            run(*ctl, 'reread')
            run(*ctl, 'update')
            run(*ctl, 'restart', *['codex-notify-' + role for role in programs])
        run(*ctl, 'status')
    print(json.dumps({'skill': str(skill), 'runtime': str(runtime), 'started': not args.no_start}))


if __name__ == '__main__':
    main()
