"""Producer-neutral CLI. Workflow observation is an explicit optional command."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from . import store
from .delivery import dispatch, worker_lock


def state_default():
    return Path(os.environ.get('XDG_STATE_HOME', str(Path.home() / '.local/state'))) / 'codex-notify'


def socket_default():
    return Path(os.environ.get('CODEX_HOME', str(Path.home() / '.codex'))) / 'app-server-control/app-server-control.sock'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, default=state_default())
    commands = parser.add_subparsers(dest='command', required=True)
    send = commands.add_parser('enqueue', help='Persist a notification; service delivers it asynchronously')
    send.add_argument('--thread', required=True)
    send.add_argument('--body-file', type=Path, required=True, help='UTF-8 message file')
    send.add_argument('--key', required=True, help='Stable idempotency key within source + target')
    send.add_argument('--source', default='manual')
    for action in ('ack', 'resolve'):
        commands.add_parser(action).add_argument('id')
    commands.add_parser('status')
    serve = commands.add_parser('serve')
    serve.add_argument('--socket', type=Path, default=socket_default())
    serve.add_argument('--poll', type=float, default=10)
    serve.add_argument('--once', action='store_true')
    doctor = commands.add_parser('doctor', help='Read-only transport handshake and worker health')
    doctor.add_argument('--socket', type=Path, default=socket_default())
    for action in ('workflow-register', 'workflow-run'):
        wf = commands.add_parser(action)
        wf.add_argument('--work-dir', type=Path, required=True)
        wf.add_argument('--thread', required=True)
        wf.add_argument('--stall-after', type=float, default=1800)
        wf.add_argument('--instructions', default='Inspect the workflow state and act only within existing user authorization.')
        if action == 'workflow-register':
            wf.add_argument('--pid', type=int, required=True)
        else:
            wf.add_argument('argv', nargs=argparse.REMAINDER)
    watch = commands.add_parser('workflow-watch')
    watch.add_argument('--poll', type=float, default=10)
    watch.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if hasattr(args, 'poll') and args.poll <= 0:
        parser.error('--poll must be positive')
    db = store.connect(args.state_dir)
    if args.command == 'enqueue':
        print(json.dumps({'id': store.enqueue(db, args.thread, args.body_file.read_text(encoding='utf-8'), args.key, args.source)}))
    elif args.command in ('ack', 'resolve'):
        print(json.dumps(store.acknowledge(db, args.id, resolve=args.command == 'resolve')))
    elif args.command == 'status':
        print(json.dumps(store.status(db), ensure_ascii=False, indent=2))
    elif args.command == 'doctor':
        from .transport import Transport
        with Transport(args.socket):
            pass
        health = dict(db.execute('SELECT role,heartbeat FROM health'))
        fresh = time.time() - health.get('delivery', 0) < 120
        print(json.dumps({'handshake': 'pass', 'delivery_heartbeat_fresh': fresh, 'health': health,
                          'note': 'Handshake is not proof of receiving-task acknowledgment.'}))
        return 0 if fresh else 1
    elif args.command in ('serve', 'workflow-watch'):
        role = 'delivery' if args.command == 'serve' else 'workflow'
        with worker_lock(args.state_dir, role):
            while True:
                if role == 'delivery':
                    from .transport import Transport
                    dispatch(db, args.state_dir, lambda: Transport(args.socket))
                else:
                    from .workflow import observe
                    observe(db)
                with db:
                    db.execute('INSERT OR REPLACE INTO health VALUES(?,?)', (role, time.time()))
                if args.once:
                    break
                time.sleep(args.poll)
    else:
        from . import workflow
        if args.command == 'workflow-register':
            print(workflow.register(db, args.work_dir, args.thread, args.pid, args.stall_after, args.instructions))
        else:
            argv = args.argv[1:] if args.argv[:1] == ['--'] else args.argv
            if not argv:
                parser.error('workflow-run needs a command after --')
            work = args.work_dir.resolve()
            work.mkdir(parents=True, exist_ok=True)
            with worker_lock(work, 'codex-notify-run'):
                workflow.schema(db)
                for old in db.execute('SELECT * FROM workflow_runs WHERE directory=? AND active=1', (str(work),)):
                    if old['identity'] and workflow.identity(old['pid']) == old['identity']:
                        raise ValueError('Registered workflow already alive')
                with (work / 'orchestrator.log').open('a') as log:
                    child = subprocess.Popen(argv, cwd=work, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                    try:
                        run_id = workflow.register(db, work, args.thread, child.pid, args.stall_after, args.instructions)
                    except BaseException:
                        os.killpg(child.pid, signal.SIGTERM)
                        child.wait()
                        raise
                    print(json.dumps({'run': run_id, 'pid': child.pid}), flush=True)
                    return child.wait()
    return 0
