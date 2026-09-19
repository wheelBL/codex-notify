"""Optional producer for .workflow/status.json. Never used by the outbox core."""
import hashlib
import json
from pathlib import Path
import time
import uuid
from .store import enqueue


def identity(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        if fields[0] in ('Z', 'X'):
            return None
        return Path('/proc/sys/kernel/random/boot_id').read_text().strip() + ':' + fields[19]
    except (OSError, IndexError):
        return None


def schema(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS workflow_runs (
      id TEXT PRIMARY KEY, directory TEXT NOT NULL, thread TEXT NOT NULL,
      pid INTEGER NOT NULL, identity TEXT, active INTEGER NOT NULL,
      progress REAL NOT NULL, signature TEXT, stall_after REAL NOT NULL,
      instructions TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS workflow_events (
      run_id TEXT NOT NULL, kind TEXT NOT NULL, notification_id TEXT UNIQUE NOT NULL);
    ''')


def register(db, directory, thread, pid, stall_after=1800, instructions=''):
    schema(db)
    directory = str(Path(directory).resolve())
    thread = str(uuid.UUID(thread))
    if pid <= 0 or stall_after <= 0:
        raise ValueError('PID and stall interval must be positive')
    process = identity(pid)
    # Registration is serialized across clients, including duplicate repair turns.
    db.execute('BEGIN IMMEDIATE')
    try:
        old = list(db.execute('SELECT * FROM workflow_runs WHERE directory=? AND active=1', (directory,)))
        for row in old:
            if process and row['pid'] == pid and row['identity'] == process:
                if thread != row['thread']:
                    raise ValueError('Existing run belongs to a different target')
                db.commit()
                return row['id']
            if row['identity'] and identity(row['pid']) == row['identity']:
                raise ValueError('A live workflow is already registered')
        run_id = str(uuid.uuid4())
        db.execute('INSERT INTO workflow_runs VALUES(?,?,?,?,?,?,?,NULL,?,?)',
                   (run_id, directory, thread, pid, process, 1, time.time(), stall_after, instructions))
        db.execute('UPDATE workflow_runs SET active=0 WHERE directory=? AND id!=?', (directory, run_id))
        # Replacement closes the prior recovery request, not an acceptance gate.
        db.execute('''UPDATE notifications SET resolved_at=COALESCE(resolved_at,?) WHERE id IN
          (SELECT notification_id FROM workflow_events JOIN workflow_runs ON run_id=workflow_runs.id
           WHERE directory=? AND run_id!=?)''', (time.time(), directory, run_id))
        db.commit()
        return run_id
    except BaseException:
        db.rollback()
        raise


def emit(db, run, kind, detail, token=''):
    # Persist the producer-to-outbox mapping. Idempotent enqueue also repairs a
    # crash between committing the outbox and committing this derived mapping.
    body = json.dumps({'event': kind, 'run': run['id'], 'work_dir': run['directory'],
                       'evidence': detail, 'instructions': run['instructions']}, ensure_ascii=False)
    event_id = enqueue(db, run['thread'], body, run['id'] + ':' + kind + ':' + token, 'workflow')
    with db:
        db.execute('INSERT OR IGNORE INTO workflow_events VALUES(?,?,?)', (run['id'], kind, event_id))
    return event_id


def clear_transient(db, run_id):
    with db:
        db.execute('''UPDATE notifications SET resolved_at=COALESCE(resolved_at,?) WHERE id IN
          (SELECT notification_id FROM workflow_events WHERE run_id=? AND kind IN ('stalled','observation_error'))''',
                   (time.time(), run_id))


def observe(db, now=None):
    schema(db)
    now = time.time() if now is None else now
    for run in db.execute('SELECT * FROM workflow_runs WHERE active=1').fetchall():
        root = Path(run['directory'])
        try:
            state_path = root / '.workflow/status.json'
            state = json.loads(state_path.read_text())
            tasks = state['tasks']
            if not isinstance(tasks, dict) or not tasks or not all(isinstance(t, dict) for t in tasks.values()):
                raise ValueError('Expected a nonempty tasks object')
            files = [state_path, root / 'orchestrator.log', *(root / '.workflow/sessions').rglob('*.jsonl')]
            signature = hashlib.sha256(json.dumps([(str(p), p.stat().st_size, p.stat().st_mtime_ns)
                                                  for p in files if p.exists()]).encode()).hexdigest()
            if signature != run['signature']:
                with db:
                    db.execute('UPDATE workflow_runs SET progress=?,signature=? WHERE id=?', (now, signature, run['id']))
                clear_transient(db, run['id'])
                run = db.execute('SELECT * FROM workflow_runs WHERE id=?', (run['id'],)).fetchone()
            if not run['identity'] or identity(run['pid']) != run['identity']:
                kind = 'completed' if all(t.get('status') == 'pass' for t in tasks.values()) else 'stopped'
                # Reference full evidence on disk; keep large workflows within the
                # generic message size bound and make retries byte-for-byte stable.
                emit(db, run, kind, {'status_path': str(state_path),
                                    'note': 'Process exited; classification uses recorded task states, not exit code.'})
                clear_transient(db, run['id'])
                with db:
                    db.execute('UPDATE workflow_runs SET active=0 WHERE id=?', (run['id'],))
            elif now - run['progress'] >= run['stall_after']:
                emit(db, run, 'stalled', {'note': 'No observed progress; inspect without automatically killing work.'}, signature)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            if now - run['progress'] >= min(60, run['stall_after']):
                emit(db, run, 'observation_error', {'status_path': str(root / '.workflow/status.json'),
                                                'note': 'Sustained unreadable or invalid workflow state; inspect source.'}, str(run['progress']))
