"""The durable outbox: no process, workflow, or operator concepts."""
import json
from pathlib import Path
import sqlite3
import time
import uuid


def connect(directory):
    directory = Path(directory).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect(directory / 'notify.sqlite3', timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS notifications (
      id TEXT PRIMARY KEY, source TEXT NOT NULL, dedupe_key TEXT NOT NULL,
      thread TEXT NOT NULL, body TEXT NOT NULL, created REAL NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL,
      message_id TEXT, delivered_at REAL, acknowledged_at REAL, resolved_at REAL,
      last_error TEXT, UNIQUE(source, thread, dedupe_key));
    CREATE TABLE IF NOT EXISTS health (role TEXT PRIMARY KEY, heartbeat REAL NOT NULL);
    ''')
    return db


def enqueue(db, thread, body, key, source='manual', now=None):
    thread = str(uuid.UUID(thread))
    if not body.strip() or not key.strip() or not source.strip():
        raise ValueError('body, idempotency key, and source must be nonempty')
    if len(body.encode('utf-8')) > 64000:
        raise ValueError('body exceeds 64000 UTF-8 bytes; reference an artifact instead')
    now = time.time() if now is None else now
    identity = json.dumps([source, thread, key], ensure_ascii=False)
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, 'codex-notify:' + identity))
    with db:
        db.execute('''INSERT OR IGNORE INTO notifications
          (id,source,dedupe_key,thread,body,created,next_attempt) VALUES(?,?,?,?,?,?,?)''',
                   (event_id, source, key, thread, body, now, now))
        row = db.execute('SELECT * FROM notifications WHERE id=?', (event_id,)).fetchone()
        if row['body'] != body:
            raise ValueError('Idempotency key already exists with different content')
    return event_id


def acknowledge(db, event_id, resolve=False, now=None, grace=1800):
    now = time.time() if now is None else now
    column = 'resolved_at' if resolve else 'acknowledged_at'
    with db:
        row = db.execute('SELECT * FROM notifications WHERE id=?', (event_id,)).fetchone()
        if row is None:
            raise ValueError('Unknown notification')
        db.execute(f'UPDATE notifications SET {column}=COALESCE({column},?),next_attempt=? WHERE id=?',
                   (now, now + grace, event_id))
    return {'id': event_id, 'first': row[column] is None,
            'resolved': resolve or row['resolved_at'] is not None}


def status(db):
    return {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)]
            for table in ('notifications', 'health')}
