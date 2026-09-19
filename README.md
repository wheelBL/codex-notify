# codex-notify

Reliable notifications for Codex tasks: durable enqueueing, retry, queue reconciliation,
and explicit receiver acknowledgment. Send build results, job failures, reminders from
your own scheduler, or other authorized events. No workflow directory is required.

**Status:** early Linux release. Uses an experimental local Codex app-server queue
protocol, initially verified against Codex 0.154.0. Not an official OpenAI product.
A successful enqueue or handshake is not proof that a task handled a notification.

## Install

Requires Linux, Python 3.10+ with venv/pip, dependency download access (or a configured
wheel mirror), and an existing Codex app control socket on the same machine.

```bash
git clone https://github.com/wheelBL/codex-notify.git
cd codex-notify
python3 scripts/install.py
```

The installer creates an isolated environment and Supervisor service under
`${XDG_DATA_HOME:-$HOME/.local/share}/codex-notify` and installs the Skill under
`${CODEX_HOME:-$HOME/.codex}/skills/codex-notify`. Use `--runtime-dir` or
`--codex-home` to override. `--no-start` stages the installation without starting
workers. Repeat the command to upgrade, preserving the notification database.
The installed Skill's `deployment.json` records machine-local paths. Do not copy
that file or runtime state to other machines.

A release archive can be extracted and installed with the same command. Verify its
SHA256 file first. The package includes its transport implementation; no personal
plugin, cloud API key, or separate Codex CLI session is needed. Runtime Python
dependencies are installed with pip; this is not an air-gapped bundle.

## Send a notification

First obtain the exact UUID of a task on this Codex app and the user's authorization
to notify it. The CLI does not infer targets from display names.

```bash
RUNTIME="${XDG_DATA_HOME:-$HOME/.local/share}/codex-notify"
CLI="$RUNTIME/venv/bin/codex-notify"
printf '%s\n' 'Build 123 finished. Review the saved build report.' > /tmp/build-message.txt
"$CLI" --state-dir "$RUNTIME/state" enqueue \
  --thread "$THREAD_UUID" --source build --key build-123 \
  --body-file /tmp/build-message.txt
"$CLI" --state-dir "$RUNTIME/state" status
```

`enqueue` returns a durable event ID immediately. A source + target + key identifies
one notification. Retrying identical input returns the same ID; changed content with
the same key is rejected. Use a new key for a new event. Message bodies are UTF-8,
nonempty, and limited to 64,000 bytes; link larger evidence instead.

The receiving task gets commands for:

```bash
"$CLI" --state-dir "$RUNTIME/state" ack "$EVENT_ID"
# Handle within existing user authorization, then verify the result.
"$CLI" --state-dir "$RUNTIME/state" resolve "$EVENT_ID"
```

`ack` means ownership; `resolve` means handling is complete. Both are idempotent.
Notification text does not grant extra permission or override task instructions.
The global Skill teaches duplicate handling and this distinction. Existing sessions
may need to reload Skills or explicitly read its SKILL.md.

## Architecture and guarantees

```text
Any script / CLI / optional workflow producer
                   |
                   v
      SQLite outbox (WAL + FULL sync)
                   |
      supervised delivery worker
                   |
      Codex task queue over local socket
                   |
           receiver ack -> resolve
```

- `store.py` owns only notifications, deduplication and acknowledgment.
- `delivery.py` owns retry and queue reconciliation; it knows no workflow schema.
- `transport.py` owns the experimental Codex queue protocol.
- `workflow.py` is an optional producer with its own run observations.

Delivery is **at-least-once**, not exactly-once. Intent and retry state are committed
before network calls. A lost response is reconciled by paging the target queue for
the event marker. A consumed but unacknowledged message can be sent again, so receivers
must make side effects idempotent. Fixed client IDs are correlation identifiers, not
a claim of server-side deduplication.

Transport failures back off from 5 seconds to at most 5 minutes. Successful sends
and acknowledgments get a 30-minute handling grace period; unresolved events become
eligible again afterward. A still-pending queue marker is rechecked after 30 seconds.
Receiver completion suppresses subsequent delivery; an in-flight send can still race
with acknowledgment. `status` exposes attempts, errors and delivery/ack/resolve times.

Supervisor restarts workers after a crash. Separate workflow and delivery workers
prevent network stalls from blocking observation. A role lock prevents duplicate
workers on one state directory. This covers transient failures while the container
or host and its persistent filesystem remain available. It does **not** guarantee
startup after host/container reboot, recovery after all supervisors are killed,
or delivery while Codex is permanently unavailable. Use your external service manager
if you need a stronger lifecycle guarantee. Keep state on a local reliable filesystem.

## Optional workflow adapter

```bash
python3 scripts/install.py --workflow-adapter
"$CLI" --state-dir "$RUNTIME/state" workflow-register \
  --work-dir "$WORK_DIR" --thread "$THREAD_UUID" --pid "$PID" \
  --instructions 'Inspect the failure; repair and resume only within the authorized task.'
```

The adapter reads `.workflow/status.json` with a nonempty `tasks` mapping, e.g.
`{"tasks":{"build":{"status":"pass"}}}`. After process exit, all tasks must be
`pass` to classify completion; otherwise it reports stopped. It cannot recover an
unrecorded exit code. PID identity includes Linux boot ID and process start time.
Status/log progress updates clear transient stall alerts; stalled or persistently
unreadable state generates a diagnostic notification without killing jobs.

For a new job, use `workflow-run --work-dir ... --thread ... -- COMMAND ARGS...`.
It records the process and rejects a second registered live execution in the same
directory. Starting outside this wrapper requires explicit registration. This is
not a distributed lock across machines or unrelated state directories.

A replacement run closes the previous recovery request, not its correctness claim.
The adapter never changes authoritative workflow state or marks acceptance tests passed.
When migrating from `workflow-watch`, handle pending old events, stop its workers,
retain its database for audit, then register the existing process here. Do not have
both monitors observe the same job. The new core has no dependency on the old service.

## Operations and validation

```bash
"$CLI" --state-dir "$RUNTIME/state" doctor
"$RUNTIME/venv/bin/supervisorctl" -c "$RUNTIME/supervisord.conf" status
# Stop notifications; does not stop the underlying jobs.
"$RUNTIME/venv/bin/supervisorctl" -c "$RUNTIME/supervisord.conf" stop all
python3 -m unittest discover -s tests -v
python3 scripts/package.py --output dist
```

For a custom Codex home, pass `doctor --socket /path/to/app-server-control.sock`.
Doctor checks a connection and delivery heartbeat; it never sends a message.
For end-to-end acceptance, enqueue an authorized self-test instructing the receiver
to only ack/resolve, then verify all three timestamps in `status`. Do not use a
production restart as a notification test. Repeat actual receipt testing when the
Codex protocol/version changes. Unit tests use fake transports and temporary state.

CI runs tests and builds a reproducible archive. No credentials, task UUIDs,
databases, logs, venvs or machine-local installation paths are packaged.

## License

MIT. Dependencies are installed separately under their own licenses:
`websocket-client` (Apache-2.0) and `supervisor` (BSD-derived). This project does not
bundle the personal Thread Messaging MCP plugin or CANN/operator sources.
