from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock
import uuid

from codex_notify import cli, store, workflow


class WorkflowAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.state_dir = root / "state"
        self.work_dir = root / "work"
        (self.work_dir / ".workflow" / "sessions").mkdir(parents=True)
        (self.work_dir / "orchestrator.log").write_text("", encoding="utf-8")
        self.db = store.connect(self.state_dir)
        self.thread = str(uuid.uuid4())

    def tearDown(self):
        self.db.close()
        self.temp_dir.cleanup()

    def write_status(self, tasks):
        status_path = self.work_dir / ".workflow" / "status.json"
        status_path.write_text(
            json.dumps({"tasks": tasks}, ensure_ascii=False), encoding="utf-8"
        )

    def register(self, identity_value="boot:live", pid=123, thread=None, stall_after=1800):
        with mock.patch.object(workflow, "identity", return_value=identity_value):
            return workflow.register(
                self.db,
                self.work_dir,
                thread or self.thread,
                pid,
                stall_after=stall_after,
            )

    def run_row(self, run_id):
        return self.db.execute(
            "SELECT * FROM workflow_runs WHERE id=?", (run_id,)
        ).fetchone()

    def notification(self, run_id, kind):
        return self.db.execute(
            """
            SELECT n.* FROM notifications AS n
            JOIN workflow_events AS e ON e.notification_id=n.id
            WHERE e.run_id=? AND e.kind=?
            """,
            (run_id, kind),
        ).fetchone()

    def test_dead_nonpassing_workflow_emits_stopped_and_deactivates(self):
        self.write_status({"compile": {"status": "failed"}})
        with mock.patch.object(
            workflow, "identity", side_effect=["boot:old", None]
        ):
            run_id = workflow.register(
                self.db, self.work_dir, self.thread, 123, stall_after=1800
            )
            run = self.run_row(run_id)
            workflow.observe(self.db, now=run["progress"] + 1.0)

        event = self.notification(run_id, "stopped")
        self.assertIsNotNone(event)
        self.assertEqual(json.loads(event["body"])["event"], "stopped")
        self.assertEqual(self.run_row(run_id)["active"], 0)

    def test_dead_all_passing_workflow_emits_completed(self):
        self.write_status(
            {
                "compile": {"status": "pass"},
                "tests": {"status": "pass"},
            }
        )
        with mock.patch.object(
            workflow, "identity", side_effect=["boot:old", None]
        ):
            run_id = workflow.register(
                self.db, self.work_dir, self.thread, 123, stall_after=1800
            )
            run = self.run_row(run_id)
            workflow.observe(self.db, now=run["progress"] + 1.0)

        event = self.notification(run_id, "completed")
        self.assertIsNotNone(event)
        self.assertEqual(json.loads(event["body"])["event"], "completed")
        self.assertEqual(self.run_row(run_id)["active"], 0)

    def test_pid_reuse_is_classified_as_stopped(self):
        self.write_status({"compile": {"status": "running"}})
        with mock.patch.object(
            workflow, "identity", side_effect=["boot:pid-one", "boot:pid-two"]
        ):
            run_id = workflow.register(
                self.db, self.work_dir, self.thread, 456, stall_after=1800
            )
            run = self.run_row(run_id)
            workflow.observe(self.db, now=run["progress"] + 1.0)

        self.assertIsNotNone(self.notification(run_id, "stopped"))
        self.assertEqual(self.run_row(run_id)["active"], 0)

    def test_stall_is_reported_and_progress_resolves_the_transient_event(self):
        self.write_status({"compile": {"status": "running", "step": 1}})
        with mock.patch.object(workflow, "identity", return_value="boot:live"):
            run_id = workflow.register(
                self.db, self.work_dir, self.thread, 123, stall_after=10
            )
            run = self.run_row(run_id)
            first_observe = run["progress"] + 1.0
            workflow.observe(self.db, now=first_observe)
            self.assertEqual(self.run_row(run_id)["progress"], first_observe)

            workflow.observe(self.db, now=first_observe + 11.0)
            stalled = self.notification(run_id, "stalled")
            self.assertIsNotNone(stalled)
            self.assertIsNone(stalled["resolved_at"])
            self.assertEqual(self.run_row(run_id)["active"], 1)

            # The same stalled observation is idempotent while no files change.
            workflow.observe(self.db, now=first_observe + 11.5)
            self.assertEqual(
                self.db.execute(
                    "SELECT COUNT(*) FROM workflow_events WHERE run_id=? AND kind='stalled'",
                    (run_id,),
                ).fetchone()[0],
                1,
            )

            self.write_status({"compile": {"status": "running", "step": 2000}})
            workflow.observe(self.db, now=first_observe + 12.0)

        stalled = self.notification(run_id, "stalled")
        self.assertIsNotNone(stalled["resolved_at"])
        self.assertEqual(self.run_row(run_id)["progress"], first_observe + 12.0)
        self.assertEqual(self.run_row(run_id)["active"], 1)

    def test_repeated_observation_errors_are_idempotent_until_progress_then_reopen(self):
        status_path = self.work_dir / ".workflow" / "status.json"
        with mock.patch.object(workflow, "identity", return_value="boot:live"):
            run_id = workflow.register(
                self.db, self.work_dir, self.thread, 123, stall_after=10
            )
            run = self.run_row(run_id)
            first_error = run["progress"] + 10.0

            # A sustained copy of the same outage does not enqueue duplicates.
            workflow.observe(self.db, now=first_error)
            workflow.observe(self.db, now=first_error + 1.0)
            self.assertEqual(
                self.db.execute(
                    "SELECT COUNT(*) FROM workflow_events WHERE run_id=? AND kind='observation_error'",
                    (run_id,),
                ).fetchone()[0],
                1,
            )
            first_event = self.notification(run_id, "observation_error")
            self.assertIsNotNone(first_event)

            # Progress clears the transient error and advances the token used
            # for the next independent outage.
            self.write_status({"compile": {"status": "running", "step": 1}})
            recovered = first_error + 2.0
            workflow.observe(self.db, now=recovered)
            self.assertIsNotNone(self.notification(run_id, "observation_error")["resolved_at"])

            status_path.unlink()
            second_error = recovered + 10.0
            workflow.observe(self.db, now=second_error)

        events = self.db.execute(
            """
            SELECT n.* FROM notifications AS n
            JOIN workflow_events AS e ON e.notification_id=n.id
            WHERE e.run_id=? AND e.kind='observation_error'
            ORDER BY n.created
            """,
            (run_id,),
        ).fetchall()
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["id"], first_event["id"])
        self.assertNotEqual(events[0]["id"], events[1]["id"])
        self.assertIsNotNone(events[0]["resolved_at"])
        self.assertIsNone(events[1]["resolved_at"])

    def test_duplicate_live_registration_is_idempotent_and_rejects_other_targets(self):
        with mock.patch.object(workflow, "identity", return_value="boot:live"):
            first = workflow.register(
                self.db, self.work_dir, self.thread, 123, stall_after=1800
            )
            repeated = workflow.register(
                self.db, self.work_dir, self.thread, 123, stall_after=1800
            )
            self.assertEqual(first, repeated)

            with self.assertRaisesRegex(ValueError, "different target"):
                workflow.register(
                    self.db,
                    self.work_dir,
                    str(uuid.uuid4()),
                    123,
                    stall_after=1800,
                )
            with self.assertRaisesRegex(ValueError, "already registered"):
                workflow.register(
                    self.db, self.work_dir, self.thread, 456, stall_after=1800
                )

        self.assertEqual(
            self.db.execute(
                "SELECT COUNT(*) FROM workflow_runs WHERE directory=? AND active=1",
                (str(self.work_dir.resolve()),),
            ).fetchone()[0],
            1,
        )

    def test_workflow_run_launches_once_then_refuses_duplicate_without_relaunching(self):
        marker = self.work_dir / "launch-marker"
        launches = []
        arguments = [
            "codex-notify",
            "--state-dir",
            str(self.state_dir),
            "workflow-run",
            "--work-dir",
            str(self.work_dir),
            "--thread",
            self.thread,
            "--",
            "command-that-must-not-start",
        ]

        class FakeChild:
            pid = 1234

            def wait(self):
                return 0

        def fake_popen(argv, **_kwargs):
            launches.append(argv)
            marker.write_text(marker.read_text(encoding="utf-8") + "launched\n" if marker.exists() else "launched\n", encoding="utf-8")
            return FakeChild()

        with (
            mock.patch.object(workflow, "identity", return_value="boot:live"),
            mock.patch.object(
                cli.subprocess, "Popen", side_effect=fake_popen
            ) as popen,
            mock.patch.object(sys, "argv", arguments),
        ):
            self.assertEqual(cli.main(), 0)
            self.assertEqual(marker.read_text(encoding="utf-8"), "launched\n")
            with self.assertRaisesRegex(ValueError, "already alive"):
                cli.main()

        self.assertEqual(len(launches), 1)
        self.assertEqual(popen.call_count, 1)
        self.assertEqual(marker.read_text(encoding="utf-8"), "launched\n")

    def test_workflow_event_can_be_emitted_without_an_existing_work_directory(self):
        workflow.schema(self.db)
        run = {
            "id": str(uuid.uuid4()),
            "directory": str(self.work_dir / "does-not-exist"),
            "thread": self.thread,
            "instructions": "inspect the recorded evidence",
        }

        event_id = workflow.emit(
            self.db, run, "external-observation", {"detail": "producer-only"}
        )
        row = self.db.execute(
            "SELECT * FROM notifications WHERE id=?", (event_id,)
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(json.loads(row["body"])["event"], "external-observation")
        self.assertFalse(Path(run["directory"]).exists())


if __name__ == "__main__":
    unittest.main()
