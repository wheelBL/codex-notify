from __future__ import annotations

from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
import uuid

from codex_notify import delivery, store


class FakeTransport:
    def __init__(self, pending=False, message_id="queued-message"):
        self.pending_result = pending
        self.message_id = message_id
        self.pending_calls = []
        self.send_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def pending(self, thread, marker):
        self.pending_calls.append((thread, marker))
        return self.pending_result

    def send(self, thread, message, client_id):
        self.send_calls.append((thread, message, client_id))
        return self.message_id


class CoreTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temp_dir.name) / "state"
        self.db = store.connect(self.state_dir)
        self.thread = str(uuid.uuid4())

    def tearDown(self):
        self.db.close()
        self.temp_dir.cleanup()

    def row(self, event_id):
        return self.db.execute(
            "SELECT * FROM notifications WHERE id=?", (event_id,)
        ).fetchone()

    def test_external_producer_needs_no_workflow_state_or_working_directory(self):
        event_id = store.enqueue(
            self.db,
            self.thread,
            "external producer event",
            "external-event",
            source="external",
            now=100.0,
        )

        tables = {
            row[0]
            for row in self.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertNotIn("workflow_runs", tables)
        self.assertNotIn("workflow_events", tables)
        self.assertEqual(store.status(self.db)["notifications"][0]["id"], event_id)
        self.assertEqual(self.row(event_id)["source"], "external")

    def test_notification_survives_database_reopen(self):
        event_id = store.enqueue(
            self.db,
            self.thread,
            "persisted event",
            "persisted-event",
            now=100.0,
        )
        store.acknowledge(self.db, event_id, now=110.0, grace=60.0)
        self.db.close()

        self.db = store.connect(self.state_dir)
        row = self.row(event_id)
        self.assertIsNotNone(row)
        self.assertEqual(row["body"], "persisted event")
        self.assertEqual(row["acknowledged_at"], 110.0)
        self.assertEqual(row["next_attempt"], 170.0)

    def test_same_source_thread_key_rejects_changed_body_but_scope_isolated(self):
        event_id = store.enqueue(
            self.db,
            self.thread,
            "original body",
            "stable-key",
            source="producer-a",
            now=100.0,
        )

        with self.assertRaisesRegex(ValueError, "different content"):
            store.enqueue(
                self.db,
                self.thread,
                "changed body",
                "stable-key",
                source="producer-a",
                now=101.0,
            )

        other_source = store.enqueue(
            self.db,
            self.thread,
            "other source body",
            "stable-key",
            source="producer-b",
            now=102.0,
        )
        other_thread = store.enqueue(
            self.db,
            str(uuid.uuid4()),
            "other thread body",
            "stable-key",
            source="producer-a",
            now=103.0,
        )
        self.assertEqual(self.row(event_id)["body"], "original body")
        self.assertNotEqual(event_id, other_source)
        self.assertNotEqual(event_id, other_thread)

    def test_lost_reply_is_reconciled_from_queue_without_resending(self):
        event_id = store.enqueue(
            self.db,
            self.thread,
            "message accepted before the reply was lost",
            "lost-reply",
            now=100.0,
        )

        class ResponseLostTransport(FakeTransport):
            def __init__(self):
                super().__init__()
                self.accepted_markers = set()

            def pending(self, thread, marker):
                self.pending_calls.append((thread, marker))
                return marker in self.accepted_markers

            def send(self, thread, message, client_id):
                self.send_calls.append((thread, message, client_id))
                self.accepted_markers.add(message.splitlines()[0])
                raise ConnectionError("response lost after queue acceptance")

        transport = ResponseLostTransport()
        with mock.patch("builtins.print"):
            delivery.dispatch(self.db, self.state_dir, lambda: transport, now=100.0)
        first = self.row(event_id)
        self.assertEqual(first["attempts"], 1)
        self.assertEqual(len(transport.send_calls), 1)
        self.assertEqual(first["next_attempt"], 105.0)

        delivery.dispatch(self.db, self.state_dir, lambda: transport, now=105.0)
        reconciled = self.row(event_id)
        self.assertEqual(reconciled["attempts"], 2)
        self.assertEqual(len(transport.send_calls), 1)
        self.assertEqual(len(transport.pending_calls), 2)
        self.assertIsNotNone(reconciled["delivered_at"])
        self.assertIsNone(reconciled["message_id"])
        self.assertIsNone(reconciled["last_error"])

    def test_delivery_failure_backoff_survives_database_reopen(self):
        event_id = store.enqueue(
            self.db,
            self.thread,
            "retry after restart",
            "restart-retry",
            now=100.0,
        )

        class DisconnectedTransport:
            def __enter__(self):
                raise ConnectionError("transport unavailable")

            def __exit__(self, *_exc):
                return False

        with mock.patch("builtins.print"):
            delivery.dispatch(
                self.db,
                self.state_dir,
                lambda: DisconnectedTransport(),
                now=100.0,
            )
        failed = self.row(event_id)
        self.assertEqual(failed["attempts"], 1)
        self.assertEqual(failed["next_attempt"], 105.0)
        self.assertIn("transport unavailable", failed["last_error"])

        self.db.close()
        self.db = store.connect(self.state_dir)
        transport = FakeTransport(message_id="retry-after-restart")
        factory_calls = []

        def factory():
            factory_calls.append(True)
            return transport

        delivery.dispatch(self.db, self.state_dir, factory, now=104.99)
        self.assertEqual(factory_calls, [])
        delivery.dispatch(self.db, self.state_dir, factory, now=105.0)
        retried = self.row(event_id)
        self.assertEqual(len(factory_calls), 1)
        self.assertEqual(retried["attempts"], 2)
        self.assertEqual(retried["message_id"], "retry-after-restart")

    def test_acknowledge_repeats_without_resolving_and_resolve_is_distinct(self):
        event_id = store.enqueue(
            self.db,
            self.thread,
            "acknowledgment test",
            "acknowledgment",
            now=100.0,
        )

        first_ack = store.acknowledge(
            self.db, event_id, now=110.0, grace=30.0
        )
        repeated_ack = store.acknowledge(
            self.db, event_id, now=120.0, grace=30.0
        )
        self.assertTrue(first_ack["first"])
        self.assertFalse(first_ack["resolved"])
        self.assertFalse(repeated_ack["first"])
        self.assertFalse(repeated_ack["resolved"])
        self.assertEqual(self.row(event_id)["acknowledged_at"], 110.0)
        self.assertIsNone(self.row(event_id)["resolved_at"])

        first_resolve = store.acknowledge(
            self.db, event_id, resolve=True, now=130.0
        )
        repeated_resolve = store.acknowledge(
            self.db, event_id, resolve=True, now=140.0
        )
        row = self.row(event_id)
        self.assertTrue(first_resolve["first"])
        self.assertTrue(first_resolve["resolved"])
        self.assertFalse(repeated_resolve["first"])
        self.assertTrue(repeated_resolve["resolved"])
        self.assertEqual(row["acknowledged_at"], 110.0)
        self.assertEqual(row["resolved_at"], 130.0)

    def test_acknowledgment_defers_retry_until_grace_expires(self):
        event_id = store.enqueue(
            self.db,
            self.thread,
            "grace period test",
            "grace-period",
            now=100.0,
        )
        store.acknowledge(self.db, event_id, now=100.0, grace=60.0)
        transport = FakeTransport(message_id="grace-message")
        factory_calls = []

        def factory():
            factory_calls.append(True)
            return transport

        delivery.dispatch(self.db, self.state_dir, factory, now=159.99)
        self.assertEqual(factory_calls, [])

        delivery.dispatch(self.db, self.state_dir, factory, now=160.0)
        row = self.row(event_id)
        self.assertEqual(len(factory_calls), 1)
        self.assertEqual(row["attempts"], 1)
        self.assertEqual(row["message_id"], "grace-message")
        self.assertEqual(row["delivered_at"], 160.0)

    def test_resolve_committed_during_queue_check_suppresses_send(self):
        event_id = store.enqueue(
            self.db,
            self.thread,
            "resolve race test",
            "resolve-race",
            now=100.0,
        )
        resolve_started = threading.Event()
        resolve_finished = threading.Event()
        resolver_errors = []

        def resolver():
            resolve_started.wait(5.0)
            connection = store.connect(self.state_dir)
            try:
                store.acknowledge(connection, event_id, resolve=True, now=101.0)
            except BaseException as exc:  # surfaced below after the worker joins
                resolver_errors.append(exc)
            finally:
                connection.close()
                resolve_finished.set()

        resolver_thread = threading.Thread(target=resolver)
        resolver_thread.start()

        class ResolveDuringPending(FakeTransport):
            def pending(self, thread, marker):
                self.pending_calls.append((thread, marker))
                resolve_started.set()
                if not resolve_finished.wait(5.0):
                    raise RuntimeError("resolver did not commit")
                return False

            def send(self, thread, message, client_id):
                self.send_calls.append((thread, message, client_id))
                raise AssertionError("resolved notification must not be sent")

        transport = ResolveDuringPending()
        with mock.patch("builtins.print"):
            delivery.dispatch(self.db, self.state_dir, lambda: transport, now=100.0)
        resolver_thread.join(5.0)

        self.assertFalse(resolver_thread.is_alive())
        self.assertEqual(resolver_errors, [])
        row = self.row(event_id)
        self.assertIsNotNone(row["resolved_at"])
        self.assertEqual(row["resolved_at"], 101.0)
        self.assertEqual(transport.send_calls, [])
        self.assertIsNone(row["delivered_at"])


if __name__ == "__main__":
    unittest.main()
