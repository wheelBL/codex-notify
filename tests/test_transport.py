"""Protocol-level tests for the standalone Codex notification transport."""

from __future__ import annotations

import json
import unittest
from unittest.mock import patch

import codex_notify.transport as transport_module
from codex_notify.transport import ProtocolError, Transport


THREAD = "01a08e41-01fa-7940-bd21-318b901518f9"
MARKER = "[workflow-event:6d3e6c1d-6aa6-4fb7-b7e0-7a9c6a12fd41]"
CLIENT_ID = "2b5a4c9e-7e89-4db0-a6af-17c8ed0dbf65"


class FakeSocket:
    def __init__(self, *, connect_error: Exception | None = None):
        self.connect_error = connect_error
        self.connected_to = None
        self.timeouts = []
        self.closed = False

    def settimeout(self, value):
        self.timeouts.append(value)

    def connect(self, path):
        if self.connect_error is not None:
            raise self.connect_error
        self.connected_to = path

    def close(self):
        self.closed = True


class FakeWebSocket:
    def __init__(self, mode="success"):
        self.mode = mode
        self.requests = []
        self.replies = []
        self.timeouts = []
        self.connected = False
        self.shutdown_calls = 0
        self.connect_error = None

    def connect(self, *args, **kwargs):
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True
        self.connect_args = (args, kwargs)

    def settimeout(self, value):
        self.timeouts.append(value)

    def send(self, raw):
        request = json.loads(raw)
        self.requests.append(request)
        if "id" not in request:
            return

        request_id = request["id"]
        method = request["method"]
        if method == "initialize":
            # An unrelated notification must be ignored while waiting for the
            # initialize response.
            self.replies.append(json.dumps({"method": "thread/queue/changed"}))
            result = {"protocolVersion": "2025-06-18"}
        elif method == "thread/queue/list":
            if self.mode == "malformed":
                self.replies.append("{not-json")
                return
            cursor = request["params"].get("cursor")
            if self.mode == "page2" and cursor is None:
                result = {"data": [{"text": "another event"}], "nextCursor": "page-2"}
            elif self.mode == "page2" and cursor == "page-2":
                result = {"data": [{"text": MARKER}], "nextCursor": None}
            else:
                result = {"data": [], "nextCursor": None}
        elif method == "thread/queue/add":
            if self.mode in {"lost", "closed"}:
                return
            returned_client_id = request["params"]["clientUserMessageId"]
            if self.mode == "wrong-client-id":
                returned_client_id = "a-different-client-id"
            result = {
                "queuedSubmission": {
                    "id": "queued-message-id",
                    "clientUserMessageId": returned_client_id,
                }
            }
        else:
            raise AssertionError(f"unexpected method: {method}")

        self.replies.append(json.dumps({"id": request_id, "result": result}))

    def recv(self):
        if self.mode == "closed" and not self.replies:
            return ""
        if self.mode == "lost" and not self.replies:
            raise TimeoutError("simulated response timeout")
        if not self.replies:
            raise AssertionError("test server has no reply queued")
        return self.replies.pop(0)

    def shutdown(self):
        self.shutdown_calls += 1


class TransportTests(unittest.TestCase):
    def run_transport(self, ws, stream=None):
        stream = stream or FakeSocket()
        socket_patch = patch.object(transport_module.socket, "socket", return_value=stream)
        websocket_patch = patch.object(transport_module.websocket, "WebSocket", return_value=ws)
        return socket_patch, websocket_patch, stream

    def test_page_two_marker_and_fixed_client_id(self):
        ws = FakeWebSocket("page2")
        socket_patch, websocket_patch, stream = self.run_transport(ws)
        with socket_patch, websocket_patch:
            with Transport("/tmp/codex.sock") as transport:
                self.assertTrue(transport.pending(THREAD, MARKER))
                self.assertEqual(transport.send(THREAD, "resume", CLIENT_ID), "queued-message-id")

        self.assertEqual(stream.connected_to, "/tmp/codex.sock")
        self.assertTrue(stream.closed)
        self.assertEqual(ws.shutdown_calls, 1)
        methods = [request.get("method") for request in ws.requests]
        self.assertEqual(methods[:2], ["initialize", "initialized"])
        list_requests = [r for r in ws.requests if r.get("method") == "thread/queue/list"]
        self.assertEqual(list_requests[0]["params"], {"threadId": THREAD, "limit": 100})
        self.assertEqual(list_requests[1]["params"]["cursor"], "page-2")
        add = next(r for r in ws.requests if r.get("method") == "thread/queue/add")
        self.assertEqual(add["params"]["clientUserMessageId"], CLIENT_ID)
        self.assertEqual(add["params"]["input"], [{"type": "text", "text": "resume", "text_elements": []}])

    def test_malformed_reply_closes_the_connection(self):
        ws = FakeWebSocket("malformed")
        socket_patch, websocket_patch, stream = self.run_transport(ws)
        with socket_patch, websocket_patch:
            with self.assertRaises(ProtocolError):
                with Transport("/tmp/codex.sock") as transport:
                    transport.pending(THREAD, MARKER)
        self.assertEqual(ws.shutdown_calls, 1)
        self.assertTrue(stream.closed)

    def test_acknowledgment_must_echo_the_fixed_client_id(self):
        ws = FakeWebSocket("wrong-client-id")
        socket_patch, websocket_patch, stream = self.run_transport(ws)
        with socket_patch, websocket_patch:
            with self.assertRaisesRegex(ProtocolError, "identity mismatch"):
                with Transport("/tmp/codex.sock") as transport:
                    transport.send(THREAD, "resume", CLIENT_ID)
        self.assertTrue(stream.closed)

    def test_lost_reply_times_out_once_and_closes(self):
        ws = FakeWebSocket("lost")
        socket_patch, websocket_patch, stream = self.run_transport(ws)
        with socket_patch, websocket_patch:
            with self.assertRaises(TimeoutError):
                with Transport("/tmp/codex.sock") as transport:
                    transport.send(THREAD, "resume", CLIENT_ID)
        adds = [r for r in ws.requests if r.get("method") == "thread/queue/add"]
        self.assertEqual(len(adds), 1)
        self.assertEqual(ws.shutdown_calls, 1)
        self.assertTrue(stream.closed)

    def test_empty_reply_is_connection_loss_and_closes(self):
        ws = FakeWebSocket("closed")
        socket_patch, websocket_patch, stream = self.run_transport(ws)
        with socket_patch, websocket_patch:
            with self.assertRaises(ConnectionError):
                with Transport("/tmp/codex.sock") as transport:
                    transport.send(THREAD, "resume", CLIENT_ID)
        self.assertEqual(ws.shutdown_calls, 1)
        self.assertTrue(stream.closed)

    def test_failed_socket_connection_is_closed(self):
        stream = FakeSocket(connect_error=OSError("missing socket"))
        ws = FakeWebSocket()
        socket_patch, websocket_patch, _ = self.run_transport(ws, stream)
        with socket_patch, websocket_patch:
            with self.assertRaises(OSError):
                with Transport("/tmp/missing.sock"):
                    pass
        self.assertTrue(stream.closed)
        self.assertEqual(ws.shutdown_calls, 0)

    def test_failed_websocket_handshake_closes_both_objects(self):
        stream = FakeSocket()
        ws = FakeWebSocket()
        ws.connect_error = OSError("handshake failed")
        socket_patch, websocket_patch, _ = self.run_transport(ws, stream)
        with socket_patch, websocket_patch:
            with self.assertRaises(OSError):
                with Transport("/tmp/codex.sock"):
                    pass
        self.assertEqual(ws.shutdown_calls, 1)
        self.assertTrue(stream.closed)

    def test_deadline_is_shared_across_initialize_and_queue_call(self):
        ws = FakeWebSocket()
        socket_patch, websocket_patch, _ = self.run_transport(ws)
        clock = [0.0]

        def monotonic():
            value = clock[0]
            clock[0] += 0.25
            return value

        with patch.object(transport_module, "RPC_TIMEOUT", 10.0), \
                patch.object(transport_module.time, "monotonic", side_effect=monotonic), \
                socket_patch, websocket_patch:
            with Transport("/tmp/codex.sock") as transport:
                self.assertFalse(transport.pending(THREAD, MARKER))

        self.assertGreater(len(ws.timeouts), 3)
        self.assertEqual(ws.timeouts, sorted(ws.timeouts, reverse=True))
        self.assertLess(ws.timeouts[-1], ws.timeouts[0])


if __name__ == "__main__":
    unittest.main()
