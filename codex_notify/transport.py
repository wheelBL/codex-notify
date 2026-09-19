"""Small client for the Codex app-server queue over a Unix socket.

The app-server exposes a WebSocket handshake on a Unix stream socket.  This
module deliberately contains only the transport operations needed by the
notification service: initialization, queue inspection, and queue insertion.
It does not start an app-server or depend on any Codex plugin.
"""

from __future__ import annotations

import json
import socket
import time
from typing import Any

import websocket


RPC_TIMEOUT = 30.0
CLIENT_INFO = {"name": "codex-notify", "version": "0.1.0"}


class TransportError(RuntimeError):
    """Base class for app-server transport failures."""


class ProtocolError(TransportError):
    """The app-server returned a reply outside the expected protocol shape."""


class RpcError(TransportError):
    """The app-server returned an error for an RPC request."""


class Transport:
    """Connect to an existing Codex app-server and queue notifications.

    A single deadline covers the socket connection, initialization, and all
    operations performed while the context is open.  The caller supplies the
    idempotency value used by ``thread/queue/add``; the returned acknowledgment
    must carry that same value before a message id is accepted.
    """

    def __init__(self, socket_path: str):
        self.socket_path = str(socket_path)
        self._socket: socket.socket | None = None
        self._ws: websocket.WebSocket | None = None
        self._deadline: float | None = None
        self._sequence = 0

    def __enter__(self) -> "Transport":
        if self._socket is not None or self._ws is not None:
            raise RuntimeError("transport is already open")

        self._deadline = time.monotonic() + RPC_TIMEOUT
        try:
            stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._socket = stream
            stream.settimeout(self._remaining())
            stream.connect(self.socket_path)

            ws = websocket.WebSocket()
            self._ws = ws
            ws.connect(
                "ws://localhost/",
                socket=stream,
                timeout=self._remaining(),
            )
            self._call(
                "initialize",
                {
                    "clientInfo": CLIENT_INFO,
                    "capabilities": {"experimentalApi": True},
                },
            )
            self._send_notification("initialized")
            return self
        except BaseException:
            # __enter__ is not paired with __exit__ when initialization fails.
            # Close both objects here so a failed connection cannot leak the
            # Unix descriptor or a partially-created WebSocket.
            self.close()
            raise

    def __exit__(self, *_exc: object) -> bool:
        self.close()
        return False

    def close(self) -> None:
        """Close the WebSocket and underlying socket, including partial opens."""

        ws, stream = self._ws, self._socket
        self._ws = None
        self._socket = None
        self._deadline = None

        if ws is not None:
            # websocket-client exposes shutdown(); small test doubles and some
            # compatible implementations expose close() instead.
            for name in ("shutdown", "close"):
                closer = getattr(ws, name, None)
                if not callable(closer):
                    continue
                try:
                    closer()
                except Exception:
                    # The descriptor still needs closing even if WebSocket
                    # teardown reports that its peer already disappeared.
                    continue
                break

        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass

    def pending(self, thread: str, marker: str) -> bool:
        """Return whether an exact marker occurs in the thread queue.

        ``thread/queue/list`` is cursor-paginated.  Only the returned ``data``
        is searched, so a marker-like cursor or envelope field cannot count as
        a queued message.  A repeated cursor is treated as a malformed reply
        instead of looping forever.
        """

        self._require_text(thread, "thread")
        self._require_text(marker, "marker")

        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            params: dict[str, Any] = {"threadId": thread, "limit": 100}
            if cursor is not None:
                params["cursor"] = cursor
            reply = self._call("thread/queue/list", params)
            if not isinstance(reply, dict):
                raise ProtocolError("queue/list result must be an object")

            data = reply.get("data")
            if not isinstance(data, list):
                raise ProtocolError("queue/list result data must be a list")
            if self._contains_marker(data, marker):
                return True

            next_cursor = reply.get("nextCursor")
            if next_cursor is None:
                return False
            if not isinstance(next_cursor, str) or not next_cursor:
                raise ProtocolError("queue/list nextCursor must be a non-empty string or null")
            if next_cursor in seen_cursors:
                raise ProtocolError("queue pagination repeated its cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    def send(self, thread: str, message: str, client_id: str) -> str:
        """Queue ``message`` and return its acknowledged server message id."""

        self._require_text(thread, "thread")
        self._require_text(message, "message")
        self._require_text(client_id, "client_id")

        result = self._call(
            "thread/queue/add",
            {
                "threadId": thread,
                "input": [{"type": "text", "text": message, "text_elements": []}],
                "clientUserMessageId": client_id,
            },
        )
        if not isinstance(result, dict):
            raise ProtocolError("queue/add result must be an object")
        queued = result.get("queuedSubmission")
        if not isinstance(queued, dict):
            raise ProtocolError("queue/add result is missing queuedSubmission")

        message_id = queued.get("id")
        if not isinstance(message_id, str) or not message_id:
            raise ProtocolError("queue acknowledgment is missing a message id")
        if queued.get("clientUserMessageId") != client_id:
            raise ProtocolError("queue acknowledgment identity mismatch")
        return message_id

    def _call(self, method: str, params: dict[str, Any]) -> Any:
        ws = self._require_ws()
        self._sequence += 1
        request_id = self._sequence
        request = {"id": request_id, "method": method, "params": params}

        self._set_timeout(ws)
        ws.send(json.dumps(request, ensure_ascii=False))
        while True:
            self._set_timeout(ws)
            raw = ws.recv()
            if not raw:
                raise ConnectionError("app-server closed the connection")
            try:
                reply = json.loads(raw)
            except (TypeError, ValueError) as exc:
                raise ProtocolError("app-server returned malformed JSON") from exc
            if not isinstance(reply, dict):
                raise ProtocolError("app-server reply must be an object")

            # Notifications and replies for earlier requests may share this
            # stream.  Ignore those until this request's id arrives.
            if reply.get("id") != request_id:
                continue
            self._remaining()
            if "error" in reply:
                raise RpcError(self._format_error(reply["error"]))
            if "result" not in reply:
                raise ProtocolError("app-server reply is missing result")
            return reply["result"]

    def _send_notification(self, method: str) -> None:
        ws = self._require_ws()
        self._set_timeout(ws)
        ws.send(json.dumps({"method": method}, ensure_ascii=False))

    def _set_timeout(self, ws: websocket.WebSocket) -> None:
        ws.settimeout(self._remaining())

    def _remaining(self) -> float:
        if self._deadline is None:
            raise RuntimeError("transport is not open")
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("app-server RPC deadline exceeded")
        return remaining

    def _require_ws(self) -> websocket.WebSocket:
        if self._ws is None:
            raise RuntimeError("transport is not open")
        return self._ws

    @staticmethod
    def _require_text(value: Any, name: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty string")

    @classmethod
    def _contains_marker(cls, value: Any, marker: str) -> bool:
        if isinstance(value, str):
            return marker in value
        if isinstance(value, dict):
            return any(cls._contains_marker(item, marker) for item in value.values())
        if isinstance(value, list):
            return any(cls._contains_marker(item, marker) for item in value)
        return False

    @staticmethod
    def _format_error(error: Any) -> str:
        try:
            return json.dumps(error, ensure_ascii=False)
        except (TypeError, ValueError):
            return repr(error)


__all__ = ["ProtocolError", "RpcError", "Transport", "TransportError"]
