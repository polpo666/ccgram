"""Transport for the herdr-core (``hc``) socket API, plus its pure helpers.

herdr-core speaks JSON-RPC 2.0, one message per line, on a per-session unix
socket. Errors carry a stable ``error.data.kind`` (``not_found``, ``blocked``,
``finished`` …) that the backend needs as proof of absence, so this module keeps
it instead of flattening errors to a message.

Raw input (text without Enter, escape sequences) has no line-mode method:
``terminal.keys`` takes key names only and ``terminal.send`` always submits.
``inject`` therefore switches one short-lived connection to framed mode and
attaches as a viewer at the terminal's *current* size (so no resize reaches the
user's other viewers), writes one AUTOMATIC_INPUT frame, and closes the stream.
The frame layout — ``<channel:u32 LE><kind:u8><length:u32 LE><payload>`` — is
what ``hc attach`` itself sends; it is not part of the published schema.

Stdlib only (plus ``hc_targets``): the hook process imports this without
bot configuration.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import struct
import uuid
from collections.abc import AsyncGenerator, Mapping
from pathlib import Path

from ..hc_targets import is_hc_address

__all__ = [
    "HcRpcError",
    "inject",
    "is_hc_address",
    "request",
    "request_sync",
    "resolve_socket_path",
    "subscribe",
    "terminal_display_name",
]

MAX_LINE_BYTES = 16 * 1024 * 1024
_CLOSE_TIMEOUT_SECONDS = 0.5

# Framed-mode frame header and the kinds this client uses.
_FRAME_HEADER = struct.Struct("<IBI")
_FRAME_DATA = 0
_FRAME_CLOSE = 2
_FRAME_AUTOMATIC_INPUT = 6

# The label ccgram writes when a topic renames its terminal, and the label the
# Herdr GUI writes for the space/tab a terminal sits in.
NAME_LABEL = "ccgram.name"
WHERE_LABEL = "where"


class HcRpcError(RuntimeError):
    """An hc request failed: transport, protocol, or a remote error."""

    def __init__(self, message: str, kind: str = "") -> None:
        super().__init__(message)
        self.message = message
        self.kind = kind
        """``error.data.kind`` for a remote error; ``""`` for transport failures."""


def resolve_socket_path(env: Mapping[str, str] | None = None) -> str:
    """Return the hc socket path, resolved the way ``hc`` resolves it.

    ``HC_SOCKET`` names it outright; inside an hc terminal
    ``HC_TERMINAL_SOCKET`` is the core that terminal belongs to; otherwise it
    is ``$XDG_STATE_HOME/herdr-core/<session>/<session>.sock`` with
    ``HC_SESSION`` defaulting to ``default``.
    """
    env = os.environ if env is None else env
    explicit = env.get("HC_SOCKET") or env.get("HC_TERMINAL_SOCKET")
    if explicit:
        return explicit
    session = env.get("HC_SESSION") or "default"
    state = env.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return str(Path(state) / "herdr-core" / session / f"{session}.sock")


def terminal_display_name(record: Mapping[str, object]) -> str:
    """The label a terminal is shown under.

    A name ccgram gave it wins, then the hc name, then the Herdr GUI's
    ``space / tab`` placement, then the program's title, then the folder.
    """
    labels = record.get("labels")
    labels = labels if isinstance(labels, Mapping) else {}
    foreground = record.get("foreground")
    cwd = (
        foreground.get("cwd") if isinstance(foreground, Mapping) else None
    ) or record.get("cwd")
    for candidate in (
        labels.get(NAME_LABEL),
        record.get("name"),
        labels.get(WHERE_LABEL),
        record.get("title"),
        Path(cwd).name if isinstance(cwd, str) and cwd else None,
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return str(record.get("address") or "")


def _encode(method: str, params: Mapping[str, object]) -> tuple[str, bytes]:
    request_id = uuid.uuid4().hex
    line = json.dumps(
        {"jsonrpc": "2.0", "id": request_id, "method": method, "params": dict(params)},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return request_id, line


def _parse(line: bytes, request_id: str) -> dict:
    """Return the ``result`` object of a response, raising ``HcRpcError``."""
    if not line:
        raise HcRpcError("hc closed the connection before answering")
    try:
        envelope = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HcRpcError("hc answered with something that is not JSON") from exc
    if not isinstance(envelope, dict) or envelope.get("id") != request_id:
        raise HcRpcError("hc answered a different request")
    error = envelope.get("error")
    if error is not None:
        if not isinstance(error, dict):
            raise HcRpcError("hc returned a malformed error")
        data = error.get("data")
        kind = data.get("kind") if isinstance(data, dict) else ""
        raise HcRpcError(str(error.get("message") or "hc error"), str(kind or ""))
    result = envelope.get("result")
    if not isinstance(result, dict):
        raise HcRpcError("hc response has no result object")
    return result


async def _close(writer: asyncio.StreamWriter) -> None:
    with contextlib.suppress(OSError, RuntimeError):
        writer.close()
    try:
        async with asyncio.timeout(_CLOSE_TIMEOUT_SECONDS):
            await writer.wait_closed()
    except TimeoutError, OSError, RuntimeError, asyncio.CancelledError:
        transport = getattr(writer, "transport", None)
        if transport is not None:
            with contextlib.suppress(Exception):
                transport.abort()


async def request(
    socket_path: str,
    method: str,
    params: Mapping[str, object],
    *,
    timeout: float = 8.0,
) -> dict:
    """Send one request on a fresh connection and return its ``result``."""
    request_id, line = _encode(method, params)
    writer: asyncio.StreamWriter | None = None
    try:
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_unix_connection(
                socket_path, limit=MAX_LINE_BYTES
            )
            writer.write(line + b"\n")
            await writer.drain()
            response = await reader.readline()
    except TimeoutError as exc:
        raise HcRpcError(f"hc {method} timed out") from exc
    except (OSError, ValueError, asyncio.IncompleteReadError) as exc:
        raise HcRpcError(f"hc socket failed: {exc}") from exc
    finally:
        if writer is not None:
            await _close(writer)
    return _parse(response, request_id)


def request_sync(
    socket_path: str,
    method: str,
    params: Mapping[str, object],
    *,
    timeout: float = 2.0,
) -> dict:
    """``request`` for the synchronous hook process."""
    request_id, line = _encode(method, params)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(timeout)
            connection.connect(socket_path)
            connection.sendall(line + b"\n")
            with connection.makefile("rb") as stream:
                response = stream.readline(MAX_LINE_BYTES)
    except OSError as exc:
        raise HcRpcError(f"hc socket failed: {exc}") from exc
    return _parse(response, request_id)


def _frame(channel: int, kind: int, payload: bytes) -> bytes:
    return _FRAME_HEADER.pack(channel, kind, len(payload)) + payload


async def _read_frame(reader: asyncio.StreamReader) -> tuple[int, int, bytes]:
    channel, kind, length = _FRAME_HEADER.unpack(
        await reader.readexactly(_FRAME_HEADER.size)
    )
    if length > MAX_LINE_BYTES:
        raise HcRpcError("hc sent an oversized frame")
    return channel, kind, await reader.readexactly(length)


async def inject(
    socket_path: str,
    terminal: str,
    data: bytes,
    *,
    cols: int,
    rows: int,
    timeout: float = 8.0,
) -> None:
    """Write raw bytes into a live terminal as automatic input.

    *cols*/*rows* must be the terminal's current size: an attached viewer
    sizes the terminal, and anything else would resize every other viewer.
    """
    hello_id, hello = _encode("hello", {"generation": 1, "framed": True})
    writer: asyncio.StreamWriter | None = None
    try:
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_unix_connection(
                socket_path, limit=MAX_LINE_BYTES
            )
            writer.write(hello + b"\n")
            await writer.drain()
            # The hello reply is the last line-mode message; framing follows.
            _parse(await reader.readline(), hello_id)
            attach_id, attach = _encode(
                "terminal.attach",
                {"terminal": terminal, "cols": cols, "rows": rows},
            )
            writer.write(_frame(0, _FRAME_DATA, attach))
            await writer.drain()
            while True:
                channel, _kind, payload = await _read_frame(reader)
                if channel == 0:
                    break
            stream = _parse(payload, attach_id).get("channel")
            if not isinstance(stream, int):
                raise HcRpcError("hc attach returned no stream channel")
            writer.write(_frame(stream, _FRAME_AUTOMATIC_INPUT, data))
            writer.write(_frame(stream, _FRAME_CLOSE, b""))
            await writer.drain()
    except TimeoutError as exc:
        raise HcRpcError("hc input injection timed out") from exc
    except (OSError, ValueError, struct.error, asyncio.IncompleteReadError) as exc:
        raise HcRpcError(f"hc socket failed: {exc}") from exc
    finally:
        if writer is not None:
            await _close(writer)


async def subscribe(
    socket_path: str, *, idle_timeout: float = 45.0
) -> AsyncGenerator[dict, None]:
    """Yield the subscription snapshot, then every notification, in order.

    The first item is ``{"method": "snapshot", "params": <subscribe result>}``;
    the rest are the server's notifications (``event``, ``heartbeat``,
    ``missed``) as sent. Ends with ``HcRpcError`` when the connection drops or
    goes quiet for *idle_timeout* (the core sends a heartbeat every 15 s).
    """
    request_id, line = _encode("events.subscribe", {})
    writer: asyncio.StreamWriter | None = None
    try:
        try:
            async with asyncio.timeout(idle_timeout):
                reader, writer = await asyncio.open_unix_connection(
                    socket_path, limit=MAX_LINE_BYTES
                )
                writer.write(line + b"\n")
                await writer.drain()
                snapshot = _parse(await reader.readline(), request_id)
        except TimeoutError as exc:
            raise HcRpcError("hc events.subscribe timed out") from exc
        except (OSError, ValueError, asyncio.IncompleteReadError) as exc:
            raise HcRpcError(f"hc socket failed: {exc}") from exc
        yield {"method": "snapshot", "params": snapshot}
        while True:
            try:
                async with asyncio.timeout(idle_timeout):
                    raw = await reader.readline()
            except TimeoutError as exc:
                raise HcRpcError("hc event stream went quiet") from exc
            except (OSError, ValueError) as exc:
                raise HcRpcError(f"hc socket failed: {exc}") from exc
            if not raw:
                raise HcRpcError("hc closed the event stream")
            try:
                message = json.loads(raw.decode("utf-8"))
            except UnicodeDecodeError, json.JSONDecodeError:
                continue
            if isinstance(message, dict) and isinstance(message.get("method"), str):
                yield message
    finally:
        if writer is not None:
            await _close(writer)
