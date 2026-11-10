"""herdr-core socket transport tests, against an in-process unix server.

The server speaks the wire shapes ``hc`` 0.1.19 was observed to use: JSON-RPC
lines with ``error.data.kind``, a framed upgrade after ``hello`` whose frames
are ``<channel:u32 LE><kind:u8><length:u32 LE><payload>``, and a subscription
answered with a snapshot followed by ``event``/``heartbeat`` notifications.
"""

from __future__ import annotations

import asyncio
import json
import struct
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from ccgram.multiplexer import hc_rpc
from ccgram.multiplexer.hc_rpc import HcRpcError

_HEADER = struct.Struct("<IBI")

Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@pytest.fixture
async def serve() -> AsyncIterator[Callable[[Handler], Awaitable[str]]]:
    """Start a unix server with a handler; return its socket path.

    The directory is under ``/tmp`` because macOS caps socket paths at 104
    bytes and pytest's ``tmp_path`` there is longer.
    """
    servers: list[asyncio.Server] = []
    socket_dir = TemporaryDirectory(prefix="ccgram-hc-", dir="/tmp")

    async def start(handler: Handler) -> str:
        path = str(Path(socket_dir.name) / "hc.sock")
        servers.append(await asyncio.start_unix_server(handler, path=path))
        return path

    yield start
    for server in servers:
        server.close()
        await server.wait_closed()
    socket_dir.cleanup()


async def _read_request(reader: asyncio.StreamReader) -> dict:
    return json.loads(await reader.readline())


def _reply(writer: asyncio.StreamWriter, request: dict, **body: object) -> None:
    writer.write(
        json.dumps({"jsonrpc": "2.0", "id": request["id"], **body}).encode() + b"\n"
    )


async def test_request_returns_the_result(serve) -> None:
    seen: list[dict] = []

    async def handler(reader, writer) -> None:
        request = await _read_request(reader)
        seen.append(request)
        _reply(writer, request, result={"address": "t7", "state": "live"})
        await writer.drain()

    path = await serve(handler)
    result = await hc_rpc.request(path, "terminal.get", {"terminal": "t7"})
    assert result == {"address": "t7", "state": "live"}
    assert seen[0]["method"] == "terminal.get"
    assert seen[0]["params"] == {"terminal": "t7"}
    assert seen[0]["jsonrpc"] == "2.0"


async def test_remote_error_keeps_its_kind(serve) -> None:
    async def handler(reader, writer) -> None:
        request = await _read_request(reader)
        _reply(
            writer,
            request,
            error={
                "code": -32000,
                "message": 'no terminal "t404"',
                "data": {"kind": "not_found", "hint": "see `hc ls --all`"},
            },
        )
        await writer.drain()

    path = await serve(handler)
    with pytest.raises(HcRpcError) as caught:
        await hc_rpc.request(path, "terminal.get", {"terminal": "t404"})
    assert caught.value.kind == "not_found"


async def test_unreachable_socket_is_a_transport_error() -> None:
    with pytest.raises(HcRpcError) as caught:
        await hc_rpc.request("/tmp/ccgram-no-such-hc.sock", "server.status", {})
    assert caught.value.kind == ""


async def test_mismatched_id_is_rejected(serve) -> None:
    async def handler(reader, writer) -> None:
        await _read_request(reader)
        writer.write(b'{"jsonrpc":"2.0","id":"other","result":{}}\n')
        await writer.drain()

    path = await serve(handler)
    with pytest.raises(HcRpcError):
        await hc_rpc.request(path, "server.status", {})


async def test_request_sync(serve) -> None:
    async def handler(reader, writer) -> None:
        request = await _read_request(reader)
        _reply(writer, request, result={"state": "live"})
        await writer.drain()

    path = await serve(handler)
    result = await asyncio.to_thread(
        hc_rpc.request_sync, path, "terminal.get", {"terminal": "t7"}
    )
    assert result == {"state": "live"}


async def test_inject_attaches_framed_and_writes_automatic_input(serve) -> None:
    frames: list[tuple[int, int, bytes]] = []
    attach_params: list[dict] = []
    done = asyncio.Event()

    async def handler(reader, writer) -> None:
        hello = await _read_request(reader)
        assert hello["params"] == {"generation": 1, "framed": True}
        _reply(writer, hello, result={"framed": True})
        await writer.drain()
        channel, kind, length = _HEADER.unpack(await reader.readexactly(9))
        attach = json.loads(await reader.readexactly(length))
        attach_params.append(attach["params"])
        response = json.dumps(
            {"jsonrpc": "2.0", "id": attach["id"], "result": {"channel": 1}}
        ).encode()
        # The first paint may arrive before the reply on the stream channel.
        writer.write(_HEADER.pack(1, 0, 5) + b"paint")
        writer.write(_HEADER.pack(0, 0, len(response)) + response)
        await writer.drain()
        while len(frames) < 2:
            channel, kind, length = _HEADER.unpack(await reader.readexactly(9))
            frames.append((channel, kind, await reader.readexactly(length)))
        done.set()

    path = await serve(handler)
    await hc_rpc.inject(path, "t7", "héllo".encode(), cols=151, rows=35)
    await asyncio.wait_for(done.wait(), 2)
    assert attach_params == [{"terminal": "t7", "cols": 151, "rows": 35}]
    assert frames == [(1, 6, "héllo".encode()), (1, 2, b"")]


async def test_subscribe_yields_snapshot_then_notifications(serve) -> None:
    async def handler(reader, writer) -> None:
        request = await _read_request(reader)
        assert request["method"] == "events.subscribe"
        _reply(writer, request, result={"seq": 4, "terminals": [], "channel": None})
        writer.write(b'{"jsonrpc":"2.0","method":"heartbeat","params":{"seq":4}}\n')
        writer.write(b"not json\n")
        writer.write(
            b'{"jsonrpc":"2.0","method":"event","params":'
            b'{"seq":5,"terminal":"t7","type":"finished","data":{}}}\n'
        )
        await writer.drain()
        writer.close()

    path = await serve(handler)
    received: list[dict] = []
    with pytest.raises(HcRpcError, match="closed"):
        async for message in hc_rpc.subscribe(path):
            received.append(message)
    assert [m["method"] for m in received] == ["snapshot", "heartbeat", "event"]
    assert received[0]["params"]["seq"] == 4


async def test_subscribe_gives_up_on_a_silent_stream(serve) -> None:
    async def handler(reader, writer) -> None:
        request = await _read_request(reader)
        _reply(writer, request, result={"seq": 0, "terminals": []})
        await writer.drain()
        await asyncio.sleep(0.5)

    path = await serve(handler)
    stream = hc_rpc.subscribe(path, idle_timeout=0.2)
    assert (await anext(stream))["method"] == "snapshot"
    with pytest.raises(HcRpcError, match="quiet"):
        await anext(stream)


class TestSocketPath:
    def test_explicit_socket_wins(self) -> None:
        env = {"HC_SOCKET": "/a.sock", "HC_TERMINAL_SOCKET": "/b.sock"}
        assert hc_rpc.resolve_socket_path(env) == "/a.sock"

    def test_terminal_socket_inside_hc(self) -> None:
        assert (
            hc_rpc.resolve_socket_path({"HC_TERMINAL_SOCKET": "/b.sock"}) == "/b.sock"
        )

    def test_session_under_state_home(self) -> None:
        env = {"HC_SESSION": "work", "XDG_STATE_HOME": "/state"}
        assert hc_rpc.resolve_socket_path(env) == "/state/herdr-core/work/work.sock"

    def test_default(self) -> None:
        path = hc_rpc.resolve_socket_path({})
        assert path.endswith("/.local/state/herdr-core/default/default.sock")


class TestDisplayName:
    def test_priority(self) -> None:
        record = {
            "address": "t7",
            "labels": {"where": "space / tab"},
            "name": "review",
            "title": "Claude Code",
            "cwd": "/src/app",
        }
        assert hc_rpc.terminal_display_name(record) == "review"
        record["name"] = None
        assert hc_rpc.terminal_display_name(record) == "space / tab"
        record["labels"] = {}
        assert hc_rpc.terminal_display_name(record) == "Claude Code"
        record["title"] = ""
        assert hc_rpc.terminal_display_name(record) == "app"

    def test_address_as_last_resort(self) -> None:
        assert hc_rpc.terminal_display_name({"address": "t7", "cwd": ""}) == "t7"


@pytest.mark.parametrize(
    ("value", "expected"),
    [("t7", True), ("t1r", True), ("vps/t3", False), ("@3", False), ("t", False)],
)
def test_is_hc_address(value: str, expected: bool) -> None:
    assert hc_rpc.is_hc_address(value) is expected
