"""herdr-core (``hc``) adapter tests.

The backend talks to the hc socket through three injected seams — one-shot
requests, raw-input injection, and the event stream — so every test here runs
on canned records shaped like what ``hc`` 0.1.19 returns, without a live core.

What gets the most attention: which terminals discovery may adopt (an ``hc
attach`` client mirrors another terminal's status and must never become a
duplicate topic), that polling reads never ask for history (a history read of a
full-screen app scrolls it), and the split between ``terminal.send``,
``terminal.keys`` and raw injection.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Mapping
from pathlib import Path

import pytest

from ccgram.multiplexer import get_multiplexer
from ccgram.multiplexer.base import AgentStatus, Multiplexer, MuxEvent
from ccgram.multiplexer.hc import HcManager, _key_segments
from ccgram.multiplexer.hc_rpc import HcRpcError


def _terminal(
    address: str,
    *,
    argv: list[str] | None = None,
    name: str | None = None,
    labels: dict[str, str] | None = None,
    state: str = "live",
    status: dict | None = None,
    parent: str | None = None,
    cwd: str = "/work/repo",
    fg_cwd: str | None = None,
    title: str = "",
) -> dict:
    """A ``Terminal`` record as ``terminal.get``/``terminal.list`` return it."""
    foreground = (
        None
        if state != "live" or argv is None
        else {
            "argv": argv,
            "cwd": fg_cwd or cwd,
            "name": Path(argv[0]).name,
            "pid": 4242,
        }
    )
    return {
        "address": address,
        "argv": ["/bin/zsh", "-l"],
        "cols": 151,
        "rows": 35,
        "created_ms": 1,
        "cwd": cwd,
        "foreground": foreground,
        "labels": labels or {},
        "name": name,
        "parent": parent,
        "screen": "exact",
        "state": state,
        "status": status,
        "title": title,
        "turn": 0,
    }


CLAUDE = _terminal(
    "t18",
    argv=["claude"],
    labels={"herdr.space": "sp1", "where": "wownow / bugs"},
    status={"app": "claude-code", "state": "done"},
    fg_cwd="/work/repo/.worktrees/x",
)
ATTACH_CLIENT = _terminal(
    "t23",
    argv=["hc", "attach", "t18"],
    labels={"where": "wownow / bugs"},
    status={"app": "claude-code", "state": "done"},
)
SHELL = _terminal("t2", argv=["/bin/zsh", "-l"])
CHILD = _terminal("t3", argv=["codex"], parent="t18")
HIDDEN = _terminal("t4", argv=["claude"], name="_scratch")
OWN = _terminal("t9", argv=["claude"])
REMOTE = _terminal("vps/t3", argv=["claude"])
FINISHED = _terminal("t5", argv=None, state="finished")


class FakeHc:
    """Canned hc socket: requests by method, plus recorded injections."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.injected: list[tuple[str, bytes, int, int]] = []
        self.responses: dict[str, object] = {}
        self.records: dict[str, dict] = {}

    def on(self, method: str, result: object) -> FakeHc:
        """Answer *method* with a dict, or raise it when it is an exception."""
        self.responses[method] = result
        return self

    def with_terminals(self, *records: dict) -> FakeHc:
        for record in records:
            self.records[record["address"]] = record
        return self.on("terminal.list", {"terminals": list(records)})

    async def request(
        self, method: str, params: Mapping[str, object], timeout: float
    ) -> dict:
        del timeout
        self.calls.append((method, dict(params)))
        if method == "terminal.get" and method not in self.responses:
            record = self.records.get(str(params["terminal"]))
            if record is None:
                raise HcRpcError("no terminal", "not_found")
            return record
        result = self.responses.get(method, {})
        if isinstance(result, Exception):
            raise result
        assert isinstance(result, dict)
        return result

    async def inject(self, terminal: str, data: bytes, cols: int, rows: int) -> None:
        self.injected.append((terminal, data, cols, rows))

    def methods(self) -> list[str]:
        return [method for method, _params in self.calls]

    def params(self, method: str) -> dict:
        return next(params for name, params in self.calls if name == method)


@pytest.fixture
def fake() -> FakeHc:
    return FakeHc().with_terminals(
        CLAUDE, ATTACH_CLIENT, SHELL, CHILD, HIDDEN, OWN, REMOTE, FINISHED
    )


@pytest.fixture
def hc(fake: FakeHc) -> HcManager:
    return HcManager(
        socket_path="/nonexistent/hc.sock",
        requester=fake.request,
        injector=fake.inject,
        own_terminal="t9",
        label_scope={},
    )


class TestRegistration:
    def test_registry_builds_the_hc_backend(self) -> None:
        backend = get_multiplexer("hc")
        assert isinstance(backend, HcManager)
        assert isinstance(backend, Multiplexer)

    def test_capabilities(self, hc: HcManager) -> None:
        caps = hc.capabilities
        assert caps.name == "hc"
        assert caps.ids_stable_across_restart is True
        assert caps.native_agent_status is True
        assert caps.supports_event_stream is True
        assert caps.self_identify_env == "HC_TERMINAL_ID"
        assert caps.exposes_pane_tty is False
        assert caps.native_worktrees is False
        assert caps.recovers_stale_ids_by_name is False

    def test_constructor_does_no_io(self) -> None:
        async def refuse(*_args: object) -> dict:
            raise AssertionError("constructor must not call hc")

        HcManager(requester=refuse)


class TestListing:
    async def test_reconciliation_lists_every_live_local_terminal(
        self, hc: HcManager
    ) -> None:
        windows = await hc.list_windows_for_reconciliation()
        assert windows is not None
        # Remote and finished terminals are not this backend's windows.
        assert [w.window_id for w in windows] == ["t18", "t23", "t2", "t3", "t4", "t9"]

    async def test_only_foreground_agents_are_adoptable(self, hc: HcManager) -> None:
        windows = await hc.list_windows_for_reconciliation()
        assert windows is not None
        eligible = {w.window_id for w in windows if w.topic_eligible}
        # Not the attach client mirroring t18's status, not a bare shell, not
        # another agent's child, not a hidden name, not ccgram's own terminal.
        assert eligible == {"t18"}

    async def test_pickers_hide_own_and_underscore_terminals(
        self, hc: HcManager
    ) -> None:
        ids = [w.window_id for w in await hc.list_windows()]
        assert "t9" not in ids
        assert "t4" not in ids
        assert "t18" in ids

    async def test_window_ref_uses_the_agents_current_folder(
        self, hc: HcManager
    ) -> None:
        window = await hc.find_window_by_id("t18")
        assert window is not None
        assert window.cwd == "/work/repo/.worktrees/x"
        assert window.window_name == "wownow / bugs"
        assert window.pane_current_command == "claude"
        assert (window.pane_width, window.pane_height) == (151, 35)

    async def test_ccgram_name_label_wins(self, fake: FakeHc, hc: HcManager) -> None:
        fake.with_terminals(
            _terminal(
                "t7",
                argv=["claude"],
                name="named",
                labels={"ccgram.name": "renamed", "where": "a / b"},
            )
        )
        window = await hc.find_window_by_id("t7")
        assert window is not None and window.window_name == "renamed"

    async def test_name_falls_back_to_folder(self, fake: FakeHc, hc: HcManager) -> None:
        fake.with_terminals(_terminal("t7", argv=["claude"], cwd="/src/project"))
        window = await hc.find_window_by_id("t7")
        assert window is not None and window.window_name == "project"

    async def test_label_scope_limits_pickers_and_adoption(self, fake: FakeHc) -> None:
        scoped = HcManager(
            requester=fake.request, own_terminal="", label_scope={"herdr.space": "sp1"}
        )
        assert [w.window_id for w in await scoped.list_windows()] == ["t18"]
        reconciled = await scoped.list_windows_for_reconciliation()
        assert reconciled is not None
        assert {w.window_id for w in reconciled if w.topic_eligible} == {"t18"}
        # Out-of-scope terminals still exist for cleanup.
        assert len(reconciled) == 6

    async def test_unconfirmed_listing_is_none_not_empty(self, fake: FakeHc) -> None:
        fake.on("terminal.list", HcRpcError("hc socket failed: refused"))
        hc = HcManager(requester=fake.request, own_terminal="", label_scope={})
        assert await hc.list_windows_for_reconciliation() is None
        assert await hc.list_windows() == []

    async def test_finished_terminal_is_not_found(self, hc: HcManager) -> None:
        assert await hc.find_window_by_id("t5") is None

    async def test_foreign_ids_never_reach_hc(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        assert await hc.find_window_by_id("@3") is None
        assert await hc.find_window_by_id("vps/t3") is None
        assert fake.calls == []


class TestWindowExists:
    async def test_live(self, hc: HcManager) -> None:
        assert await hc.window_exists("t18") is True

    async def test_finished_and_not_found_are_absent(self, hc: HcManager) -> None:
        assert await hc.window_exists("t5") is False
        assert await hc.window_exists("t404") is False

    async def test_transport_failure_is_unknown(self, fake: FakeHc) -> None:
        fake.on("terminal.get", HcRpcError("hc socket failed: refused"))
        hc = HcManager(requester=fake.request, own_terminal="", label_scope={})
        assert await hc.window_exists("t18") is None

    async def test_foreign_namespace_is_unknown(self, hc: HcManager) -> None:
        assert await hc.window_exists("@1") is None


class TestCapture:
    async def test_screen_capture_never_asks_for_history(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        fake.on(
            "terminal.read", {"text": "  hello\n", "complete": True, "how": "screen"}
        )
        assert await hc.capture_pane("t18", with_ansi=True) == "hello"
        assert fake.params("terminal.read") == {"terminal": "t18", "lines": 0}

    async def test_scrollback_reports_incomplete_reads(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        fake.on(
            "terminal.read",
            {"text": "screen", "complete": False, "how": "screen", "why": "working"},
        )
        result = await hc.capture_scrollback("t18", 300)
        assert result is not None
        assert result.truncated is True
        assert fake.params("terminal.read")["lines"] == 300

    async def test_failed_read_is_none(self, fake: FakeHc, hc: HcManager) -> None:
        fake.on("terminal.read", HcRpcError("gone", "not_found"))
        assert await hc.capture_pane("t18") is None
        assert await hc.capture_scrollback("t18") is None

    async def test_foreign_pane_read_is_rejected(self, hc: HcManager) -> None:
        assert await hc.capture_pane_by_id("t2", window_id="t18") is None

    async def test_pane_dims(self, hc: HcManager) -> None:
        dims = await hc.pane_dims("t18")
        assert dims is not None and (dims.width, dims.height) == (151, 35)


class TestSend:
    async def test_text_with_enter_is_one_forced_send(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        assert await hc.send("t18", "fix the bug") is True
        assert fake.methods() == ["terminal.send"]
        # Forced: the person answering in Telegram is who hc's blocked guard
        # waits for.
        assert fake.params("terminal.send") == {
            "terminal": "t18",
            "text": "fix the bug",
            "force": True,
        }
        assert fake.injected == []

    async def test_text_without_enter_is_injected_at_the_current_size(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        assert await hc.send("t18", "1", enter=False) is True
        assert fake.injected == [("t18", b"1", 151, 35)]
        assert "terminal.send" not in fake.methods()

    async def test_multiline_without_enter_is_a_bracketed_paste(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        await hc.send("t18", "a\nb", enter=False)
        assert fake.injected[0][1] == b"\x1b[200~a\nb\x1b[201~"

    async def test_raw_multiline_is_not_wrapped(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        await hc.send("t18", "a\nb", enter=False, raw=True)
        assert fake.injected[0][1] == b"a\nb"

    async def test_injection_into_a_finished_terminal_fails(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        assert await hc.send("t5", "x", enter=False) is False
        assert fake.injected == []

    async def test_key_names_go_through_terminal_keys(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        assert await hc.send(
            "t18", "Escape C-c BTab M-Enter", literal=False, enter=False
        )
        assert fake.params("terminal.keys") == {
            "terminal": "t18",
            "keys": ["esc", "ctrl+c", "shift+tab", "alt+enter"],
        }

    async def test_enter_is_appended_to_key_names(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        await hc.send("t18", "Down", literal=False)
        assert fake.params("terminal.keys")["keys"] == ["down", "enter"]

    async def test_unknown_key_name_types_nothing(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        assert await hc.send("t18", "NotAKey", literal=False, enter=False) is False
        assert fake.calls == [] and fake.injected == []

    async def test_foreign_pane_write_is_rejected(self, hc: HcManager) -> None:
        assert await hc.send_to_pane("t2", "x", window_id="t18") is False

    async def test_send_to_remote_address_is_refused(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        assert await hc.send("vps/t3", "x") is False
        assert fake.calls == []


class TestKeySegments:
    def test_unnameable_characters_are_raw(self) -> None:
        # hc's ``A`` key types ``a``, and it has no name for ``!``.
        assert _key_segments(["Up", "A", "!", "Enter"]) == [
            ["up"],
            b"A!",
            ["enter"],
        ]

    def test_lowercase_and_digits_are_key_names(self) -> None:
        assert _key_segments(["y", "1", "Tab"]) == [["y", "1", "tab"]]

    def test_control_and_meta_chords(self) -> None:
        assert _key_segments(["C-u", "M-t", "M-b"]) == [["ctrl+u", "alt+t", "alt+b"]]

    def test_unknown_name(self) -> None:
        assert _key_segments(["Hyper-x"]) is None


class TestLifecycle:
    async def test_kill_removes_the_finished_record(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        assert await hc.kill_window("t18") is True
        assert fake.methods() == ["terminal.kill", "terminal.rm"]

    async def test_kill_of_a_finished_terminal_still_removes_it(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        fake.on("terminal.kill", HcRpcError("already finished", "finished"))
        assert await hc.kill_window("t5") is True
        assert fake.methods() == ["terminal.kill", "terminal.rm"]

    async def test_failed_kill_reports_failure(
        self, fake: FakeHc, hc: HcManager
    ) -> None:
        fake.on("terminal.kill", HcRpcError("hc socket failed: refused"))
        assert await hc.kill_window("t18") is False
        assert "terminal.rm" not in fake.methods()

    async def test_rename_records_a_label(self, fake: FakeHc, hc: HcManager) -> None:
        assert await hc.rename_window("t18", "auth fix") is True
        assert fake.params("terminal.label") == {
            "terminal": "t18",
            "labels": {"ccgram.name": "auth fix"},
        }

    async def test_create_starts_a_detached_shell_then_types_the_agent(
        self, fake: FakeHc, hc: HcManager, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.setenv("CCGRAM_DIR", "/cfg/ccgram-test")
        fake.on("terminal.run", _terminal("t30", argv=["/bin/zsh", "-l"]))
        ok, message, name, window_id = await hc.create_window(
            str(tmp_path),
            window_name="api",
            launch_command="claude",
            agent_args="--continue",
        )
        assert (ok, message, name, window_id) == (True, "", "api", "t30")
        run = fake.params("terminal.run")
        assert "argv" not in run and "line" not in run  # the login shell
        assert run["cwd"] == str(tmp_path.resolve())
        assert run["no_parent"] is True and run["outlive_parent"] is True
        assert run["keep"] is True and run["no_notify"] is True
        assert run["labels"] == {"ccgram.name": "api"}
        assert run["env"] == {"CCGRAM_DIR": "/cfg/ccgram-test"}
        assert fake.params("terminal.send")["text"] == "claude --continue"

    async def test_failed_launch_removes_the_terminal(
        self, fake: FakeHc, hc: HcManager, tmp_path: Path
    ) -> None:
        fake.on("terminal.run", _terminal("t30", argv=["/bin/zsh", "-l"]))
        fake.on("terminal.send", HcRpcError("hc socket failed"))
        ok, message, _name, _id = await hc.create_window(
            str(tmp_path), launch_command="claude"
        )
        assert ok is False and "claude" in message
        assert fake.methods()[-2:] == ["terminal.kill", "terminal.rm"]

    async def test_create_refuses_a_missing_directory(
        self, fake: FakeHc, hc: HcManager, tmp_path: Path
    ) -> None:
        ok, message, _name, _id = await hc.create_window(str(tmp_path / "nope"))
        assert ok is False and "does not exist" in message
        assert fake.calls == []

    async def test_topic_target_is_the_address(
        self, fake: FakeHc, hc: HcManager, tmp_path: Path
    ) -> None:
        fake.on("terminal.run", _terminal("t31", argv=["/bin/zsh", "-l"]))
        target = await hc.create_topic_target(
            str(tmp_path), launch_command=None, workspace_id=None
        )
        assert target.target_id == target.window_id == "t31"

    async def test_worktrees_and_splits_are_unsupported(self, hc: HcManager) -> None:
        ok, *_ = await hc.create_worktree_window("/r", "/r/w", "b")
        assert ok is False
        assert await hc.split_window("t18") is None
        assert await hc.list_panes("t18") == []
        assert await hc.list_workspaces() == []


class TestProcessAndStatus:
    async def test_foreground_reports_the_group_leader(self, hc: HcManager) -> None:
        fg = await hc.foreground("t18")
        assert fg is not None
        assert fg.argv == ["claude"]
        assert fg.pid == fg.pgid == 4242
        assert fg.cwd == "/work/repo/.worktrees/x"
        assert fg.tty == ""

    async def test_agent_status_normalises_the_app(self, fake: FakeHc) -> None:
        fake.with_terminals(
            _terminal(
                "t7",
                argv=["claude"],
                status={"app": "claude-code", "state": "working", "msg": "Testing"},
            )
        )
        hc = HcManager(requester=fake.request, own_terminal="", label_scope={})
        assert await hc.agent_status("t7") == AgentStatus(
            state="working", agent="claude", custom_status="Testing"
        )

    async def test_no_report_is_no_status(self, hc: HcManager) -> None:
        assert await hc.agent_status("t2") is None

    async def test_title(self, fake: FakeHc) -> None:
        fake.with_terminals(_terminal("t7", argv=["claude"], title="Claude Code"))
        hc = HcManager(requester=fake.request, own_terminal="", label_scope={})
        assert await hc.get_pane_title("t7") == "Claude Code"


def _event(seq: int, terminal: str, kind: str, data: dict | None = None) -> dict:
    return {
        "method": "event",
        "params": {"seq": seq, "terminal": terminal, "type": kind, "data": data or {}},
    }


def _snapshot(seq: int, *records: dict) -> dict:
    return {"method": "snapshot", "params": {"seq": seq, "terminals": list(records)}}


class _Streams:
    """Stream opener yielding scripted subscriptions, then blocking."""

    def __init__(self, *scripts: list[dict | Exception]) -> None:
        self.scripts = list(scripts)
        self.opened = 0

    def __call__(self) -> AsyncGenerator[dict, None]:
        script = self.scripts.pop(0) if self.scripts else []
        self.opened += 1
        return self._run(script)

    @staticmethod
    async def _run(script: list[dict | Exception]) -> AsyncGenerator[dict, None]:
        for item in script:
            if isinstance(item, Exception):
                raise item
            yield item
        await asyncio.Event().wait()


async def _collect(hc: HcManager, ids: list[str], count: int) -> list[MuxEvent]:
    events: list[MuxEvent] = []
    stream = hc.watch_events(ids)
    try:
        async with asyncio.timeout(5):
            async for event in stream:
                events.append(event)
                if len(events) == count:
                    break
    finally:
        await stream.aclose()
    return events


class TestWatchEvents:
    async def test_snapshot_primes_status_and_reports_finished(self) -> None:
        streams = _Streams(
            [
                _snapshot(
                    10,
                    CLAUDE,
                    _terminal("t5", state="finished"),
                    _terminal("t6", argv=["claude"], status={"state": "working"}),
                )
            ]
        )
        hc = HcManager(stream_opener=streams, own_terminal="", label_scope={})
        events = await _collect(hc, ["t18", "t5"], 2)
        assert events == [
            MuxEvent(
                kind="agent_status",
                window_id="t18",
                status=AgentStatus(state="done", agent="claude"),
            ),
            MuxEvent(kind="window_died", window_id="t5"),
        ]

    async def test_events_translate_and_filter(self) -> None:
        streams = _Streams(
            [
                _snapshot(10),
                _event(9, "t18", "finished"),  # already in the snapshot
                _event(11, "t18", "size", {"cols": 80, "rows": 24}),
                _event(12, "t99", "finished"),  # not watched
                _event(
                    13,
                    "t18",
                    "status",
                    {
                        "status": {
                            "app": "codex",
                            "state": "blocked",
                            "kind": "permission",
                        }
                    },
                ),
                _event(14, "t18", "status", {"status": None, "reported": True}),
                _event(15, "t18", "removed"),
            ]
        )
        hc = HcManager(stream_opener=streams, own_terminal="", label_scope={})
        events = await _collect(hc, ["t18"], 3)
        assert events == [
            MuxEvent(
                kind="agent_status",
                window_id="t18",
                status=AgentStatus(state="blocked", agent="codex"),
            ),
            MuxEvent(
                kind="agent_status",
                window_id="t18",
                status=AgentStatus(state="unknown"),
            ),
            MuxEvent(kind="window_died", window_id="t18"),
        ]

    async def test_missed_and_dropped_streams_resubscribe(self, monkeypatch) -> None:
        monkeypatch.setattr("ccgram.multiplexer.hc._STREAM_BACKOFF_BASE", 0.0)
        streams = _Streams(
            [_snapshot(1), {"method": "missed", "params": {}}],
            [_snapshot(5), HcRpcError("hc event stream went quiet")],
            [_snapshot(9), _event(10, "t18", "finished")],
        )
        hc = HcManager(stream_opener=streams, own_terminal="", label_scope={})
        events = await _collect(hc, ["t18"], 1)
        assert events == [MuxEvent(kind="window_died", window_id="t18")]
        assert streams.opened == 3
