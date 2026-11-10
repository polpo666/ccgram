"""Claude hooks inside herdr-core (``hc``) terminals.

The hook keys the session map by the terminal address (``hc:<address>``) after
confirming it against the live core; the core is faked at ``request_sync``.
"""

import io
import json
import sys
from pathlib import Path

import pytest

from ccgram import hook
from ccgram.hook import _hc_hook_target, hook_main
from ccgram.multiplexer.hc_rpc import HcRpcError

_SESSION_ID = "6f0c6c0e-3a1f-4d55-9b1e-2f1d1a0c9e11"
_ADDRESS = "t1r"


def _record(state: str = "live", pid: int = 4242) -> dict:
    return {
        "address": _ADDRESS,
        "state": state,
        "labels": {"where": "repo / tab"},
        "foreground": {
            "argv": ["claude"],
            "cwd": "/repo",
            "name": "claude",
            "pid": pid,
        },
        "cwd": "/repo",
    }


class FakeCore:
    """Stands in for ``hc_rpc.request_sync``: answers ``terminal.get``."""

    def __init__(self) -> None:
        self.answer: dict | Exception = _record()
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, _path: str, method: str, params: dict, **_kw: object) -> dict:
        self.calls.append((method, params))
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


@pytest.fixture
def core(monkeypatch: pytest.MonkeyPatch) -> FakeCore:
    fake = FakeCore()
    monkeypatch.setattr(hook.hc_rpc, "request_sync", fake)
    monkeypatch.setattr(hook, "_ps_snapshot", lambda: {})
    return fake


class TestHookTarget:
    def test_live_terminal_is_the_identity(self, core) -> None:
        assert _hc_hook_target(_ADDRESS, "claude") == (_ADDRESS, "repo / tab")
        assert core.calls == [("terminal.get", {"terminal": _ADDRESS})]

    def test_finished_terminal_drops_the_hook(self, core) -> None:
        core.answer = _record(state="finished")
        assert _hc_hook_target(_ADDRESS, "claude") is None

    def test_unreachable_core_drops_the_hook(self, core) -> None:
        core.answer = HcRpcError("hc socket failed: refused")
        assert _hc_hook_target(_ADDRESS, "claude") is None

    def test_nested_claude_is_skipped(self, core, monkeypatch) -> None:
        monkeypatch.setattr(hook, "_ps_snapshot", lambda: {1: (0, 0, "S", "x")})
        seen: list[int] = []

        def nested(_snapshot: object, fg_pgid: int) -> bool:
            seen.append(fg_pgid)
            return True

        monkeypatch.setattr(hook, "_is_nested_below", nested)
        assert _hc_hook_target(_ADDRESS, "claude") is None
        # The foreground group leader hc reports stands in for the tty lookup.
        assert seen == [4242]

    def test_nested_check_is_claude_only(self, core, monkeypatch) -> None:
        monkeypatch.setattr(hook, "_ps_snapshot", lambda: {1: (0, 0, "S", "x")})
        monkeypatch.setattr(hook, "_is_nested_below", lambda *_a: True)
        assert _hc_hook_target(_ADDRESS, "codex") == (_ADDRESS, "repo / tab")


@pytest.fixture
def hook_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, core: FakeCore) -> Path:
    del core
    for key in (
        "TMUX_PANE",
        "HERDR_WORKSPACE_ID",
        "HERDR_PANE_ID",
        "AGTERM_SESSION_ID",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("CCGRAM_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HC_TERMINAL_ID", _ADDRESS)
    return tmp_path


def _hook(monkeypatch: pytest.MonkeyPatch, event: str, **extra: object) -> None:
    payload = {
        "session_id": _SESSION_ID,
        "cwd": "/repo",
        "hook_event_name": event,
        **extra,
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    hook_main()


def _session_map(home: Path) -> dict:
    return json.loads((home / "state" / "session_map.json").read_text())


def test_session_start_keys_the_map_by_address(hook_home: Path, monkeypatch) -> None:
    _hook(monkeypatch, "SessionStart")
    entry = _session_map(hook_home)[f"hc:{_ADDRESS}"]
    assert entry["session_id"] == _SESSION_ID
    assert entry["window_name"] == "repo / tab"
    assert entry["provider_name"] == "claude"


def test_later_hook_recovers_a_missed_session_start(
    hook_home: Path, monkeypatch
) -> None:
    # An agent started before ccgram's hook was installed never sent
    # SessionStart; its address is durable, so a later hook may bind it.
    transcript = hook_home / "transcript.jsonl"
    transcript.write_text("{}\n")
    _hook(monkeypatch, "Stop", transcript_path=str(transcript))
    entry = _session_map(hook_home)[f"hc:{_ADDRESS}"]
    assert entry["session_id"] == _SESSION_ID
    assert entry["transcript_path"] == str(transcript)
