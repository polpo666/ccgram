"""herdr-core (``hc``) backend for the Multiplexer contract, via its socket API.

Anti-corruption layer over `herdr-core <https://herdr.dev>`_, the terminal
runtime behind the Herdr GUI app. Every hc JSON shape (``Terminal`` records,
``event`` notifications, framed attach) stays private to this module and
``hc_rpc``; callers see only the neutral value types from ``multiplexer.base``.

Identity: ``window_id`` is the terminal's ``address`` (``t7``, ``t1r``), which
hc documents as "this terminal forever on this core": it survives core
restarts and updates, so ``ids_stable_across_restart`` is True and no alias
reconciliation is needed. One terminal is one window is one pane. Terminals on
other machines (``vps/t3``) are never listed: their hooks cannot reach this
machine's session map.

How the contract maps onto hc:

* **Status is native.** Programs report OSC 7501 words (``idle``, ``working``,
  ``done``, ``blocked``, ``error``) and hc pushes every change on
  ``events.subscribe``, so status and death are push-first.
* **Reads are text.** ``terminal.read`` returns the grid as UTF-8 without SGR;
  ``with_ansi`` is ignored, as on agterm. The visible screen is read with
  ``lines=0`` on purpose: a history read of a full-screen app is *stitched* by
  scrolling it, which the polling loop must never do to a user's view.
* **Submitting is one call.** ``terminal.send`` pastes and presses the
  program's own submit key, waiting out a person's typing. It refuses a
  ``blocked`` program; ccgram passes ``force`` because the person answering in
  Telegram is exactly who that guard waits for.
* **Keys go through ``terminal.keys``**, which encodes them for the program's
  current keyboard modes (kitty protocol included) — better than raw bytes.
  What key names cannot express (text without Enter, ``!``, unicode, escape
  sequences from the toolbar) is injected raw through ``hc_rpc.inject``.
* **No rename, no worktrees, no workspaces, no splits.** hc does not rename a
  live terminal; ``rename_window`` records the name as the ``ccgram.name``
  label, which listings then prefer.

Discovery: a live local terminal whose foreground process is a known agent is
adoptable, except ccgram's own terminal, names starting with ``_``, terminals
another agent started (``parent`` set — those are that agent's helpers), and
anything outside the optional ``CCGRAM_HC_LABELS`` scope (``key=value,...``).
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from pathlib import Path

import structlog

from ..utils import log_throttled
from . import hc_rpc
from .base import (
    AgentStatus,
    CaptureResult,
    ForegroundInfo,
    MultiplexerCapabilities,
    MuxEvent,
    PaneDims,
    PaneInfo,
    TopicTargetResult,
    WindowRef,
    WorkspaceRef,
)

__all__ = ["HcManager"]

logger = structlog.get_logger()

# ``(method, params, timeout) -> result``; injected in tests.
HcRequester = Callable[[str, Mapping[str, object], float], Awaitable[dict]]
# ``(terminal, data, cols, rows) -> None``; injected in tests.
HcInjector = Callable[[str, bytes, int, int], Awaitable[None]]
# ``() -> snapshot item, then notifications``; injected in tests.
HcStreamOpener = Callable[[], AsyncGenerator[dict, None]]

_CALL_TIMEOUT_SECONDS = 8.0
# ``terminal.send`` waits for nobody to have typed for ~3 s, then watches a few
# seconds for the program to start working before it answers.
_SEND_TIMEOUT_SECONDS = 20.0
_STREAM_BACKOFF_BASE = 1.0
_STREAM_BACKOFF_MAX = 30.0

_LIVE = "live"
_FINISHED = "finished"

_HC_CAPABILITIES = MultiplexerCapabilities(
    name="hc",
    ids_stable_across_restart=True,
    exposes_pane_tty=False,
    native_agent_status=True,
    read_max_lines=None,
    self_identify_env="HC_TERMINAL_ID",
    supports_event_stream=True,
    native_worktrees=False,
    # Addresses are durable; display names are neither unique nor stable.
    supports_display_name_rebind=False,
    recovers_stale_ids_by_name=False,
    supports_workspace_selection=False,
    native_topic_targets=False,
    # ``foreground`` is the tty's foreground process group, as on tmux.
    supports_shell_prompt_markers=True,
)

# OSC 7501 ``app`` words → ccgram provider names.
_APP_PROVIDERS: dict[str, str] = {
    "claude-code": "claude",
    "claude": "claude",
    "codex": "codex",
    "gemini": "gemini",
    "gemini-cli": "gemini",
    "pi": "pi",
}

# tmux key names (callers send these with ``literal=False``) → hc key names.
_KEY_NAMES: dict[str, str] = {
    "Enter": "enter",
    "Escape": "esc",
    "Tab": "tab",
    "BTab": "shift+tab",
    "S-Tab": "shift+tab",
    "S-Enter": "shift+enter",
    "Space": "space",
    "BSpace": "backspace",
    "Up": "up",
    "Down": "down",
    "Left": "left",
    "Right": "right",
    "Home": "home",
    "End": "end",
    "PageUp": "pageup",
    "PageDown": "pagedown",
    "PPage": "pageup",
    "NPage": "pagedown",
    "Delete": "delete",
    "DC": "delete",
    **{f"F{n}": f"f{n}" for n in range(1, 13)},
}

# ``C-x``: a control chord is exactly the prefix plus one character.
_CONTROL_CHORD_LEN = 3

# Bracketed paste (DEC mode 2004), for multi-line text typed without Enter.
_PASTE_START = "\x1b[200~"
_PASTE_END = "\x1b[201~"


def _key_name(token: str) -> str | None:
    """Translate one tmux key name to an hc key name, or None.

    Single characters are left to raw input unless hc names them exactly:
    hc's ``A`` types ``a``, and it rejects most punctuation.
    """
    if token in _KEY_NAMES:
        return _KEY_NAMES[token]
    if len(token) == 1:
        return (
            token if token.isascii() and (token.islower() or token.isdigit()) else None
        )
    if token.startswith("M-"):
        rest = token[2:]
        inner = rest.lower() if len(rest) == 1 and rest.isalpha() else _key_name(rest)
        return None if inner is None else f"alt+{inner}"
    if (
        token.startswith("C-")
        and len(token) == _CONTROL_CHORD_LEN
        and token[2].isalpha()
    ):
        return f"ctrl+{token[2].lower()}"
    return None


def _key_segments(tokens: Sequence[str]) -> list[list[str] | bytes] | None:
    """Group tmux key tokens into ordered runs: hc key names, or raw bytes.

    A printable character hc cannot name (``!``, ``A``) is typed raw; an
    unknown multi-character name returns None so it is never typed as text.
    """
    segments: list[list[str] | bytes] = []
    for token in tokens:
        name = _key_name(token)
        if name is not None:
            if segments and isinstance(segments[-1], list):
                segments[-1].append(name)
            else:
                segments.append([name])
        elif len(token) == 1 and token.isprintable():
            data = token.encode("utf-8")
            if segments and isinstance(segments[-1], bytes):
                segments[-1] += data
            else:
                segments.append(data)
        else:
            return None
    return segments


def _parse_label_scope(raw: str) -> dict[str, str]:
    """Parse ``CCGRAM_HC_LABELS`` (``key=value,...``); empty means no scope."""
    scope: dict[str, str] = {}
    for part in raw.split(","):
        key, sep, value = part.strip().partition("=")
        if sep and key.strip():
            scope[key.strip()] = value.strip()
    return scope


def _validated_work_dir(work_dir: str) -> tuple[Path | None, str]:
    path = Path(work_dir).expanduser().resolve()
    if not path.exists():
        return None, f"Directory does not exist: {work_dir}"
    if not path.is_dir():
        return None, f"Not a directory: {work_dir}"
    return path, ""


def _foreground(record: Mapping[str, object]) -> Mapping[str, object]:
    foreground = record.get("foreground")
    return foreground if isinstance(foreground, Mapping) else {}


def _argv(record: Mapping[str, object]) -> list[str]:
    argv = _foreground(record).get("argv")
    return [str(part) for part in argv] if isinstance(argv, list) else []


def _runs_a_known_agent(record: Mapping[str, object]) -> bool:
    """Whether the terminal's foreground process is an agent ccgram can drive.

    Classified from the whole argv with the codebase's wrapper-skipping
    matcher, never from the 7501 ``app`` word: an ``hc attach`` client mirrors
    the status of the terminal it shows, and adopting it would duplicate a
    topic.
    """
    argv = _argv(record)
    if not argv:
        return False
    # Lazy: providers reach back into the multiplexer seam, so a module-level
    # import here would close the loop.
    from ..providers.process_detection import classify_provider_from_argv

    provider = classify_provider_from_argv(argv)
    return bool(provider) and provider != "shell"


def _agent_status(record: Mapping[str, object]) -> AgentStatus | None:
    status = record.get("status")
    if not isinstance(status, Mapping):
        return None
    state = status.get("state")
    if not isinstance(state, str) or not state.strip():
        return None
    app = status.get("app")
    msg = status.get("msg")
    app_name = app.strip() if isinstance(app, str) else ""
    return AgentStatus(
        state=state.strip(),
        agent=_APP_PROVIDERS.get(app_name, app_name),
        custom_status=msg.strip() if isinstance(msg, str) else "",
    )


def _snapshot_events(
    snapshot: Mapping[str, object], watched: set[str]
) -> list[MuxEvent]:
    """Prime status for watched live terminals; report watched finished ones."""
    events: list[MuxEvent] = []
    terminals = snapshot.get("terminals")
    for record in terminals if isinstance(terminals, list) else []:
        if not isinstance(record, dict):
            continue
        address = str(record.get("address") or "")
        if address not in watched:
            continue
        if record.get("state") == _FINISHED:
            events.append(MuxEvent(kind="window_died", window_id=address))
        elif (status := _agent_status(record)) is not None:
            events.append(
                MuxEvent(kind="agent_status", window_id=address, status=status)
            )
    return events


def _translate_event(
    params: Mapping[str, object], watched: set[str], snapshot_seq: int
) -> MuxEvent | None:
    """Translate one hc ``event`` notification for a watched terminal."""
    event_seq = params.get("seq")
    if isinstance(event_seq, int) and event_seq <= snapshot_seq:
        return None
    address = str(params.get("terminal") or "")
    if address not in watched:
        return None
    kind = params.get("type")
    if kind in {"finished", "removed"}:
        return MuxEvent(kind="window_died", window_id=address)
    if kind == "status":
        data = params.get("data")
        status = _agent_status(data) if isinstance(data, Mapping) else None
        # A cleared word (the program exited to a kept shell) must replace the
        # cached one, not leave a stale ``working`` standing.
        return MuxEvent(
            kind="agent_status",
            window_id=address,
            status=status or AgentStatus(state="unknown"),
        )
    return None


class HcManager:
    """Drives herdr-core terminals over its socket, behind the Multiplexer contract.

    Returns the neutral value types and exposes ``capabilities``. Methods
    return ``None``/``[]``/``False`` on failure like the other backends, so
    callers gate on results and never on an hc-specific error type.
    """

    @property
    def capabilities(self) -> MultiplexerCapabilities:
        """Return the static capability declaration for the hc backend."""
        return _HC_CAPABILITIES

    def __init__(
        self,
        *,
        socket_path: str | None = None,
        requester: HcRequester | None = None,
        injector: HcInjector | None = None,
        stream_opener: HcStreamOpener | None = None,
        own_terminal: str | None = None,
        label_scope: Mapping[str, str] | None = None,
    ) -> None:
        """Build the backend without touching the socket (I/O-free).

        Args:
            socket_path: the hc socket; defaults to how ``hc`` resolves it.
            requester: async ``(method, params, timeout) -> result`` for tests.
            injector: async ``(terminal, data, cols, rows)`` raw input for tests.
            stream_opener: event-stream factory for tests.
            own_terminal: the terminal ccgram runs in, never adopted; defaults
                to ``$HC_TERMINAL_ID``.
            label_scope: labels a terminal must carry to be listed; defaults to
                ``$CCGRAM_HC_LABELS`` (empty: every terminal).
        """
        self._socket_path = socket_path or hc_rpc.resolve_socket_path()
        self._request: HcRequester = requester or self._socket_request
        self._inject: HcInjector = injector or self._socket_inject
        self._open_stream: HcStreamOpener = stream_opener or self._socket_stream
        self._own_terminal = (
            own_terminal
            if own_terminal is not None
            else os.environ.get("HC_TERMINAL_ID", "")
        )
        self._label_scope = dict(
            label_scope
            if label_scope is not None
            else _parse_label_scope(os.environ.get("CCGRAM_HC_LABELS", ""))
        )

    # ── Socket plumbing (private) ──────────────────────────────────────

    async def _socket_request(
        self, method: str, params: Mapping[str, object], timeout: float
    ) -> dict:
        return await hc_rpc.request(self._socket_path, method, params, timeout=timeout)

    async def _socket_inject(
        self, terminal: str, data: bytes, cols: int, rows: int
    ) -> None:
        await hc_rpc.inject(self._socket_path, terminal, data, cols=cols, rows=rows)

    def _socket_stream(self) -> AsyncGenerator[dict, None]:
        return hc_rpc.subscribe(self._socket_path)

    async def _call(
        self,
        method: str,
        params: Mapping[str, object],
        *,
        timeout: float = _CALL_TIMEOUT_SECONDS,
    ) -> dict | None:
        """Make one request; None on any failure (logged at debug)."""
        try:
            return await self._request(method, params, timeout)
        except hc_rpc.HcRpcError as exc:
            logger.debug(
                "hc call failed", method=method, kind=exc.kind, error=exc.message
            )
            return None

    async def _get(self, window_id: str) -> dict | None:
        """The live terminal record for *window_id*, or None."""
        if not hc_rpc.is_hc_address(window_id):
            return None
        record = await self._call("terminal.get", {"terminal": window_id})
        if record is None or record.get("state") != _LIVE:
            return None
        return record

    async def _list(self) -> list[dict] | None:
        """Every live local terminal, or None when hc could not answer."""
        result = await self._call("terminal.list", {})
        terminals = None if result is None else result.get("terminals")
        if not isinstance(terminals, list):
            log_throttled(
                logger,
                "hc-terminal-list",
                "hc terminal.list failed; window listing unconfirmed",
                cooldown=60.0,
                level="warning",
            )
            return None
        return [
            record
            for record in terminals
            if isinstance(record, dict)
            and record.get("state") == _LIVE
            and hc_rpc.is_hc_address(str(record.get("address") or ""))
        ]

    def _is_visible(self, record: Mapping[str, object]) -> bool:
        """Whether pickers may offer this terminal (see module docs)."""
        address = str(record.get("address") or "")
        if self._own_terminal and address == self._own_terminal:
            return False
        if str(record.get("name") or "").startswith("_"):
            return False
        labels = record.get("labels")
        labels = labels if isinstance(labels, Mapping) else {}
        return all(labels.get(key) == value for key, value in self._label_scope.items())

    def _to_window(self, record: Mapping[str, object]) -> WindowRef:
        """Map one hc terminal record onto the neutral ``WindowRef``."""
        foreground = _foreground(record)
        argv = _argv(record)
        command = str(foreground.get("name") or "") or (
            os.path.basename(argv[0]) if argv else ""
        )
        # The launch folder is ``cwd``; the agent may have moved since (a
        # worktree), and transcript matching needs where it runs now.
        cwd = str(foreground.get("cwd") or record.get("cwd") or "")
        cols, rows = record.get("cols"), record.get("rows")
        return WindowRef(
            window_id=str(record.get("address") or ""),
            window_name=hc_rpc.terminal_display_name(record),
            cwd=cwd,
            pane_current_command=command,
            pane_tty="",
            pane_width=cols if isinstance(cols, int) else 0,
            pane_height=rows if isinstance(rows, int) else 0,
            topic_eligible=self._is_visible(record)
            and not record.get("parent")
            and _runs_a_known_agent(record),
        )

    # ── Multiplexer Protocol surface ───────────────────────────────────

    async def ensure_session(self) -> None:
        """Verify the hc core answers; hc owns its terminals, nothing to create."""
        if await self._call("server.status", {}) is None:
            raise RuntimeError(
                f"herdr-core is not reachable at {self._socket_path} — "
                "is it running (hc server status), and is HC_SESSION/HC_SOCKET right?"
            )

    async def list_windows(self) -> list[WindowRef]:
        """List the terminals offered for explicit selection ([] on failure)."""
        records = await self._list()
        if records is None:
            return []
        return [
            self._to_window(record) for record in records if self._is_visible(record)
        ]

    async def list_windows_for_reconciliation(self) -> list[WindowRef] | None:
        """Every live local terminal, or None when hc cannot confirm the listing.

        Unfiltered by visibility: cleanup asks what exists, so a bound terminal
        that left the label scope must not read as gone.
        """
        records = await self._list()
        if records is None:
            return None
        return [self._to_window(record) for record in records]

    async def window_exists(self, window_id: str) -> bool | None:
        """Ask hc about one terminal: True, False, or None if it could not say.

        ``not_found`` and a ``finished`` record are proof of absence; a
        transport failure or any other refusal is not.
        """
        if not hc_rpc.is_hc_address(window_id):
            return None
        try:
            record = await self._request(
                "terminal.get", {"terminal": window_id}, _CALL_TIMEOUT_SECONDS
            )
        except hc_rpc.HcRpcError as exc:
            return False if exc.kind == "not_found" else None
        state = record.get("state")
        if state == _LIVE:
            return True
        return False if state == _FINISHED else None

    async def list_workspaces(self) -> list[WorkspaceRef]:
        """hc has no workspaces (the Herdr GUI's spaces are labels on terminals)."""
        return []

    async def find_window_by_id(self, window_id: str) -> WindowRef | None:
        """Find a live terminal by address; None when it is gone or finished."""
        record = await self._get(window_id)
        return None if record is None else self._to_window(record)

    async def _read(self, window_id: str, lines: int) -> dict | None:
        if not hc_rpc.is_hc_address(window_id):
            return None
        return await self._call(
            "terminal.read", {"terminal": window_id, "lines": max(lines, 0)}
        )

    async def capture_pane(self, window_id: str, with_ansi: bool = False) -> str | None:
        """Capture the visible screen as text; ``with_ansi`` is ignored.

        ``lines=0`` reads the screen only, so a full-screen app is never
        scrolled to stitch history.
        """
        del with_ansi
        result = await self._read(window_id, 0)
        text = None if result is None else result.get("text")
        return text.strip() or None if isinstance(text, str) else None

    async def capture_pane_scrollback(
        self, window_id: str, history: int = 200
    ) -> str | None:
        """Capture the screen plus *history* lines (plain text)."""
        result = await self.capture_scrollback(window_id, history)
        return None if result is None else result.text.strip() or None

    async def capture_scrollback(
        self, window_id: str, lines: int = 200
    ) -> CaptureResult | None:
        """Capture with history; ``truncated`` when hc says part may be missing.

        While a full-screen agent is ``working``/``blocked``/``error`` hc
        returns only its screen and reports the read incomplete.
        """
        result = await self._read(window_id, lines)
        if result is None or not isinstance(text := result.get("text"), str):
            return None
        return CaptureResult(text=text, truncated=result.get("complete") is False)

    async def capture_pane_by_id(
        self,
        pane_id: str,
        *,
        with_ansi: bool = False,
        window_id: str | None = None,
    ) -> str | None:
        """A terminal has one pane, addressed by the terminal itself."""
        if window_id is None or pane_id != window_id:
            logger.warning("Rejected hc pane read for a foreign pane id")
            return None
        return await self.capture_pane(window_id, with_ansi=with_ansi)

    async def pane_dims(self, window_id: str) -> PaneDims | None:
        """The terminal's size in cells."""
        record = await self._get(window_id)
        if record is None:
            return None
        cols, rows = record.get("cols"), record.get("rows")
        if not isinstance(cols, int) or not isinstance(rows, int):
            return None
        return PaneDims(width=cols, height=rows)

    async def _inject_raw(self, window_id: str, data: bytes) -> bool:
        """Type raw bytes, attaching at the terminal's current size."""
        record = await self._get(window_id)
        if record is None:
            return False
        cols, rows = record.get("cols"), record.get("rows")
        if not isinstance(cols, int) or not isinstance(rows, int):
            return False
        try:
            await self._inject(window_id, data, cols, rows)
        except hc_rpc.HcRpcError as exc:
            logger.debug(
                "hc input injection failed", window_id=window_id, error=exc.message
            )
            return False
        return True

    async def _keys(self, window_id: str, names: list[str]) -> bool:
        return (
            await self._call("terminal.keys", {"terminal": window_id, "keys": names})
            is not None
        )

    async def send(
        self,
        window_id: str,
        text: str,
        *,
        enter: bool = True,
        literal: bool = True,
        raw: bool = False,
    ) -> bool:
        """Send text or tmux key names to the terminal.

        Literal text with Enter is one ``terminal.send``: hc pastes it (as a
        bracketed paste when the program enabled it) and presses the submit
        key. Literal text without Enter is typed raw, wrapped in bracketed
        paste when multi-line and not ``raw``. ``literal=False`` means *text*
        is whitespace-separated tmux key names.
        """
        if not hc_rpc.is_hc_address(window_id):
            return False
        if literal:
            return await self._send_literal(window_id, text, enter=enter, raw=raw)
        segments = _key_segments(text.split())
        if not segments:
            logger.debug("unknown hc key names", keys=text)
            return False
        if enter:
            if isinstance(segments[-1], list):
                segments[-1].append("enter")
            else:
                segments.append(["enter"])
        for segment in segments:
            if isinstance(segment, list):
                ok = await self._keys(window_id, segment)
            else:
                ok = await self._inject_raw(window_id, segment)
            if not ok:
                return False
        return True

    async def _send_literal(
        self, window_id: str, text: str, *, enter: bool, raw: bool
    ) -> bool:
        if text and enter:
            return (
                await self._call(
                    "terminal.send",
                    {"terminal": window_id, "text": text, "force": True},
                    timeout=_SEND_TIMEOUT_SECONDS,
                )
                is not None
            )
        if text:
            payload = (
                f"{_PASTE_START}{text}{_PASTE_END}"
                if not raw and "\n" in text
                else text
            )
            if not await self._inject_raw(window_id, payload.encode("utf-8")):
                return False
        return await self._keys(window_id, ["enter"]) if enter else True

    async def send_keys(
        self,
        window_id: str,
        text: str,
        enter: bool = True,
        literal: bool = True,
        *,
        raw: bool = False,
    ) -> bool:
        """Legacy alias of ``send``."""
        return await self.send(window_id, text, enter=enter, literal=literal, raw=raw)

    async def send_to_pane(
        self,
        pane_id: str,
        text: str,
        *,
        enter: bool = True,
        literal: bool = True,
        window_id: str | None = None,
        raw: bool = True,
    ) -> bool:
        """A terminal has one pane, addressed by the terminal itself."""
        if window_id is None or pane_id != window_id:
            logger.warning("Rejected hc pane write for a foreign pane id")
            return False
        return await self.send(window_id, text, enter=enter, literal=literal, raw=raw)

    async def send_keys_to_pane(
        self,
        pane_id: str,
        text: str,
        *,
        enter: bool = True,
        literal: bool = True,
        window_id: str | None = None,
    ) -> bool:
        """Legacy alias of ``send_to_pane``."""
        return await self.send_to_pane(
            pane_id, text, enter=enter, literal=literal, window_id=window_id
        )

    async def kill_window(self, window_id: str) -> bool:
        """End the terminal's program and remove its finished record."""
        if not hc_rpc.is_hc_address(window_id):
            return False
        try:
            await self._request(
                "terminal.kill", {"terminal": window_id}, _CALL_TIMEOUT_SECONDS
            )
        except hc_rpc.HcRpcError as exc:
            if exc.kind not in {"finished", "not_found"}:
                logger.debug("hc kill failed", window_id=window_id, error=exc.message)
                return False
        await self._call("terminal.rm", {"terminal": window_id})
        return True

    async def rename_window(self, window_id: str, new_name: str) -> bool:
        """Record the name as a label; hc cannot rename a live terminal."""
        if not hc_rpc.is_hc_address(window_id):
            return False
        return (
            await self._call(
                "terminal.label",
                {"terminal": window_id, "labels": {hc_rpc.NAME_LABEL: new_name}},
            )
            is not None
        )

    async def list_panes(self, window_id: str) -> list[PaneInfo]:
        """No sibling panes: an hc terminal is a single pane."""
        del window_id
        return []

    async def split_window(self, window_id: str) -> str | None:
        """Always None — hc has no panes to split (the GUI lays out terminals)."""
        del window_id
        return None

    async def _discard(self, window_id: str, reason: str) -> str:
        """Remove a half-created terminal; say so when that fails too."""
        if await self.kill_window(window_id):
            return reason
        logger.warning(
            "hc terminal left behind after a failed creation", window_id=window_id
        )
        return f"{reason}; the terminal {window_id} could not be removed"

    async def create_window(
        self,
        work_dir: str,
        window_name: str | None = None,
        start_agent: bool = True,
        agent_args: str = "",
        launch_command: str | None = None,
        *,
        workspace_id: str | None = None,
    ) -> tuple[bool, str, str, str]:
        """Start a login shell in a new terminal and type the agent command.

        Mirrors tmux: the shell outlives the agent, so a mistyped command or an
        agent exit leaves a usable terminal instead of a vanished one. The
        terminal is detached from ccgram (``no_parent``) so it never ends with
        a ccgram that runs inside hc, and ``keep`` replaces a lost program with
        a shell like the Herdr GUI's own terminals. ``CCGRAM_DIR`` is passed
        through so hooks in it write to this ccgram's state.
        """
        del workspace_id
        path, problem = _validated_work_dir(work_dir)
        if path is None:
            return (False, problem, "", "")
        params: dict[str, object] = {
            "cwd": str(path),
            "keep": True,
            "no_parent": True,
            "no_notify": True,
            "outlive_parent": True,
        }
        if window_name:
            params["labels"] = {hc_rpc.NAME_LABEL: window_name}
        if ccgram_dir := os.environ.get("CCGRAM_DIR"):
            params["env"] = {"CCGRAM_DIR": ccgram_dir}
        record = await self._call("terminal.run", params)
        window_id = "" if record is None else str(record.get("address") or "")
        if not hc_rpc.is_hc_address(window_id):
            return (False, "hc could not start a terminal", "", "")
        command = launch_command
        if command and agent_args:
            command = f"{command} {agent_args}"
        if start_agent and command and not await self.send(window_id, command):
            message = await self._discard(
                window_id, f"could not start {command!r} in the new terminal"
            )
            return (False, message, "", "")
        name = window_name or hc_rpc.terminal_display_name(record or {}) or path.name
        return (True, "", name, window_id)

    async def create_topic_target(
        self,
        work_dir: str,
        *,
        launch_command: str | None,
        workspace_id: str | None,
        window_name: str | None = None,
        agent_args: str = "",
    ) -> TopicTargetResult:
        """Create a topic target; the terminal address is its durable target."""
        success, message, label, window_id = await self.create_window(
            work_dir,
            window_name=window_name,
            launch_command=launch_command,
            agent_args=agent_args,
            workspace_id=workspace_id,
        )
        if not success:
            raise RuntimeError(message)
        return TopicTargetResult(target_id=window_id, label=label, window_id=window_id)

    async def create_worktree_window(
        self,
        repo_path: str,
        worktree_path: str,
        branch: str,
        *,
        window_name: str | None = None,
        launch_command: str | None = None,
    ) -> tuple[bool, str, str, str]:
        """Not supported (``native_worktrees`` is False)."""
        del repo_path, worktree_path, branch, window_name, launch_command
        return (False, "hc does not create worktrees natively", "", "")

    async def foreground(self, window_id: str) -> ForegroundInfo | None:
        """The terminal's foreground process: hc reports its group leader."""
        record = await self._get(window_id)
        if record is None:
            return None
        argv = _argv(record)
        if not argv:
            return None
        foreground = _foreground(record)
        pid = foreground.get("pid")
        pid = pid if isinstance(pid, int) else 0
        return ForegroundInfo(
            pid=pid,
            pgid=pid,
            argv=argv,
            cwd=str(foreground.get("cwd") or record.get("cwd") or ""),
            tty="",
        )

    async def agent_status(self, window_id: str) -> AgentStatus | None:
        """The program's latest OSC 7501 word, or None if it never reported."""
        record = await self._get(window_id)
        return None if record is None else _agent_status(record)

    async def watch_events(
        self, window_ids: Sequence[str]
    ) -> AsyncGenerator[MuxEvent, None]:
        """Stream status and death for *window_ids* (see ``Multiplexer``).

        Each (re)connect starts from hc's snapshot, which primes the status of
        every watched live terminal and reports one that finished while the
        stream was down. Events at or below the snapshot's ``seq`` are already
        in it. ``missed`` (a slow subscriber) and a dropped or silent stream
        resubscribe with backoff. A watched terminal absent from the snapshot
        is left to the poll loop, which confirms absence before acting.
        """
        watched = set(window_ids)
        backoff = _STREAM_BACKOFF_BASE
        while True:
            try:
                async for event in self._stream_once(watched):
                    backoff = _STREAM_BACKOFF_BASE
                    yield event
            except hc_rpc.HcRpcError as exc:
                log_throttled(
                    logger,
                    "hc-event-stream",
                    "hc event stream dropped (%s); reconnecting",
                    exc.message,
                    cooldown=60.0,
                    level="warning",
                )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _STREAM_BACKOFF_MAX)

    async def _stream_once(self, watched: set[str]) -> AsyncGenerator[MuxEvent, None]:
        """One subscription: snapshot events, then translated notifications.

        Returns (rather than raising) on ``missed`` so the caller resubscribes.
        """
        stream = self._open_stream()
        try:
            seq = -1
            async for message in stream:
                method = message.get("method")
                params = message.get("params")
                params = params if isinstance(params, dict) else {}
                if method == "snapshot":
                    raw_seq = params.get("seq")
                    seq = raw_seq if isinstance(raw_seq, int) else -1
                    for event in _snapshot_events(params, watched):
                        yield event
                elif method == "missed":
                    return
                elif method == "event":
                    event = _translate_event(params, watched, seq)
                    if event is not None:
                        yield event
        finally:
            await stream.aclose()

    async def get_pane_title(self, window_id: str) -> str:
        """The terminal title the program set, or ''."""
        record = await self._get(window_id)
        return "" if record is None else str(record.get("title") or "")

    async def stamp_pane_title(self, window_id: str, provider_name: str) -> None:
        """No-op: the title is the program's own; detection reads foreground argv."""
        del window_id, provider_name
