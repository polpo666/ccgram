"""Backend-neutral hook identity resolver.

The Claude Code hook runs as a separate process spawned inside a multiplexer
pane; it cannot import bot config or wire the ``multiplexer`` proxy. It only
needs to answer "which window am I?" from the environment. Each backend exposes
that differently — tmux via ``$TMUX_PANE`` + ``tmux display-message``, herdr via
``$HERDR_PANE_ID``, hc via ``$HC_TERMINAL_ID``, agterm via
``$AGTERM_SESSION_ID`` — so this module picks the
backend by which ``self_identify_env`` variable is present (never a ``name ==
"<backend>"`` conditional) and returns a neutral ``SelfIdentity``.

The tmux probe (a ``display-message`` subprocess) is injected as ``tmux_query``
so this module stays I/O-free and table-testable; the hook supplies its own
``_resolve_window_id`` as the default probe. The herdr branch resolves the
``(workspace_id, pane_id)`` locator through an injected ``herdr_query``. It
never falls back to a pane or tab identifier: absent, missing, or ambiguous
live records cause the hook to skip its write.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from ..hc_targets import is_hc_address

# tmux_query returns ``(session_window_key, window_id, window_name, pane_tty)``
# or None on failure — the exact shape of ``hook._resolve_window_id``.
TmuxQuery = Callable[[str], "tuple[str, str, str, str] | None"]

# herdr_query returns one opaque session target for a ``(workspace_id, pane_id)``
# locator, or None when no unique live record exists.  The resolver must fail
# closed rather than bind a pane or tab key.
HerdrQuery = Callable[[str, str], "str | None"]

# hc_query checks one hc terminal address against the live core and returns
# ``(address, window_name)``, or None to drop the hook (a finished terminal, a
# nested agent, an unreachable core).
HcQuery = Callable[[str], "tuple[str, str] | None"]


@dataclass(frozen=True)
class SelfIdentity:
    """Neutral identity of the window that fired the hook.

    ``session_window_key`` is the ``session_map.json`` key (``<session>:<id>``
    for tmux, ``herdr:<opaque-target-id>`` for herdr, ``hc:<address>`` for hc,
    ``agterm:<session-uuid>``
    for an agterm primary, or ``agterm:<guarded-split-target>`` for its peer).
    ``pane_tty`` is tmux-only
    (herdr does not expose a tty).
    """

    mux: str
    session_window_key: str
    window_id: str
    window_name: str
    pane_tty: str = ""


def _hc_identity(hc_terminal: str, hc_query: HcQuery | None) -> SelfIdentity | None:
    """The hc identity for ``$HC_TERMINAL_ID``, or None to drop the hook."""
    if not is_hc_address(hc_terminal):
        return None
    if hc_query is None:
        address, name = hc_terminal, ""
    else:
        resolved = hc_query(hc_terminal)
        if resolved is None:
            return None
        address, name = resolved
    return SelfIdentity(
        mux="hc",
        session_window_key=f"hc:{address}",
        window_id=address,
        window_name=name,
    )


def resolve_self_identity(
    env: Mapping[str, str],
    *,
    tmux_query: TmuxQuery,
    herdr_query: HerdrQuery | None = None,
    agterm_query: Callable[[str], tuple[str, str] | None] | None = None,
    hc_query: HcQuery | None = None,
) -> SelfIdentity | None:
    """Resolve the firing window's identity from ``env``.

    Dispatches on which backend's ``self_identify_env`` var is present:
    ``$TMUX_PANE`` → tmux (via ``tmux_query``), ``$HERDR_PANE_ID`` → herdr.
    Returns None when none is set or the tmux probe fails (today's
    "cannot determine window" path).

    The order is fixed: tmux, then herdr, then hc, then agterm. It is a precedence list,
    not a rule about which multiplexer is innermost. tmux is first because it
    is the established path and a herdr pane inside a tmux pane reports the
    tmux identity. agterm is last because its variable is the least specific
    evidence of anything: every shell agterm spawns inherits
    ``AGTERM_SESSION_ID``, including one running a nested tmux or herdr, so
    checking it earlier would claim panes owned by the inner multiplexer.
    ``HC_TERMINAL_ID`` has the same inheritance problem one level down: a tmux
    or herdr running inside an hc terminal inherits it, so hc follows both.

    For hc the address is the identity (``hc:<address>``). Only a local
    address qualifies (``vps/t3`` belongs to another machine's core);
    ``hc_query`` confirms it against the live core and returns None for a
    nested agent, and without a query the address is taken as is.

    For herdr: ``herdr_query(workspace_id, pane_id)`` resolves the exact live
    locator to a guarded session target, so ``session_window_key`` becomes
    ``herdr:<opaque-target-id>``. Returns None when either locator component is
    unavailable or the probe has zero or multiple matches; the hook then skips
    the session_map write rather than recording a location as identity.
    """
    tmux_pane = env.get("TMUX_PANE", "")
    if tmux_pane:
        resolved = tmux_query(tmux_pane)
        if resolved is None:
            return None
        session_window_key, window_id, window_name, pane_tty = resolved
        return SelfIdentity(
            mux="tmux",
            session_window_key=session_window_key,
            window_id=window_id,
            window_name=window_name,
            pane_tty=pane_tty,
        )

    herdr_pane = env.get("HERDR_PANE_ID", "")
    if herdr_pane:
        workspace_id = env.get("HERDR_WORKSPACE_ID", "")
        target_id = (
            herdr_query(workspace_id, herdr_pane)
            if herdr_query is not None and workspace_id
            else None
        )
        if target_id is None:
            return None
        return SelfIdentity(
            mux="herdr",
            session_window_key=f"herdr:{target_id}",
            window_id=target_id,
            window_name="",
        )

    hc_terminal = env.get("HC_TERMINAL_ID", "")
    if hc_terminal:
        return _hc_identity(hc_terminal, hc_query)

    # agterm is checked last on purpose. ``AGTERM_SESSION_ID`` belongs to the
    # outer terminal and every shell it spawns inherits it, including one
    # running a nested tmux or herdr session, so an earlier check would claim
    # panes that belong to the inner multiplexer. Modern hooks inject a live
    # agent probe to distinguish the split peer; legacy callers retain the UUID.
    agterm_session = env.get("AGTERM_SESSION_ID", "")
    if agterm_session:
        if agterm_query is not None:
            resolved = agterm_query(agterm_session)
            if resolved is None:
                return None
            target_id, name = resolved
        else:
            target_id, name = agterm_session, ""
        return SelfIdentity(
            mux="agterm",
            session_window_key=f"agterm:{target_id}",
            window_id=target_id,
            window_name=name,
        )

    return None
