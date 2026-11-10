# herdr-core (`hc`) backend — notes

Branch `feat/hc-backend`. This branch adds a fourth multiplexer backend, `CCGRAM_MULTIPLEXER=hc`, for agents running in [herdr-core](https://herdr.dev) terminals. These are the terminals behind the Herdr GUI app. The existing `herdr` backend for the Herdr TUI is unchanged.

## What was added

| File | Role |
| --- | --- |
| `src/ccgram/multiplexer/hc.py` | `HcManager`, the `Multiplexer` implementation. |
| `src/ccgram/multiplexer/hc_rpc.py` | Socket transport, stdlib only. It provides JSON-RPC requests (async, plus sync for the hook), raw-input injection, the event subscription, socket-path resolution and display names. |
| `src/ccgram/hc_targets.py` | Pure address validation (`t7`, `t1r`), shared by the neutral `self_identify` and `reconciliation` modules. |
| `registry.py`, `config.py` | `hc` is registered. Auto-detection order is `HERDR_PANE_ID` > `TMUX_PANE` > `HC_TERMINAL_ID` > `AGTERM_SESSION_ID`. |
| `self_identify.py`, `hook.py` | Hooks key the session map as `hc:<address>` from `HC_TERMINAL_ID`. See [Hooks](#hooks). |
| `reconciliation.py`, `session_lifecycle.py` | The `hc` id namespace, so reconciliation can prove dead terminals absent. |
| `doctor_cmd.py` | `ccgram doctor` checks that the hc socket answers. |
| Docs | `README.md`, `docs/architecture.md`, `.claude/rules/architecture.md` |
| Tests | `tests/ccgram/test_hc_backend.py`, `test_hc_rpc.py` (a real unix-socket server), `test_hc_hook.py`, plus hc cases in the registry, contract, boundary and self-identify tests. `tests/conftest.py` now strips `HC_*` env, so a suite run inside an hc terminal stays hermetic. |

## How hc maps onto the contract

- **Identity**
  - `window_id` is the terminal **address** (`t7`). hc documents it as "this terminal forever on this core": it survives core restarts and updates (`ids_stable_across_restart=True`).
  - Remote terminals (`machine/t7`) are never listed. Their hooks cannot reach this machine's session map.
- **Discovery** (`topic_eligible`): a live local terminal qualifies if its foreground argv classifies as a known agent. These are excluded:
  - ccgram's own terminal;
  - `_` names;
  - terminals another agent started (`parent` set);
  - terminals outside `CCGRAM_HC_LABELS=key=value,...`.

  Eligibility deliberately ignores the OSC 7501 status. The Herdr GUI's `hc attach` viewers mirror the status of the terminal they show, so status-based detection would create duplicate topics. A read-only listing of the live core adopts the five Claude terminals and skips the attach viewer and the terminal ccgram runs in.
- **Topic names**: the `ccgram.name` label (set by rename), then the hc `name`, then the GUI's `where` label (`space / tab`), then the title, then the folder.
- **cwd** is `foreground.cwd`, the folder the agent runs in now, such as a worktree. The terminal's `cwd` is only where it was launched.
- **Status**:
  - Programs report OSC 7501 words. `events.subscribe` pushes them into ccgram's `agent_status_cache`, and `app=claude-code` is normalised to `claude`.
  - `finished`/`removed` events become `window_died`, and each (re)connect is primed from hc's snapshot.
  - On `missed`, a drop, or 45 s without a heartbeat, the backend resubscribes with backoff.
- **Reads**:
  - `terminal.read` returns text without ANSI styling.
  - The polling read uses `lines=0`. A history read of a full-screen app (Claude Code) is *stitched* by scrolling the app, and polling must never do that to the user's view.
  - Explicit scrollback reads pass history and set `truncated` when hc reports the read incomplete. hc gives only the screen while the agent is `working`/`blocked`.
- **Input**:
  - **Text + Enter** is one `terminal.send`. hc pastes the text and presses the program's submit key once nobody has typed for about 3 s.
    - ccgram passes `force: true`, because hc otherwise refuses a `blocked` program and the person in Telegram is the one that guard waits for.
    - A forced send to a still-blocked program takes about 5 s to return, because hc watches for it to start working. The timeout is 20 s.
  - **tmux key names** (`Escape`, `C-c`, `BTab`, `M-Enter`, …) map to `terminal.keys`. hc encodes these for the program's current keyboard mode, including the kitty protocol Claude Code uses.
  - **Text without Enter**, and keys hc cannot name (`!`, uppercase letters, unicode, escape-sequence toolbar payloads), are written raw:
    - the backend opens a short framed `terminal.attach` at the terminal's **current** size, so no resize reaches the GUI;
    - it writes one AUTOMATIC_INPUT frame, then closes;
    - the frame layout (`<channel:u32 LE><kind:u8><len:u32 LE>`, kinds DATA=0, CREDIT=1, CLOSE=2, AUTOMATIC_INPUT=6) was read off `hc attach` and is **not in the published schema**.
- **Create**:
  - `terminal.run` with no argv starts the machine's login shell, then the agent command is typed into it. This mirrors tmux: the shell survives the agent.
  - The run uses `no_parent`, `outlive_parent`, `no_notify` and `keep`, and sets the `ccgram.name` label.
  - `CCGRAM_DIR` is passed through `env` so hooks in the new terminal write to this ccgram's state.
  - If the launch fails, the terminal is killed and removed.
- **Kill**: `terminal.kill`, then `terminal.rm`.

## Hooks

- The hook reads `HC_TERMINAL_ID`, then confirms with a 2 s `terminal.get` (on `HC_TERMINAL_SOCKET`) that the terminal is live.
- For Claude, it skips a hook from a claude nested under the terminal's foreground one. This is the same process-ancestry rule as tmux, with hc's foreground group leader standing in for the tty lookup.
- An unreachable core drops the hook (fail closed).
- `hc:` keys get `recover_missing` (as `agterm:` does): an agent started before the hook was installed is bound on its next hook (Stop, UserPromptSubmit, …) instead of needing a restart.

## Gaps / known limitations

- **No rename of a live terminal** in hc. `rename_window` sets the `ccgram.name` label, which ccgram's listing prefers. The Herdr GUI tab title is not changed: Herdr.app's CLI (`Herdr tab`) only lists, focuses and closes tabs.
- **No worktrees, workspaces, splits or sibling panes** (`native_worktrees`, `supports_workspace_selection` are False). `/split` and `/panes` report unsupported. The `/new` worktree step uses ccgram's own `git worktree add`.
- **Text only**: no ANSI in reads, so screenshots render with default colours (same as agterm).
- **Raw injection relies on the undocumented framed protocol.** If a future hc changes framing, only text-without-Enter, raw keys and `terminal.paste` break. Enter-submitted messages and named keys use the documented line API.
- **`claude attach` terminals** (e.g. `t1v`) are adopted, because their foreground is `claude`. They are clients of a background Claude, though, and may never fire hooks for that terminal.
- **Children are not auto-adopted.** Terminals started by another agent (`hc run --bg reviewer …`) are left to their parent. They still show in pickers and can be bound by hand.
- **Address aliasing**: addresses are stable only while the core's identity is. If hc's state is wiped (new `core.id`), old addresses can name new terminals. Reconciliation then sees them as live; re-bind the topics.
- **archfit is not installed here**, so `make arch-check` skipped. `make arch-guard` (the pytest boundary audits) passes. `.archfit.yaml` was not edited. The architecture owner should add `ccgram.multiplexer.hc` and `ccgram.multiplexer.hc_rpc` to `multiplexer_backends`, `ccgram.hc_targets` beside `ccgram.herdr_targets`, and the concrete-backend forbidden-dependency rules for `ccgram.multiplexer.hc`.
- `make test` shows 3 intermittent `test_selection.py` failures under `-n auto` that also occur on the base commit. They pass serially and on rerun.

## How to try it (without touching the running instance)

The running ccgram (`~/.ccgram`, uv tool v4.21.1) must keep its own bot token. Two bots polling one token conflict on `getUpdates`, so use a **separate test bot and forum group**.

1. Create a separate config dir with its own token, group and users:

   ```bash
   mkdir -p ~/.ccgram-hc
   cat > ~/.ccgram-hc/.env <<'EOF'
   TELEGRAM_BOT_TOKEN=<test bot token>
   ALLOWED_USERS=<your telegram id>
   CCGRAM_GROUP_ID=<test forum group id>
   CCGRAM_MULTIPLEXER=hc
   # optional: only adopt terminals in one Herdr space
   # CCGRAM_HC_LABELS=herdr.space=sp...
   EOF
   ```

2. Check it from the branch checkout, without installing anything:

   ```bash
   cd ~/ClaudeProjects/ccgram
   CCGRAM_DIR=~/.ccgram-hc uv run ccgram doctor
   CCGRAM_DIR=~/.ccgram-hc uv run ccgram status
   ```

3. Run it:

   ```bash
   CCGRAM_DIR=~/.ccgram-hc uv run ccgram run --config-dir ~/.ccgram-hc
   ```

   Running it inside an hc terminal also works. Its own terminal is then excluded, and the socket comes from `HC_TERMINAL_SOCKET`.

4. **Hooks.** Claude's `~/.claude/settings.json` hooks currently run the uv-tool ccgram (4.21.1), which does not know `HC_TERMINAL_ID`. Until hooks run this branch's code, Claude messages in hc terminals will not reach Telegram. Discovery, status, screenshots and sending still work. Both options below affect the live instance's hooks, so choose deliberately:
   - **Install the branch as the tool** (`uv tool install --force .`). The hook code is a superset: tmux/herdr/agterm identities behave as before, and hc terminals gain `hc:` keys.
   - **Or** run `uv run ccgram hook --install` from this checkout, which points the hooks at the branch's venv.

   Hooks write to `$CCGRAM_DIR`, else `~/.ccgram`.
   - Terminals created by the test bot get `CCGRAM_DIR` passed in.
   - Existing Herdr GUI terminals do not have it, so their hooks write `hc:` entries to `~/.ccgram/session_map.json`. In this branch, readers and pruning only touch their own prefix (`_dead_session_map_entries`). Whether 4.21.1 prunes foreign keys was not checked.
   - For a trial, prefer terminals the test bot creates (`/new`). Those receive `CCGRAM_DIR`.
   - To bind existing GUI terminals, point the instance at `~/.ccgram`, and only after the old instance is stopped.
   - Do **not** inject `CCGRAM_DIR` into the hc core's environment. That needs `hc server stop` plus a start (a restart keeps the old environment), and a stop ends every running agent: kept terminals come back as bare shells.

The backend was exercised end to end against a throwaway core (`HC_SESSION=cgt`, separate `XDG_STATE_HOME`). The run covered:

- create, then a typed launch command;
- `terminal.send`, raw injection (`!A`, `中文`) and named keys, all with exact bytes verified;
- capture, rename, kill and `window_died`;
- pushed `working`/`blocked`/`idle` status;
- a real `ccgram hook` SessionStart run inside a throwaway hc terminal, with only the environment hc injects (`HC_TERMINAL_ID`/`HC_TERMINAL_SOCKET`). It wrote `hc:t1` to a scratch `session_map.json`;
- `ccgram doctor` and `ccgram status`.

Against the real core, the run made `terminal.list`/`terminal.get` reads, plus screen reads during exploration. One early exploratory `terminal.read` with history on `t21` was stitched, which may have briefly scrolled that view. Nothing was sent to the real core's terminals.
