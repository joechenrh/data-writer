# TUI Dashboard Refactor — Design Spec

**Date:** 2026-04-07
**Status:** Approved (brainstorming)
**Author:** brainstorming session with Claude
**Scope:** Frontend (HTML/CSS/JS) of `data-writer` web UI. No server, schema, or generator changes.

## Goals

Replace the generic SaaS form (white card on light gray, ChatGPT green accent, Apple system font) with a distinctive **TUI dashboard** aesthetic — visually evoking lazygit / k9s / htop — while preserving every existing piece of functionality and adding first-class display of the new EC2 shard pool.

The result must:

1. Be unmistakably *this tool*. A user should recognize it from across the room.
2. Be at least as productive as today's UI for the existing flow (create job → watch progress → cancel).
3. Surface the new sharded EC2 worker pool (shards per task, per-shard progress) as a first-class concept.
4. Stay vanilla HTML/CSS/JS. No build step, no framework. The current `public/` static-file embed pipeline stays.

## Non-Goals

- No changes to the database schema, generator, task model, or any server logic beyond what's listed below.
- **The only allowed server change** is exposing the existing `worker_total` and `worker_done` columns in the `GET /api/tasks` and `GET /api/status` JSON responses, so the UI can display shard count and per-task progress accurately. This is a ~5-line change in `src/server/handler.go` and is in scope.
- No new features beyond the UI restructure (e.g., no log streaming, no SQL editor with autocomplete).
- No mobile/touch optimization. This is a desktop power-user tool.
- No real terminal emulation (no xterm.js, no ANSI parsing). The "terminal" is purely aesthetic.

## Aesthetic Direction

**Phosphor-green TUI dashboard.** Picked from a 4-way comparison (brutalist / phosphor terminal / editorial print / cyber blueprint) and refined through a 3-way "how literal" choice (CRT / modern dev terminal / TUI dashboard). The chosen flavor is dense, multi-pane, keyboard-surfaced, and inspired by lazygit and htop rather than retro CRT or modern Electron-style dev tools.

### Color palette

Defined as CSS variables on the body. All colors are tuned for readability against the dark green-black background.

```
--bg:        #050d08    /* page background */
--bg-alt:    #0a1a12    /* subtle panel tinting */
--bg-pane:   #061410    /* input fields, code blocks */
--line:      #1a3a26    /* solid borders */
--line-soft: #11281b    /* dotted/dashed dividers between rows */

--txt:       #b8eac9    /* primary text */
--txt-dim:   #6fcc97    /* secondary text, labels */
--txt-mute:  #4a8466    /* tertiary text, hints */

--green:     #6cf09a    /* primary accent — active/ok/headings */
--amber:     #f0c674    /* warnings, pending, ETA */
--cyan:      #6cd0f0    /* info, AI assist, hostnames */
--magenta:   #f06cd0    /* SQL keywords */
--red:       #f06c6c    /* failures, danger */
```

State color mapping:

| State        | Color   |
| ------------ | ------- |
| pending      | amber   |
| launching    | cyan    |
| running      | green   |
| completed    | txt-mute (de-emphasized; the task is done) |
| failed       | red     |

### Typography

- **One font, everywhere:** JetBrains Mono. Loaded from Google Fonts with a fallback chain to `IBM Plex Mono`, `ui-monospace`, `SF Mono`, `Consolas`, `monospace`.
- **Font sizes:** 9.5px (labels), 10px (status bar), 11px (body), 11.5px (default), 13px (pane titles).
- **Case:** Pane titles and field labels are uppercase with letter-spacing 0.08–0.12em. Body text is sentence case.
- **Line-height:** 1.45 default, 1.55 for the SQL block.

### Visual decoration

- **Subtle scanline overlay** on the whole "screen" using `repeating-linear-gradient` at ~2.5% opacity. Just enough texture, not a full CRT effect. Disabled at `prefers-reduced-motion: reduce`.
- **Blinking caret** in the schema block (`▌`, 1.1s `step-end` infinite). Disabled at `prefers-reduced-motion: reduce`.
- **No box shadows** except a single ambient outer glow on the page wrapper (`0 0 60px rgba(108,240,154,0.05)`).
- **Borders are everywhere** — solid lines for panes, dotted/dashed for row separators, 2px green left-bar on input fields.
- **No rounded corners.** TUIs don't round.

## Layout

Three-row vertical stack at full viewport height:

```
┌────────────────────────────────────────────────────┐
│ TOP AIRLINE (24px)                                 │
├────────────────────────┬───────────────────────────┤
│                        │ ┌─────────────────────┐   │
│                        │ │ QUEUE (top)         │   │
│   FORM PANE            │ ├─────────────────────┤   │
│   (~45% width)         │ │ DETAIL (bottom)     │   │
│                        │ └─────────────────────┘   │
│                        │ (~55% width)              │
├────────────────────────┴───────────────────────────┤
│ BOTTOM STATUS BAR (22px)                           │
└────────────────────────────────────────────────────┘
```

- **Top airline:** brand chevron (`DW` on green), hostname, current path/state, queue summary, clock + connection dot.
- **Form pane (left):** schema block + AI assist line + path + grid of files/rows/format + grid of target/subdirs + footer with size estimate. Pane title `[ NEW JOB ]` with shortcut `n`.
- **Right pane:** vertically split. Top is the queue table (`[ QUEUE ]`, shortcut `tab`). Bottom is the selected-task detail panel with per-shard progress bars (`[ DETAIL · #14 ]`).
- **Bottom status bar:** keystroke hints on the left (`n·new ↵·run tab·focus /·ai x·cancel r·reload ?·help :·cmd`), connection/health status on the right.

### Responsive behavior

- ≥1100px: full two-pane layout as above.
- 720–1099px: same layout but with the form pane shrinking to 380px min-width and the right pane absorbing the rest.
- <720px: single-column stack — top airline, form pane, queue, detail, bottom status bar. No horizontal scroll.

## Components

Each component is described as: what it does, what it depends on, what new state it needs.

### 1. `<dw-screen>` shell

The page wrapper. Sets the CSS variables, applies the scanline overlay, owns the focus router and the keyboard handler. Renders the three rows (airline, body grid, status bar).

**State:** focused pane (`form` | `queue`).

### 2. `<dw-airline>` (top status bar)

Read-only. Displays brand, hostname (`datawriter@db9` from `window.location.host`), current path label, queue summary (poll-derived), and a live clock.

**State:** clock tick (1s), queue summary (derived from `loadTasks()` results).

### 3. `<dw-form>` (NEW JOB pane)

Owns the existing form. Same fields, same validation, same submit. Visual restructure only.

- **Schema field** is a `<textarea>` styled to look like a code block: green left bar, monospace, padded. Syntax highlighting is **optional polish** — if the implementation time allows, render a transparent `<pre>` overlay with cheap regex coloring (`CREATE TABLE`, types, comments, strings) kept in sync via `input` event. If not, plain monospace text is fine and the design still reads as a TUI.
- **AI assist** lives directly under the schema as a vim-style command line: `/ <input> [↵ apply  esc dismiss]`. Toggled by pressing `/` while focused in the form pane, or by clicking. Same `/api/ai-assist` POST.
- **Path** is a single full-width input. The credentials section auto-expands when the path starts with `s3://`, same logic as today.
- **Grid of files / rows / format** as 3 narrow inputs.
- **Grid of target / subdirs** as 2 narrow inputs.
- **Footer line** shows: schema validity (`✓ schema valid` or `× missing schema name`) and a simple count summary (`100 × 60K rows · csv · ec2`). No size or shard estimate — the client cannot accurately predict either without column-type information that only the Go parser has. The server's `floor(bytes/5TiB)` decision is invisible to the form; users see actual shard count once the task starts running.

**State:** form values, validation result, expanded credentials section.

### 4. `<dw-queue>` (right pane top)

Replaces the existing task table. Same data source (`/api/tasks`, polled every 2s while there's an active task). Same row contents (id, state, target, progress, files, size, age) but rendered as a TUI table:

- Selectable row (one at a time). Click or `↑/↓`.
- `▶` marker on the selected row, green-tinted background.
- Compact progress bar (80px wide, 6px tall) with right-aligned percentage label.
- State as a small color-coded chip (see palette).
- `target` column shows shard count alongside (`ec2 ×4` when `worker_total > 1`, plain `ec2` / `local` otherwise). Requires `worker_total` in `/api/tasks` JSON — covered by the in-scope server change in the Non-Goals section.

**State:** task list (from poll), selected task id (defaults to most recent active).

### 5. `<dw-detail>` (right pane bottom)

Shows the selected task's full info. Replaces the per-row "actions popup" with a permanently visible detail panel.

- Header: `[ DETAIL · #<id> ]` plus inline shortcuts (`↵ open · l logs` — `l logs` is a v2 stub for now).
- Key/value rows: prefix, path, progress (with absolute counts), eta (computed client-side from progress rate over time, falling back to "—" until two polls have observed progress), created (absolute + relative).
- **Shard summary:** if `worker_total > 1`, render a compact line `shards · N workers · M done` derived from `worker_total` and `worker_done`. **No per-shard progress bars in v1** — that would require either a new `/api/shards` endpoint or per-shard rows in the DB, both of which are out of scope. The compact summary is enough to surface that sharding is happening.

**State:** none of its own — derives from queue selection.

### 6. `<dw-statusbar>` (bottom)

Static keystroke legend on the left, live connection status on the right.

- Left: `n·new ↵·run tab·focus /·ai x·cancel r·reload ?·help :·cmd`.
- Right: db9 dot (always green if poll succeeded recently), launcher dot (best-effort: green if any ec2 task has been observed in the last 24h), running-task indicator.

### 7. Help overlay (`?`)

Replaces the existing `Don't know how to write COMMENT specs?` modal. Triggered by `?` or by clicking the `[?]` icon on the schema label. Renders as a full-screen overlay (semi-transparent black backdrop, centered TUI panel with bordered title `[ HELP · COMMENT OPTIONS ]`). Same content as today (the column-comment options reference table). Dismissed by `esc`, `?`, or clicking outside.

### 8. Format options (`f` or click)

Today's "Format Options" modal becomes an **inline expansion** under the format field — clicking the format dropdown or pressing `f` opens a small section with CSV or Parquet controls right in the form pane. No modal. Closing happens by selecting a different field or pressing `esc`.

### 9. Keyboard router

A single delegated `keydown` listener on `<dw-screen>`:

| Key            | Action |
| -------------- | ------ |
| `n`            | focus the schema textarea (new job) |
| `↵` (enter, in form) | submit the form |
| `tab`          | toggle focus between form and queue |
| `↑` / `↓`      | move queue selection (when queue focused) |
| `/`            | open AI assist line (when form focused) |
| `x`            | cancel selected queued task |
| `r`            | refresh tasks |
| `?`            | toggle help overlay |
| `esc`          | close any overlay; clear AI line |
| `:`            | (reserved — future command palette; in v1 only logs to console) |

Shortcuts are best-effort. Any input that already has focus and consumes the key (e.g. typing `n` inside the schema) takes precedence — the router checks the active element before acting.

## Interactions preserved from today

- Form submit → `POST /api/create` (unchanged payload).
- Task polling → `GET /api/tasks` every 2s while there's an active task; stop polling when nothing is active. Same as today.
- Cancel → `POST /api/cancel?id=N`. Triggered by `x` on selected row, or by a "cancel" item in a small inline action popup that opens on click of a row's actions cell (kept for mouse users).
- AI assist → `POST /api/ai-assist`. Same payload, same response handling.
- SQL inline validation against `CREATE TABLE schema.table` regex — same as today, just rendered as the footer "✓/×" indicator instead of a red border.

## File structure

The current `public/` directory has three files (`index.html`, `app.js`, `style.css`) plus an unrelated `SKILL.md`. Refactor keeps the same three files but doubles their content. No new files unless the SQL syntax-highlight overlay grows enough to warrant its own module — judgment call during implementation.

```
src/server/public/
├── index.html      # restructured; new TUI markup; cache-bust v=34
├── app.js          # extended; keyboard router, focus state, detail panel, clock, syntax overlay
├── style.css       # rewritten; CSS variables, TUI grid, all the new component styles
└── SKILL.md        # unchanged
```

The Go embed (`//go:embed all:public`) picks up changes automatically. The `?v=` query string in `index.html` should be bumped to bust browser caches.

## Accessibility

- All interactive elements remain real `<button>` / `<input>` / `<a>` elements (not styled `<div>`). The TUI styling is CSS-only.
- Focus rings are visible — green outline (`0 0 0 1px var(--green)`) on focused inputs, and a green left-border on the focused pane.
- Color is never the sole signal: every state chip has a text label (`RUNNING`, `done`, `FAILED`).
- Scanline overlay and blinking caret respect `prefers-reduced-motion: reduce`.
- All content remains readable at 200% browser zoom (the layout collapses to single-column at narrow widths).

## Out of scope (explicitly)

- No real keyboard navigation modes (no `NORMAL` / `INSERT` modes; no vim modal editing).
- No command palette (`:` is reserved for v2).
- No log streaming or SSE.
- No SQL syntax-aware editor (just cheap regex overlay).
- No theme switching (this *is* the theme).

## Risks

1. **The one allowed server change must actually happen.** `worker_total` and `worker_done` need to be added to the `/api/tasks` and `/api/status` JSON responses (handler.go). If the implementation plan forgets this, the queue will show plain `ec2` for sharded tasks and the detail panel will show "shards · 1 worker" everywhere. ~5 lines of Go.
2. **Cache-bust.** The current `index.html` references `style.css?v=33` and `app.js?v=33`. The implementation plan must bump these or the browser will serve stale assets.
3. **Font loading.** JetBrains Mono is fetched from Google Fonts. If the user's network blocks Google, the fallback chain (`IBM Plex Mono` → `ui-monospace`) keeps the design coherent.
4. **Scope creep.** Easy to keep adding TUI flourishes. Implementation plan should hard-cap "polish" tasks and prefer to ship the layout first. Specifically: SQL syntax highlighting and the `:` command palette stub are explicitly optional and can be cut.

## Open questions resolved during brainstorming

| Question | Answer |
| -------- | ------ |
| Aesthetic direction? | Phosphor terminal (B), specifically TUI dashboard variant (3) |
| Layout? | 2-pane body — form left ~45%, queue+detail right ~55% |
| Modal handling? | Help → full-screen overlay. Format options → inline expansion. Cancel → keystroke + small popup. |
| Color palette? | Phosphor green primary + amber/cyan/magenta/red accents per state |
| Keyboard navigation? | Best-effort shortcuts (n, ↵, tab, /, x, r, ?, esc); not full vim modal |
| Microcopy tone? | All-lowercase mono labels; UPPERCASE pane titles; sentence case body |
| Per-shard progress? | Compact `shards · N workers · M done` line in v1; per-shard bars deferred |
| Server changes? | Only `worker_total` + `worker_done` exposed in `/api/tasks` and `/api/status` JSON |

## Mockup reference

The approved high-fidelity mockup is preserved at:
`.superpowers/brainstorm/1085345-1775556525/content/tui-fullpage.html`

This is the visual source of truth for the implementation plan.
