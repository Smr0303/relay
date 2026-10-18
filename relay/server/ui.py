"""Operator dashboard: list and filter runs, read a run's history, act on it.

Server-rendered HTML + htmx (no build step). Reads Postgres only; actions call the same
Engine methods as the API (signal, cancel, resume).
"""

import html
import json
from datetime import datetime

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from relay.server.engine import Conflict, Engine, NotFound

STATUSES = ["running", "suspended", "completed", "failed", "cancelled", "parked", "continued"]

CSS = """
:root { --bg:#f7f7f5; --fg:#1d1d1b; --muted:#6b6b66; --line:#e2e1dc; --card:#fff;
        --ok:#1f7a4d; --bad:#b3261e; --warn:#9a6700; --run:#2457c5; --chip:#efeee9; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#151514; --fg:#ecebe6; --muted:#9b9a94; --line:#2c2c2a; --card:#1d1d1b;
          --ok:#5cc28f; --bad:#ef8a80; --warn:#e0b45a; --run:#8fb0ff; --chip:#262624; } }
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--fg);
       font:14px/1.45 ui-sans-serif, system-ui, -apple-system, sans-serif; }
header { display:flex; gap:16px; align-items:baseline; padding:16px 24px;
         border-bottom:1px solid var(--line); }
header h1 { font-size:16px; margin:0; letter-spacing:.02em; }
header a { color:var(--muted); text-decoration:none; }
main { padding:16px 24px; max-width:1200px; }
table { width:100%; border-collapse:collapse; background:var(--card);
        border:1px solid var(--line); }
th, td { text-align:left; padding:7px 10px; border-bottom:1px solid var(--line);
         vertical-align:top; }
th { color:var(--muted); font-weight:500; font-size:12px; text-transform:uppercase; }
td.mono, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:12px; }
a { color:var(--run); }
.chip { display:inline-block; padding:1px 8px; border-radius:10px; background:var(--chip);
        font-size:12px; }
.s-completed { color:var(--ok); } .s-failed, .s-parked { color:var(--bad); }
.s-cancelled, .s-continued { color:var(--muted); } .s-running, .s-suspended { color:var(--run); }
form.inline { display:inline-flex; gap:6px; align-items:center; margin:0 12px 8px 0; }
input, select, button { font:inherit; padding:5px 8px; border:1px solid var(--line);
        background:var(--card); color:var(--fg); border-radius:6px; }
button { cursor:pointer; } button.danger { color:var(--bad); }
.grid { display:grid; grid-template-columns: 160px 1fr; gap:4px 16px; margin-bottom:16px; }
.grid div:nth-child(odd) { color:var(--muted); }
pre { margin:0; white-space:pre-wrap; word-break:break-all; }
.msg { padding:8px 12px; background:var(--chip); border-radius:6px; margin-bottom:12px; }
"""


def _page(title: str, body: str) -> HTMLResponse:
    return HTMLResponse(f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title><style>{CSS}</style>
<script src="https://unpkg.com/htmx.org@1.9.12" defer></script></head>
<body><header><h1>Relay</h1><a href="/ui">runs</a><a href="/metrics">metrics</a></header>
<main>{body}</main></body></html>""")


def _esc(v) -> str:
    return html.escape(v if isinstance(v, str) else json.dumps(v, default=str))


def _ts(v) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        v = datetime.fromisoformat(v)
    return v.strftime("%m-%d %H:%M:%S.%f")[:-3]


def router(engine_of) -> APIRouter:
    r = APIRouter()

    def eng() -> Engine:
        return engine_of()

    @r.get("/ui", response_class=HTMLResponse)
    async def runs(status: str = "", workflow: str = "", limit: int = 100):
        where, args = [], []
        if status:
            args.append(status)
            where.append(f"status = ${len(args)}")
        if workflow:
            args.append(workflow)
            where.append(f"workflow = ${len(args)}")
        args.append(min(limit, 500))
        sql = (
            "SELECT run_id, workflow, version, status, next_seq - 1 AS events, created_at, "
            "updated_at FROM runs "
            + (f"WHERE {' AND '.join(where)} " if where else "")
            + f"ORDER BY updated_at DESC LIMIT ${len(args)}"
        )
        rows = await eng().pool.fetch(sql, *args)
        counts = {
            r["status"]: r["n"]
            for r in await eng().pool.fetch(
                "SELECT status, count(*) AS n FROM runs GROUP BY status"
            )
        }
        options = "".join(
            f'<option value="{s}"{" selected" if s == status else ""}>{s} '
            f"({counts.get(s, 0)})</option>"
            for s in STATUSES
        )
        trs = "".join(
            f"<tr><td class=mono><a href='/ui/runs/{r['run_id']}'>{r['run_id']}</a></td>"
            f"<td>{_esc(r['workflow'])} <span class=chip>v{r['version']}</span></td>"
            f"<td class='s-{r['status']}'>{r['status']}</td><td>{r['events']}</td>"
            f"<td class=mono>{_ts(r['created_at'])}</td>"
            f"<td class=mono>{_ts(r['updated_at'])}</td></tr>"
            for r in rows
        )
        body = f"""
<form class=inline method=get action=/ui>
  <select name=status><option value="">any status</option>{options}</select>
  <input name=workflow placeholder="workflow" value="{_esc(workflow) if workflow else ""}">
  <button>Filter</button></form>
<table><thead><tr><th>run</th><th>workflow</th><th>status</th><th>events</th><th>started</th>
<th>updated</th></tr></thead><tbody>{trs or "<tr><td colspan=6>no runs</td></tr>"}</tbody>
</table>"""
        return _page("Relay · runs", body)

    @r.get("/ui/runs/{run_id}", response_class=HTMLResponse)
    async def run_page(run_id: str, msg: str = ""):
        try:
            run = await eng().get_run(run_id)
        except NotFound:
            run = None
        if run is None:
            return _page("Relay · not found", "<p>No such run.</p>")
        events = await eng().get_events(run_id)
        t0 = datetime.fromisoformat(events[0]["at"]) if events else None
        rows = "".join(
            f"<tr><td>{e['seq']}</td><td>{e['type']}</td><td>{e['call_seq'] or ''}</td>"
            f"<td class=mono>+{(datetime.fromisoformat(e['at']) - t0).total_seconds():.3f}s"
            f"</td><td class=mono><pre>{_esc(e['payload'])[:600]}</pre></td></tr>"
            for e in events
        )
        open_run = run["status"] not in ("completed", "failed", "cancelled", "continued")
        actions = ""
        if open_run:
            actions += f"""
<form class=inline method=post action="/ui/runs/{run_id}/signal">
  <input name=name placeholder="signal name" required>
  <input name=data placeholder='data (JSON)' value="null"><button>Send signal</button></form>
<form class=inline method=post action="/ui/runs/{run_id}/cancel">
  <button class=danger>Cancel run</button></form>"""
        if run["status"] == "parked":
            actions += f"""<form class=inline method=post action="/ui/runs/{run_id}/resume">
  <button>Resume (after fixing the code)</button></form>"""
        body = f"""
{f"<div class=msg>{_esc(msg)}</div>" if msg else ""}
<div class=grid>
  <div>run</div><div class=mono>{run["run_id"]}</div>
  <div>workflow</div><div>{_esc(run["workflow"])} v{run["version"]}</div>
  <div>status</div><div class="s-{run["status"]}">{run["status"]}</div>
  <div>input</div><div class=mono>{_esc(run["input"])}</div>
  <div>result</div><div class=mono>{_esc(run["result"]) if run["result"] is not None else ""}
  </div>
  <div>error</div><div class=mono>{_esc(run["error"]) if run["error"] else ""}</div>
</div>
{actions}
<table><thead><tr><th>seq</th><th>event</th><th>call</th><th>at</th><th>payload</th></tr>
</thead><tbody>{rows}</tbody></table>"""
        return _page(f"Relay · {run_id[:8]}", body)

    async def _act(run_id: str, fn) -> RedirectResponse:
        try:
            await fn()
            msg = "done"
        except (NotFound, Conflict) as err:
            msg = f"refused: {err}"
        except ValueError as err:
            msg = f"bad input: {err}"
        return RedirectResponse(f"/ui/runs/{run_id}?msg={msg}", status_code=303)

    @r.post("/ui/runs/{run_id}/signal")
    async def ui_signal(run_id: str, name: str = Form(...), data: str = Form("null")):
        return await _act(run_id, lambda: eng().signal(run_id, name, json.loads(data)))

    @r.post("/ui/runs/{run_id}/cancel")
    async def ui_cancel(run_id: str):
        return await _act(run_id, lambda: eng().cancel(run_id))

    @r.post("/ui/runs/{run_id}/resume")
    async def ui_resume(run_id: str, request: Request):
        return await _act(run_id, lambda: eng().resume(run_id))

    return r
