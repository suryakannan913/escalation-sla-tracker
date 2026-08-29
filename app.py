"""Local Escalation SLA Tracker API.

Starts with a persistent, realistic demo queue. When REFLEX_API_KEY is set in
the local .env file, POST /api/integrations/reflex/sync reads live needs_input
agents from Reflex. Human answers are dispatched to Reflex control-response.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).parent
load_dotenv(ROOT / ".env")
DB_PATH = ROOT / "tracker.db"
Risk = Literal["low", "medium", "high"]
Status = Literal["running", "waiting", "answered", "done", "error"]
SLA_MINUTES = {"high": 15, "medium": 30, "low": 120}
RISK_WEIGHT = {"high": 4, "medium": 2, "low": 1}


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or now()).isoformat()


@contextmanager
def db():
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def initialize() -> None:
    with db() as connection:
        connection.execute("""CREATE TABLE IF NOT EXISTS sessions (
          id TEXT PRIMARY KEY, source TEXT NOT NULL, devbox_id TEXT, agent_name TEXT NOT NULL,
          task_description TEXT NOT NULL, status TEXT NOT NULL, risk_level TEXT NOT NULL,
          started_at TEXT NOT NULL, waiting_since TEXT, last_question TEXT, log_tail TEXT NOT NULL,
          owner TEXT NOT NULL, reflex_agent_id TEXT UNIQUE, updated_at TEXT NOT NULL
        )""")
        connection.execute("""CREATE TABLE IF NOT EXISTS events (
          id TEXT PRIMARY KEY, session_id TEXT NOT NULL, event_type TEXT NOT NULL,
          payload TEXT NOT NULL, timestamp TEXT NOT NULL
        )""")
        count = connection.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        if count == 0:
            seed_demo(connection)


def seed_demo(connection: sqlite3.Connection) -> None:
    rows = [
        ("Payments incident agent", "Investigate elevated payment retry failures in production.", "high", 23, "Should I disable card retries for EU traffic?", "SRE on-call", "Retry volume increased after gateway timeout spikes."),
        ("Migration agent", "Prepare customer data migration for the Q3 schema release.", "high", 21, "Which customer cohort can be migrated first?", "Data Platform", "Migration completed validation for 14 of 18 tenant cohorts."),
        ("Security review agent", "Investigate an authorization anomaly in audit logs.", "high", 18, "Approve temporary read access to audit logs?", "Security", "The agent needs read-only audit-log access to correlate the alert."),
        ("Release agent", "Validate canary metrics and prepare a staged product rollout.", "medium", 36, "Proceed with the staged rollout to 25%?", "Release manager", "Error rate and latency remain inside the canary threshold."),
        ("Docs agent", "Update the API migration guide for the next release.", "low", 74, "Which API version should the migration guide recommend?", "Developer Experience", "No customer impact; the release-note draft is ready."),
    ]
    for index, (agent, task, risk, minutes, question, owner, log) in enumerate(rows, 1):
        session_id = f"demo-{index:03d}"
        waiting = now() - timedelta(minutes=minutes)
        connection.execute("INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            session_id, "demo", f"demo-devbox-{index}", agent, task, "waiting", risk,
            iso(waiting - timedelta(minutes=8)), iso(waiting), question, log, owner, None, iso(),
        ))
        write_event(connection, session_id, "question_asked", {"question": question}, iso(waiting))


def write_event(connection: sqlite3.Connection, session_id: str, event_type: str, payload: dict, timestamp: str | None = None) -> None:
    connection.execute("INSERT INTO events VALUES (?,?,?,?,?)", (str(uuid.uuid4()), session_id, event_type, json.dumps(payload), timestamp or iso()))


def classify_risk(task: str) -> Risk:
    text = task.lower()
    if any(word in text for word in ("auth", "payment", "delete", "migration", "prod", "credential", "security")):
        return "high"
    if any(word in text for word in ("release", "deploy", "database", "customer")):
        return "medium"
    return "low"


def present(row: sqlite3.Row) -> dict:
    result = dict(row)
    waiting = result["waiting_since"]
    duration = 0
    if waiting and result["status"] == "waiting":
        duration = max(0, int((now() - datetime.fromisoformat(waiting)).total_seconds()))
    risk = result["risk_level"]
    score = duration * RISK_WEIGHT[risk] if result["status"] == "waiting" else 0
    result.update({
        "wait_duration_seconds": duration,
        "sla_minutes": SLA_MINUTES[risk],
        "priority_score": score,
        "sla_state": "breached" if duration >= SLA_MINUTES[risk] * 60 else "near" if duration >= SLA_MINUTES[risk] * 45 else "on_track",
    })
    return result


def queue() -> list[dict]:
    with db() as connection:
        rows = connection.execute("SELECT * FROM sessions WHERE status='waiting'").fetchall()
    return sorted((present(row) for row in rows), key=lambda item: item["priority_score"], reverse=True)


def analytics() -> dict:
    with db() as connection:
        waiting = [present(row) for row in connection.execute("SELECT * FROM sessions WHERE status='waiting'").fetchall()]
        events = connection.execute("SELECT * FROM events ORDER BY timestamp").fetchall()

    risk_counts = {"high": 0, "medium": 0, "low": 0}
    sla_counts = {"on_track": 0, "near": 0, "breached": 0}
    owner_totals: dict[str, dict] = {}
    for item in waiting:
        risk_counts[item["risk_level"]] += 1
        sla_counts[item["sla_state"]] += 1
        bucket = owner_totals.setdefault(item["owner"], {"owner": item["owner"], "total_wait_seconds": 0, "count": 0})
        bucket["total_wait_seconds"] += item["wait_duration_seconds"]
        bucket["count"] += 1
    wait_by_owner = sorted(owner_totals.values(), key=lambda x: x["total_wait_seconds"], reverse=True)[:8]

    current = now()
    hourly: dict[str, int] = {(current - timedelta(hours=i)).strftime("%Y-%m-%dT%H:00"): 0 for i in range(23, -1, -1)}
    asked_at: dict[str, datetime] = {}
    resolution_seconds: list[float] = []
    answered_today = 0
    today_key = current.strftime("%Y-%m-%d")
    for event in events:
        timestamp = datetime.fromisoformat(event["timestamp"])
        if event["event_type"] == "question_asked":
            asked_at[event["session_id"]] = timestamp
        elif event["event_type"] == "question_answered":
            bucket_key = timestamp.strftime("%Y-%m-%dT%H:00")
            if bucket_key in hourly:
                hourly[bucket_key] += 1
            if timestamp.strftime("%Y-%m-%d") == today_key:
                answered_today += 1
            asked = asked_at.get(event["session_id"])
            if asked:
                resolution_seconds.append((timestamp - asked).total_seconds())

    return {
        "risk_counts": risk_counts,
        "sla_counts": sla_counts,
        "wait_by_owner": wait_by_owner,
        "activity_by_hour": [{"hour": hour, "answered": count} for hour, count in hourly.items()],
        "answered_today": answered_today,
        "avg_resolution_seconds": (sum(resolution_seconds) / len(resolution_seconds)) if resolution_seconds else None,
    }


class AnswerRequest(BaseModel):
    answer: str = Field(min_length=1, max_length=8000)
    notify_slack: bool = False


class DemoSessionRequest(BaseModel):
    task_description: str = Field(min_length=4, max_length=1000)
    question: str = Field(min_length=4, max_length=1000)
    risk_level: Risk | None = None
    owner: str = "Unassigned"
    agent_name: str = "Workspace agent"


class ReflexClient:
    """Thin adapter over documented Reflex endpoints; secrets stay in environment."""
    @property
    def configured(self) -> bool:
        return bool(os.getenv("REFLEX_API_KEY") and os.getenv("REFLEX_ORGANIZATION_ID"))

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {os.environ['REFLEX_API_KEY']}", "x-organization-id": os.environ["REFLEX_ORGANIZATION_ID"]}

    def base_url(self) -> str:
        return os.getenv("REFLEX_BASE_URL", "https://reflex.runloop.ai").rstrip("/")

    async def waiting_agents(self) -> list[dict]:
        if not self.configured:
            raise HTTPException(400, "Reflex is not configured. Set REFLEX_API_KEY and REFLEX_ORGANIZATION_ID in .env.")
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(f"{self.base_url()}/api/agents", headers=self.headers(), params={"status": "needs_input", "limit": 200})
            response.raise_for_status()
            return response.json().get("agents", [])

    async def answer(self, agent_id: str, answer: str) -> None:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(f"{self.base_url()}/api/agents/{agent_id}/control-response", headers=self.headers(), json={"payload": answer})
            if response.status_code == 409:
                raise HTTPException(409, "This agent no longer has a pending input request.")
            response.raise_for_status()


class SlackClient:
    """Optional, explicit notification adapter for an incoming Slack webhook."""
    @property
    def configured(self) -> bool:
        return bool(os.getenv("SLACK_WEBHOOK_URL"))

    async def notify_answer(self, session: sqlite3.Row, answer: str) -> None:
        if not self.configured:
            return
        message = {
            "text": f"Escalation answered: {session['agent_name']} ({session['id']})\n"
                    f"Risk: {session['risk_level'].upper()}\nAnswer: {answer}"
        }
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(os.environ["SLACK_WEBHOOK_URL"], json=message)
            response.raise_for_status()


reflex = ReflexClient()
slack = SlackClient()
app = FastAPI(title="Escalation SLA Tracker")
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")


@app.on_event("startup")
def startup() -> None:
    initialize()
    app.state.reflex_poll_task = asyncio.create_task(reflex_poll_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    app.state.reflex_poll_task.cancel()
    try:
        await app.state.reflex_poll_task
    except asyncio.CancelledError:
        pass


async def reflex_poll_loop() -> None:
    """Localhost-safe fallback when no event-stream worker is running."""
    while True:
        if reflex.configured:
            try:
                await import_reflex_waiting()
            except (httpx.HTTPError, HTTPException):
                # The dashboard remains usable while a remote service is unavailable.
                pass
        await asyncio.sleep(5)


@app.get("/")
def index():
    return FileResponse(ROOT / "static" / "index.html")


@app.get("/api/health")
def health():
    return {"ok": True, "reflex_configured": reflex.configured, "organization": os.getenv("REFLEX_ORGANIZATION_ID")}


@app.get("/api/queue")
def get_queue():
    return queue()


@app.get("/api/analytics")
def get_analytics():
    return analytics()


@app.get("/api/sessions")
def get_sessions():
    with db() as connection:
        rows = connection.execute("SELECT * FROM sessions ORDER BY updated_at DESC").fetchall()
    return [present(row) for row in rows]


@app.post("/api/demo/sessions", status_code=201)
def create_demo_session(body: DemoSessionRequest):
    session_id = f"demo-{uuid.uuid4().hex[:8]}"
    risk = body.risk_level or classify_risk(body.task_description)
    with db() as connection:
        connection.execute("INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            session_id, "demo", f"demo-devbox-{session_id}", body.agent_name, body.task_description, "waiting", risk,
            iso(), iso(), body.question, "Demo session created from the dashboard.", body.owner, None, iso(),
        ))
        write_event(connection, session_id, "question_asked", {"question": body.question})
    return {"id": session_id}


async def import_reflex_waiting() -> int:
    agents = await reflex.waiting_agents()
    synced = 0
    with db() as connection:
        for agent in agents:
            agent_id = agent["id"]
            existing = connection.execute("SELECT id FROM sessions WHERE reflex_agent_id=?", (agent_id,)).fetchone()
            task = agent.get("task") or agent.get("initialPrompt") or agent.get("title") or "Reflex agent requires input"
            values = (agent.get("devboxId"), agent.get("name") or agent.get("agentType") or "Reflex agent", task, classify_risk(task), iso())
            if existing:
                connection.execute("UPDATE sessions SET status='waiting', devbox_id=?, agent_name=?, task_description=?, risk_level=?, updated_at=? WHERE id=?", (*values, existing["id"]))
            else:
                session_id = f"reflex-{agent_id}"
                connection.execute("INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                    session_id, "reflex", values[0], values[1], values[2], "waiting", values[3], iso(), iso(),
                    "Agent is waiting for an input control response.", "Synced from the Reflex needs_input state.", "Unassigned", agent_id, values[4],
                ))
                write_event(connection, session_id, "question_asked", {"source": "reflex", "agent_id": agent_id})
            synced += 1
    return synced


@app.post("/api/integrations/reflex/sync")
async def sync_reflex():
    synced = await import_reflex_waiting()
    return {"ok": True, "synced": synced}


@app.post("/api/sessions/{session_id}/answer")
async def answer_session(session_id: str, body: AnswerRequest):
    with db() as connection:
        row = connection.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Session not found")
        if row["status"] != "waiting":
            raise HTTPException(409, "Session is not currently waiting")
        if row["source"] == "reflex":
            await reflex.answer(row["reflex_agent_id"], body.answer)
        connection.execute("UPDATE sessions SET status='answered', waiting_since=NULL, updated_at=? WHERE id=?", (iso(), session_id))
        write_event(connection, session_id, "question_answered", {"answer": body.answer, "slack_requested": body.notify_slack})
    if body.notify_slack and slack.configured:
        await slack.notify_answer(row, body.answer)
    return {"ok": True, "delivery": "reflex" if row["source"] == "reflex" else "dashboard_demo", "slack_notified": body.notify_slack and slack.configured}
