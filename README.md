# Escalation SLA Tracker

A localhost triage service for many Reflex agents waiting on humans. It persists sessions in SQLite and serves a responsive queue that sorts using `waiting seconds × risk weight` (high ×4, medium ×2, low ×1).

## Run locally

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/uvicorn app:app --reload --port 8123
```

Open `http://127.0.0.1:8123`. Without a configured Reflex key, the app seeds five demo sessions so the triage-and-answer loop is usable immediately. Once a key is configured, it starts empty and relies entirely on real synced agents instead.

## Connect Reflex

In `.env`, set `REFLEX_API_KEY` and leave `REFLEX_ORGANIZATION_ID=surya-workspace`; Reflex accepts an organization ID **or slug**, so the supplied workspace URL already provides a usable value. The base URL is the origin only: `https://reflex.runloop.ai`, not the `/orgs/.../runs` UI path.

A background loop polls Reflex every 5 seconds and imports anything that needs a human — no button click required (the **Sync needs-input sessions** button just triggers it on demand). It treats an agent as needing input when `status == needs_input`, **or** when `status == running` but `turnState == idle` — the polled `status` field is documented to go stale (a devbox can go idle after its turn ends while `status` stays stuck on `running`), and `turnState` doesn't share that lag. Answering a synced item calls `POST /api/agents/{id}/message` — **not** `/control-response`, which only answers a formal control-request handshake and is a no-op for an agent that's simply idle after a normal turn; `/message` is what actually wakes a suspended devbox and resumes it.

The available API endpoints are:

- `GET /api/queue` — waiting sessions, priority-sorted
- `GET /api/sessions` — all local session records
- `POST /api/sessions/{id}/answer` — dispatch answer
- `POST /api/demo/sessions` — create a local simulated waiting agent
- `POST /api/integrations/reflex/sync` — import active Reflex needs-input agents

## Security and next increment

Never commit `.env` or paste keys into source. Replace the exposed keys before using the live adapter. To enable the Slack option, set an incoming `SLACK_WEBHOOK_URL`; a checked answer sends a small resolution notification. Direct answer delivery remains the primary workflow.

For continuous live ingestion, add a small Node sidecar using `@runloop/reflex-client`'s `ReflexSocket`, subscribe once per imported `streamId`, and derive status with the SDK's own `agent-liveness` reducer instead of this app's simpler `turnState` check. Polling the documented agent list is a safe localhost starting point.
