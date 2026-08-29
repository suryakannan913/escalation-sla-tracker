# Escalation SLA Tracker

A localhost triage service for many Reflex agents waiting on humans. It persists sessions in SQLite and serves a responsive queue that sorts using `waiting seconds × risk weight` (high ×4, medium ×2, low ×1).

## Run locally

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/uvicorn app:app --reload --port 8000
```

Open `http://127.0.0.1:8000`. The app seeds five related demo sessions, so the complete triage-and-answer loop is usable immediately.

## Connect Reflex

In `.env`, set `REFLEX_API_KEY` and leave `REFLEX_ORGANIZATION_ID=surya-workspace`; Reflex accepts an organization ID **or slug**, so the supplied workspace URL already provides a usable value. The base URL is the origin only: `https://reflex.runloop.ai`, not the `/orgs/.../runs` UI path.

Click **Sync needs-input sessions**. The service calls documented `GET /api/agents?status=needs_input` with `x-organization-id`, and maps those records into its local queue. Answering a synced item calls documented `POST /api/agents/{id}/control-response` with the response payload.

The available API endpoints are:

- `GET /api/queue` — waiting sessions, priority-sorted
- `GET /api/sessions` — all local session records
- `POST /api/sessions/{id}/answer` — dispatch answer
- `POST /api/demo/sessions` — create a local simulated waiting agent
- `POST /api/integrations/reflex/sync` — import active Reflex needs-input agents

## Security and next increment

Never commit `.env` or paste keys into source. Replace the exposed keys before using the live adapter. To enable the Slack option, set an incoming `SLACK_WEBHOOK_URL`; a checked answer sends a small resolution notification. Direct answer delivery remains the primary workflow.

For continuous live ingestion, add a small Node sidecar using `@runloop/reflex-client`'s `ReflexSocket`, subscribe once per imported `streamId`, and normalize `agent.need_input` / `control_response` events into the SQLite tables. Polling the documented agent list is a safe localhost starting point.
