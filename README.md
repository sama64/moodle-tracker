# Uni Tracker

Personal academic ops system for Moodle-based university classes.

It collects Moodle data, normalizes it into structured items, detects changes and deadlines, and exposes both human-facing notifications and agent-facing briefs.

If you are an automated agent, read [AGENTS.md](AGENTS.md) first.

## What it does

- Syncs enrolled courses, course contents, assignments, quizzes, forums, grade metadata, and calendar exports from Moodle.
- Stores raw source artifacts and normalized items separately.
- Detects deadline changes, schedule changes, and new announcements.
- Sends Telegram digests and urgent alerts.
- Exposes compact item/course briefs for downstream agents.
- Keeps provenance available through item facts, notifications, and LLM job history.

## Quick Start

1. Create a `.env` file with the required settings.
2. Start the stack:

```bash
docker compose up -d --build
```

3. Visit the API:

```text
http://localhost:8000/health
```

## Configuration

Required:

- `MOODLE_BASE_URL`
- `MOODLE_USERNAME`
- `MOODLE_PASSWORD`

Common optional settings:

- `APP_ENV`
- `LOG_LEVEL`
- `DATABASE_URL`
- `RAW_STORAGE_PATH`
- `DAILY_DIGEST_HOUR`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `ENABLE_LLM`
- `NVIDIA_API_KEY`
- `NVIDIA_API_URL`
- `NVIDIA_MODEL`

Time handling:

- User-facing times are normalized to `America/Argentina/Buenos_Aires`.
- Telegram renders compact human-readable dates.

## Runtime

Docker Compose starts:

- `db`: PostgreSQL
- `api`: FastAPI server
- `worker`: collector, digest, Telegram polling, and enrichment loop

The worker runs the scheduled Moodle syncs and notification dispatches. The API serves read-only views and manual sync endpoints.

## API

Useful endpoints:

- `GET /health`
- `GET /health/details`
- `GET /courses`
- `GET /items`
- `GET /items/{item_id}`
- `GET /items/{item_id}/brief`
- `GET /items/{item_id}/provenance`
- `GET /courses/{course_id}/snapshot`
- `GET /courses/{course_id}/brief`
- `GET /changes/recent`
- `GET /deadlines/upcoming`
- `GET /risks`
- `GET /sync/collectors`
- `POST /sync/run/{collector_name}`
- `POST /items/{item_id}/acknowledge`
- `POST /notifications/dispatch`
- `GET /notifications/digest`

## Telegram Commands

If Telegram is configured, the bot can answer:

- `/digest` or `/digest 48`
- `/risks`
- `/deadlines`
- `/changes`
- `/help`

## LLM Enrichment

The LLM layer is optional.

When enabled, it compresses ambiguous or high-value Moodle content into agent-friendly briefs. It does not replace the deterministic collectors or become the source of truth.

Relevant data layers:

- `llm_jobs`: audit trail for model requests and responses
- `item_facts`: granular extracted evidence
- `item_briefs`: compact agent-facing projection

If a generated brief is weak, the system can backfill it with deterministic fallback logic instead of keeping a useless model echo.

## Backfilling Briefs

To refresh weak briefs manually:

```bash
docker compose exec worker python /app/scripts/backfill_briefs.py --weak-only
```

To target one item:

```bash
docker compose exec worker python /app/scripts/backfill_briefs.py --item-id 380
```

## Development

Run tests:

```bash
.venv/bin/pytest -q
```

Compile check:

```bash
python -m compileall -q src scripts
```

## Notes

- Moodle is the primary source of truth.
- Raw source artifacts are retained under `data/uni-tracker/artifacts/runtime/` for provenance and replay.
- Postgres persists in the named Docker volume `uni_tracker_postgres_data`.
- Human-facing alerts stay conservative and source-linked.
- Agent-facing consumers should prefer briefs and provenance over raw Moodle pages whenever possible.
