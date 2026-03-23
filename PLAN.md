# Uni Tracker Implementation Plan

## Summary
Build this in narrow stages so each stage produces a usable system before adding more complexity. The sequence is: prove ingestion, prove change detection, prove date-risk detection, then add assistive LLM tooling. Moodle web services stay primary; browser scraping remains a fallback only if a concrete gap appears.

## Stages
### Stage 0: Foundation
- Create the runtime skeleton: `api`, `worker`, `postgres`, shared artifact storage, env/config, migrations, logging.
- Define the core schemas only: `source_accounts`, `courses`, `source_objects`, `raw_artifacts`, `normalized_items`, `collector_runs`, `notifications`, `acknowledgements`.
- Implement a collector framework with checkpoints, retries, hashing, and run stats.
- Add FastAPI health/admin endpoints and a manual `run collector` endpoint.
- Acceptance: the stack boots locally and on the VPS, migrations apply cleanly, and collector runs are recorded.

### Stage 1: Moodle Inventory Sync
- Implement token auth and collectors for enrolled courses and course contents.
- Persist raw JSON responses and downloaded Moodle-linked files.
- Normalize only these item types at first: `material`, `forum`, `assignment`, `quiz`, `grade_item`.
- Do not alert yet; only ingest and expose data through API endpoints.
- Acceptance: a sync creates a stable inventory of your courses, modules, files, forums, assignments, and grade items with no duplicate rows on rerun.

### Stage 2: Change Detection Backbone
- Add content hashing and version tracking for source objects and normalized items.
- Introduce `item_versions` or equivalent change-history storage for important fields.
- Implement `core_course_get_updates_since` collector as the incremental sync path.
- Classify changes into `created`, `updated`, `removed`, `date_changed`, `schedule_changed`, `content_changed`.
- Acceptance: rerunning after a Moodle change produces a precise diff and stores old/new values with provenance.

### Stage 3: Calendar and Date Surfaces
- Add calendar export token handling and ICS ingestion.
- Parse assignments/quizzes/forum text for explicit dates only with deterministic extraction first.
- Extract PDF text from downloaded files and store it as a raw artifact derivative.
- Add structured facts for `due_at`, `exam_at`, `class_session_at`, `classroom`, `schedule_range`.
- Acceptance: upcoming dates are visible through the API from assignments, calendar events, and clear PDF/forum text.

### Stage 4: Silent Change Protection
- Add special monitoring rules for high-risk artifacts: schedule PDFs, syllabus PDFs, assignment pages, news forums, quizzes.
- On artifact binary change, re-extract text and re-derive facts.
- Diff structured facts, not just raw text, so the system can detect silent deadline or schedule edits.
- Emit explicit change events: `deadline changed`, `deadline removed`, `schedule changed`, `new exam date`.
- Acceptance: if a professor edits a date or schedule source without posting an announcement, the system detects it and records the before/after diff.

### Stage 5: Notifications
- Add Telegram delivery.
- Implement immediate alerts for urgent items and a daily digest for everything else.
- Urgent alerts cover new deadlines within 72h, changed deadlines, changed class sessions within 14 days, newly published assignments/quizzes with due dates, and high-signal announcements.
- Require acknowledgement for urgent items and re-notify on a capped reminder schedule.
- Acceptance: you receive one immediate alert per urgent event, no duplicate spam on unchanged reruns, and daily digest grouping works by course.

### Stage 6: LLM Assist Layer
- Add LLM jobs only after normalized items and deterministic facts exist.
- Use the model for summarization, ambiguous date extraction from long text/PDFs, and change summaries.
- Store all outputs with confidence and provenance; low-confidence results cannot trigger urgent alerts alone.
- Add the internal tool surface: `get_recent_changes`, `get_upcoming_deadlines`, `get_risk_items`, `get_course_snapshot`, `get_item_provenance`, `acknowledge_item`.
- Acceptance: the assistant layer can answer state questions from structured data and source-linked facts without becoming the source of truth.

### Stage 7: Hardening
- Add retry/backoff, token refresh handling, collector health metrics, and stale-source alerts.
- Add manual review flags for low-confidence schedule/date changes.
- Add backup/restore for Postgres and raw artifacts.
- Define the criteria that justify implementing a browser fallback collector.
- Acceptance: the system survives transient Moodle/API failures and makes gaps visible instead of silently dropping data.

## Key Implementation Decisions
- Primary source: Moodle token/web-service API.
- Secondary source: calendar export ICS.
- Fallback source: browser/UI collection only for verified blind spots.
- DB: Postgres from day one.
- User surface: Telegram first, API/tool layer in parallel, no chat UI in v1.
- Raw retention: keep all raw JSON, HTML, files, and extracted PDF text indefinitely.
- Diffing model: version both artifacts and structured facts so silent date changes are detectable.

## Test Plan by Stage
- Stage 1: full sync is idempotent.
- Stage 2: edited Moodle objects produce correct change classes and old/new values.
- Stage 3: dates from assignments, ICS, and clear document text normalize correctly into the user timezone.
- Stage 4: modified schedule PDF or changed due date triggers a structural fact diff.
- Stage 5: urgent alert dedup, digest grouping, acknowledgement, and reminder behavior all hold.
- Stage 6: LLM summaries and ambiguous-date facts are stored with provenance and never bypass deterministic rules.

## Assumptions
- Single-user system.
- Moodle remains the only source in v1.
- OCR is deferred; scanned PDFs are flagged, not fully processed.
- Each stage should end in a shippable checkpoint before moving to the next.
