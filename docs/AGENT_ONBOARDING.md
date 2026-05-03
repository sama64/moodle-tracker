# Moodle Tracker — Agent Onboarding

This is the fast path for a new agent to become useful in this repo without rediscovering the cursed parts.

## What this system is

`moodle-tracker` is a Moodle-backed academic monitoring system.

Core job:
1. collect Moodle data
2. normalize it into `NormalizedItem`s
3. store raw/downloaded artifacts separately
4. surface deadlines, risks, changes, and compact item/course views

Treat it as a **data pipeline first**, not an LLM product.

---

## Mental model

### Data flow

```text
Moodle APIs / calendar export / files
  -> SourceObject rows
  -> NormalizedItem rows
  -> Raw artifacts / extracted text artifacts
  -> risk/deadline/change views
  -> Telegram digests / downstream agent polling
```

### Important layers

- **Collectors**: `src/uni_tracker/collectors/moodle.py`
- **Persistence / normalization**: `src/uni_tracker/services/persistence.py`
- **Risk / deadline / change logic**: `src/uni_tracker/services/tools.py`
- **Completion logic**: `src/uni_tracker/services/completion.py`
- **Artifact storage/extraction**: `src/uni_tracker/services/storage.py`
- **API routes**: `src/uni_tracker/api/routes.py`
- **Schemas**: `src/uni_tracker/schemas.py`
- **DB models**: `src/uni_tracker/models.py`

---

## The endpoints that actually matter

### First checks

- `GET /health`
- `GET /health/details`

### Monitoring / agent use

- `GET /changes/since?since=<ISO timestamp>`
- `GET /deadlines/upcoming`
- `GET /risks`
- `GET /changes/recent`
- `GET /courses/{course_id}/snapshot`
- `GET /items/{item_id}/brief`
- `GET /items/{item_id}/provenance`
- `GET /items/{item_id}/content`

### Manual state correction

- `POST /items/{item_id}/done`
- `DELETE /items/{item_id}/done`

These matter because Moodle completion signals are not always reliable.

---

## What is a `NormalizedItem`?

A normalized academic thing that the agent cares about.

Common `item_type`s:
- `assignment`
- `quiz`
- `calendar_event`
- `material_file`
- `material`
- `grade_item`
- `announcement`

Important fields:
- `title`
- `starts_at`
- `due_at`
- `primary_url`
- `review_status`
- `review_reason`
- `completion_state`
- `source_completion_state`
- `completion_override_state`

### Completion fields

- `source_completion_state`: what Moodle seems to say
- `completion_override_state`: manual override
- `completion_state`: effective state after combining source + override

If Moodle lies or is incomplete, manual override is the fallback.

---

## Known weirdness / sharp edges

### 1. Moodle duplicates the same thing in multiple forms

One underlying assignment can appear as:
- an `assignment`
- a mirrored `calendar_event`
- a `grade_item`
- a no-due-date shadow row

Do **not** assume one title = one row.

### 2. Risks are partly deadline-driven, partly doc-driven

`/risks` is built in `services/tools.py`.

It includes:
- due items
- incomplete quizzes/assignments
- high-signal schedule documents (`review_reason == high_risk_schedule_document`)

It now also surfaces schedule docs containing upcoming exam/parcial dates.

### 3. Schedule documents matter a lot

Important exam dates often only exist inside extracted PDF/DOCX cronogramas.

Examples:
- `Cronograma CALCULO I 1er. Cuat. 2026.pdf`
- `CRONOGRAMA TERMODINAMICA 1C 2026 - Con detalle de practicas.pdf`
- `Cronograma de Ciencia de los Materiales. 1C-2026. Versión 00.docx`

These get extracted into `body_text` and can also be inspected via `/items/{item_id}/content`.

### 4. Artifact content was a major production pain point

The system now supports:
- `GET /items/{item_id}/content`
- downloaded file metadata
- extracted text artifacts

If you cannot inspect a file-backed item, check:
- whether the artifact was actually downloaded
- whether extracted text exists
- whether the worker has persistent artifact storage mounted
- whether `ARTIFACT_STORAGE_BACKEND=s3` is enabled; if so, inspect `raw_artifacts.storage_bucket/storage_key` and use the storage abstraction rather than assuming a local file exists

### 5. Moodle completion is unreliable enough that overrides exist

Automatic completion handling was added, but for some quizzes/assignments the Moodle signal still failed.

That is why manual done endpoints exist.

If a user says “I finished X”, it is valid to use:
- `POST /items/{item_id}/done`

### 6. Local deploy state matters more than GitHub state

For this repo, **the VPS local working tree is effectively deploy state**.
GitHub is useful, but local changes + docker restart are what make behavior real.

Do not assume “PR exists” means the tracker is live.

Always verify locally.

---

## Operational workflow for changes

If you change tracker code, default workflow should be:

```bash
git status
# make change
python3 -m compileall -q src scripts
# run targeted tests if possible
# then rebuild/restart locally
docker compose build api worker
docker compose up -d api worker
# then verify live endpoints
```

Do **not** stop at “patch written”.
Live verification matters.

---

## How to validate something quickly

### Check current risks

```bash
python3 - <<'PY'
import json, urllib.request
for path in ['http://localhost:8000/risks','http://localhost:8000/deadlines/upcoming']:
    with urllib.request.urlopen(path, timeout=20) as r:
        print('===', path)
        print(json.dumps(json.load(r), ensure_ascii=False)[:12000])
PY
```

### Inspect one item deeply

```bash
python3 - <<'PY'
import json, urllib.request
item_id = 395
for path in [
    f'http://localhost:8000/items/{item_id}',
    f'http://localhost:8000/items/{item_id}/brief',
    f'http://localhost:8000/items/{item_id}/provenance',
    f'http://localhost:8000/items/{item_id}/content',
]:
    with urllib.request.urlopen(path, timeout=20) as r:
        print('===', path)
        print(r.read().decode()[:12000])
PY
```

### Mark something done manually

```bash
python3 - <<'PY'
import urllib.request
item_id = 87
req = urllib.request.Request(f'http://localhost:8000/items/{item_id}/done', method='POST')
with urllib.request.urlopen(req, timeout=20) as r:
    print(r.read().decode())
PY
```

Undo:

```bash
python3 - <<'PY'
import urllib.request
item_id = 87
req = urllib.request.Request(f'http://localhost:8000/items/{item_id}/done', method='DELETE')
with urllib.request.urlopen(req, timeout=20) as r:
    print(r.read().decode())
PY
```

---

## Things another agent should know fast

### Current proven capabilities

The repo now supports:
- semantic change metadata on `/changes/since`
- content/artifact retrieval through `/items/{item_id}/content`
- token caching for Moodle auth reuse
- manual completion overrides through `/items/{item_id}/done`
- schedule-document exam surfacing in `/risks`

### Things still slightly cursed

- Moodle emits duplicate mirrored entities
- weekday labeling/user-facing summaries can still be wrong if you don’t re-check local timezone carefully
- some old/stale schedule docs remain in the DB and need human judgment
- automatic Moodle completion is not fully trustworthy in all cases

---

## If you only read one thing

When debugging:
1. inspect `/courses/{course_id}/snapshot`
2. inspect `/items/{item_id}/content`
3. inspect `services/tools.py`
4. assume duplicate Moodle rows until proven otherwise
5. restart Docker and verify live before claiming success

That will save you hours.
