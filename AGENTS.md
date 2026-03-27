# Agent Instructions

This repository is a Moodle-backed academic tracking system. If you are an agent consuming its outputs, treat it as a reliability pipeline first and an LLM system second.

## Source Of Truth

Use this hierarchy:

1. Moodle source data
2. Normalized database rows
3. `item_briefs` for compact agent-facing context
4. `item_facts` and `llm_jobs` for provenance and audit

Do not treat a brief as stronger than its source. A brief is a compression of evidence, not evidence itself.

## What To Query First

Prefer these endpoints in this order:

- `GET /health`
- `GET /changes/since?since=<ISO-8601 timestamp>`
- `GET /deadlines/upcoming`
- `GET /risks`
- `GET /changes/recent`
- `GET /courses/{course_id}/brief`
- `GET /items/{item_id}/brief`
- `GET /items/{item_id}/provenance`

Use the course brief when you need a course-level summary. Use the item brief when you need a single assignment, quiz, announcement, or document. Use provenance when you need to justify a claim.

For continuous monitoring, use `/changes/since` as the primary delta feed. Keep a cursor based on the highest returned `updated_at`. Use `/risks` and `/deadlines/upcoming` as periodic reconciliation endpoints, not as your main event stream.

## Interpretation Rules

- Prefer structured fields over raw body text.
- Prefer briefs over full PDFs or long forum posts when answering another agent.
- Use provenance when you need to explain why an item matters.
- Do not invent deadlines or schedule changes that are not explicitly supported.
- Treat `review_status == watch` and `review_reason == high_risk_schedule_document` as a strong signal that the item may affect dates or obligations.
- Treat `review_reason == text_extraction_failed` as a reason to inspect the source artifact or provenance, not as a reason to make up a summary.

## What Matters Most

Alert on:

- deadline additions, removals, or changes
- class time or location changes
- quizzes or assignments posted close to the due date
- announcements that mention `parcial`, `recuperatorio`, `examen`, `entrega`, `vence`, `suspendida`, or `cambio de aula`
- schedule PDFs and syllabus PDFs that change without notice

If an item changed but you cannot determine the exact consequence, say that explicitly and include the source link.

## Time And Formatting

- Assume `America/Argentina/Buenos_Aires` for user-facing output.
- Render dates in local time unless the user explicitly asks for UTC.
- For Telegram-style human output, keep lines short:
  - date first
  - course second
  - title third
  - source URL last

## Reading Briefs

A good brief should answer:

- What is this?
- Which course does it belong to?
- Is it a deadline, schedule change, or new material?
- What should the student do next?
- Is there a date attached?

If a brief only repeats the title, treat it as weak. The project has deterministic backfill logic for weak briefs, so a title echo is not acceptable as useful agent context.

## Reading Provenance

Use provenance when you need to verify or explain:

- why an item was marked risky
- which extracted facts exist
- which notifications were generated
- what the model produced for a given enrichment job

`llm_jobs` is the audit trail for model calls. `item_facts` are the extracted facts. `item_briefs` are the compact outputs to hand to another agent.

## Operational Constraints

- Do not assume the LLM is always enabled.
- Do not assume the LLM output is better than deterministic parsing.
- Do not rely on the Telegram digest as a complete view of the database.
- If a course or item is missing a brief, fall back to `GET /items/{item_id}/provenance` and the raw item endpoint.

## Practical Alerting Policy

When generating alerts for the user:

- prioritize due dates within the next 72 hours
- prioritize class changes within the next 14 days
- keep course names visible
- do not include low-signal content like `index.html`
- collapse repeated course names when showing multiple items from the same course
- deduplicate mirrored calendar entries and duplicate titles

## Good Default Behavior

When in doubt:

- summarize briefly
- cite the source URL
- mention the course
- mention the due date if present
- do not over-explain

The right answer here is usually a small, source-linked summary rather than a large dump of raw Moodle content.
