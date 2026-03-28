from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select

from uni_tracker.collectors.base import BaseCollector
from uni_tracker.models import Course, ItemFact, NormalizedItem, SourceObject
from uni_tracker.services.calendar import parse_ics_events
from uni_tracker.services.moodle import MoodleServiceClient, epoch_to_datetime
from uni_tracker.services.notifications import schedule_notifications_for_item
from uni_tracker.services.parsing import (
    ExtractedFact,
    derive_review_status,
    extract_date_facts_from_text,
    extract_text_for_file,
    safe_filename,
    strip_html,
)
from uni_tracker.services.persistence import (
    create_raw_artifact,
    mark_removed_source_objects,
    replace_item_facts,
    upsert_normalized_item,
    upsert_source_object,
)
from uni_tracker.services.tools import resolve_course_from_categories


MODULE_TO_ITEM_TYPE = {
    "assign": "assignment",
    "quiz": "quiz",
    "forum": "forum",
    "resource": "material",
    "folder": "material",
    "page": "material",
    "url": "material",
    "label": "material",
}


def module_item_type(modname: str) -> str:
    return MODULE_TO_ITEM_TYPE.get(modname, modname)


def extract_module_date_facts(module: dict[str, Any]) -> tuple[datetime | None, datetime | None, list[ExtractedFact]]:
    due_at = None
    starts_at = None
    facts: list[ExtractedFact] = []
    for item in module.get("dates", []) or []:
        label = str(item.get("label", ""))
        timestamp = item.get("timestamp")
        if timestamp is None:
            continue
        value = epoch_to_datetime(int(timestamp))
        if value is None:
            continue
        fact_type = "starts_at"
        if any(word in label.lower() for word in ["due", "vence", "entrega", "cierra", "close"]):
            due_at = value
            fact_type = "due_at"
        elif starts_at is None:
            starts_at = value
        facts.append(
            ExtractedFact(
                fact_type=fact_type,
                value={"value": value.isoformat(), "label": label},
                confidence=1.0,
                extractor_type="module_dates",
                source_span=label,
            )
        )
    return due_at, starts_at, facts


def forum_item_type(forum_type: str) -> str:
    return "announcement" if forum_type == "news" else "forum_discussion"


class MoodleCourseCatalogCollector(BaseCollector):
    name = "moodle_courses"

    def collect(self, run) -> dict[str, Any]:
        client = MoodleServiceClient(self.context.settings, session=self.context.session, source_account=self.context.source_account)
        try:
            courses = client.get_courses()
        finally:
            client.close()

        relative_path, content_hash, size_bytes = self.context.artifact_store.write_json(
            "moodle/course_catalog",
            "courses",
            courses,
        )
        create_raw_artifact(
            self.context.session,
            collector_run_id=run.id,
            source_object_id=None,
            artifact_type="json",
            mime_type="application/json",
            storage_path=relative_path,
            content_hash=content_hash,
            size_bytes=size_bytes,
            source_url=None,
            metadata_json={"collector": self.name, "item_count": len(courses)},
        )

        processed = 0
        for payload in courses:
            now = datetime.now(UTC)
            external_id = str(payload["id"])
            course = self.context.session.scalar(
                select(Course).where(
                    Course.source_account_id == self.context.source_account.id,
                    Course.external_id == external_id,
                )
            )
            if course is None:
                course = Course(
                    source_account_id=self.context.source_account.id,
                    external_id=external_id,
                    shortname=payload.get("shortname"),
                    fullname=payload.get("fullname") or payload.get("displayname") or external_id,
                    display_name=payload.get("displayname") or payload.get("fullname") or external_id,
                    course_url=payload.get("viewurl"),
                    visible=bool(payload.get("visible", True)),
                    raw_payload=payload,
                    first_seen_at=now,
                    last_seen_at=now,
                )
                self.context.session.add(course)
                self.context.session.flush()
            else:
                course.shortname = payload.get("shortname")
                course.fullname = payload.get("fullname") or course.fullname
                course.display_name = payload.get("displayname") or course.display_name
                course.course_url = payload.get("viewurl")
                course.visible = bool(payload.get("visible", True))
                course.raw_payload = payload
                course.last_seen_at = now

            upsert_source_object(
                self.context.session,
                source_account_id=self.context.source_account.id,
                external_id=external_id,
                object_type="course",
                course_id=course.id,
                parent_external_id=None,
                source_url=payload.get("viewurl"),
                raw_payload=payload,
            )
            processed += 1

        return {"courses_processed": processed}


class MoodleCourseContentsCollector(BaseCollector):
    name = "moodle_contents"

    def collect(self, run) -> dict[str, Any]:
        courses = self.context.session.scalars(
            select(Course).where(Course.source_account_id == self.context.source_account.id)
        ).all()
        client = MoodleServiceClient(self.context.settings, session=self.context.session, source_account=self.context.source_account)
        processed_courses = 0
        processed_modules = 0
        created_items = 0
        updated_items = 0
        removed_objects = 0
        try:
            for course in courses:
                contents = client.get_course_contents(int(course.external_id))
                relative_path, content_hash, size_bytes = self.context.artifact_store.write_json(
                    f"moodle/course_contents/{course.external_id}",
                    "contents",
                    contents,
                )
                course_object = self.context.session.scalar(
                    select(SourceObject).where(
                        SourceObject.source_account_id == self.context.source_account.id,
                        SourceObject.external_id == course.external_id,
                        SourceObject.object_type == "course",
                    )
                )
                artifact = create_raw_artifact(
                    self.context.session,
                    collector_run_id=run.id,
                    source_object_id=course_object.id if course_object else None,
                    artifact_type="json",
                    mime_type="application/json",
                    storage_path=relative_path,
                    content_hash=content_hash,
                    size_bytes=size_bytes,
                    source_url=course.course_url,
                    metadata_json={"collector": self.name, "course_external_id": course.external_id},
                )

                live_external_ids: set[str] = set()
                for section in contents:
                    for module in section.get("modules", []):
                        live_external_ids.add(str(module["id"]))
                        changed = self._upsert_module(course, module, artifact.id)
                        processed_modules += 1
                        if changed == "created":
                            created_items += 1
                        elif changed == "updated":
                            updated_items += 1

                removed_objects += mark_removed_source_objects(
                    self.context.session,
                    source_account_id=self.context.source_account.id,
                    course_id=course.id,
                    object_types=list(MODULE_TO_ITEM_TYPE.keys()),
                    live_external_ids=live_external_ids,
                )
                processed_courses += 1
            return {
                "courses_processed": processed_courses,
                "modules_processed": processed_modules,
                "items_created": created_items,
                "items_updated": updated_items,
                "removed_objects": removed_objects,
            }
        finally:
            client.close()

    def _upsert_module(self, course: Course, module: dict[str, Any], source_artifact_id: int) -> str:
        source_object, _ = upsert_source_object(
            self.context.session,
            source_account_id=self.context.source_account.id,
            external_id=str(module["id"]),
            object_type=module.get("modname", "module"),
            course_id=course.id,
            parent_external_id=course.external_id,
            source_url=module.get("url"),
            raw_payload=module,
        )
        due_at, starts_at, facts = extract_module_date_facts(module)
        body_text = strip_html(module.get("description"))
        item, state = upsert_normalized_item(
            self.context.session,
            source_object_id=source_object.id,
            course_id=course.id,
            item_type=module_item_type(module.get("modname", "module")),
            title=module.get("name") or str(module["id"]),
            body_text=body_text,
            published_at=None,
            starts_at=starts_at,
            due_at=due_at,
            primary_url=module.get("url"),
            raw_payload=module,
            source_artifact_id=source_artifact_id,
            facts_payload=[
                {"fact_type": fact.fact_type, "value": fact.value, "extractor_type": fact.extractor_type}
                for fact in facts
            ],
        )
        replace_item_facts(self.context.session, item=item, facts=facts, source_artifact_id=source_artifact_id)
        if state.state in {"created", "updated"}:
            schedule_notifications_for_item(self.context.session, item, state)
        return state.state


class MoodleCourseUpdatesCollector(BaseCollector):
    name = "moodle_updates"

    def collect(self, run) -> dict[str, Any]:
        client = MoodleServiceClient(self.context.settings, session=self.context.session, source_account=self.context.source_account)
        checkpoint = (self.context.source_account.metadata_json or {}).get("last_updates_sync")
        since = (
            datetime.fromisoformat(checkpoint)
            if checkpoint
            else datetime.now(UTC) - timedelta(days=3)
        )
        courses = self.context.session.scalars(select(Course).order_by(Course.id)).all()
        changed_course_ids: set[int] = set()
        changed_module_ids: set[str] = set()
        try:
            for course in courses:
                payload = client.get_updates_since(int(course.external_id), since)
                relative_path, content_hash, size_bytes = self.context.artifact_store.write_json(
                    f"moodle/updates/{course.external_id}",
                    "updates",
                    payload,
                )
                create_raw_artifact(
                    self.context.session,
                    collector_run_id=run.id,
                    source_object_id=None,
                    artifact_type="json",
                    mime_type="application/json",
                    storage_path=relative_path,
                    content_hash=content_hash,
                    size_bytes=size_bytes,
                    source_url=course.course_url,
                    metadata_json={"collector": self.name, "course_external_id": course.external_id},
                )
                for instance in payload.get("instances", []):
                    if instance.get("contextlevel") == "module":
                        changed_course_ids.add(course.id)
                        changed_module_ids.add(str(instance["id"]))

            if changed_course_ids:
                contents_collector = MoodleCourseContentsCollector(self.context)
                for course in courses:
                    if course.id in changed_course_ids:
                        contents = client.get_course_contents(int(course.external_id))
                        for section in contents:
                            for module in section.get("modules", []):
                                if str(module["id"]) in changed_module_ids:
                                    contents_collector._upsert_module(course, module, None)

            metadata = self.context.source_account.metadata_json or {}
            metadata["last_updates_sync"] = datetime.now(UTC).isoformat()
            self.context.source_account.metadata_json = metadata
            run.checkpoint = {"since": since.isoformat(), "changed_course_ids": sorted(changed_course_ids)}
            return {
                "courses_checked": len(courses),
                "changed_courses": len(changed_course_ids),
                "changed_modules": len(changed_module_ids),
            }
        finally:
            client.close()


class MoodleForumCollector(BaseCollector):
    name = "moodle_forums"

    def collect(self, run) -> dict[str, Any]:
        client = MoodleServiceClient(self.context.settings, session=self.context.session, source_account=self.context.source_account)
        courses = self.context.session.scalars(select(Course).order_by(Course.id)).all()
        course_ids = [int(course.external_id) for course in courses]
        forums = client.get_forums_by_courses(course_ids)
        relative_path, content_hash, size_bytes = self.context.artifact_store.write_json(
            "moodle/forums",
            "forums",
            forums,
        )
        create_raw_artifact(
            self.context.session,
            collector_run_id=run.id,
            source_object_id=None,
            artifact_type="json",
            mime_type="application/json",
            storage_path=relative_path,
            content_hash=content_hash,
            size_bytes=size_bytes,
            source_url=None,
            metadata_json={"collector": self.name, "forum_count": len(forums)},
        )
        processed = 0
        discussions_processed = 0
        for forum in forums:
            course = self.context.session.scalar(
                select(Course).where(Course.external_id == str(forum["course"]))
            )
            if course is None:
                continue
            forum_object, _ = upsert_source_object(
                self.context.session,
                source_account_id=self.context.source_account.id,
                external_id=str(forum["id"]),
                object_type="forum_container",
                course_id=course.id,
                parent_external_id=course.external_id,
                source_url=forum.get("cmid") and f"{self.context.settings.moodle_base_url}/mod/forum/view.php?id={forum['cmid']}",
                raw_payload=forum,
            )
            discussions = client.get_forum_discussions(int(forum["id"]))
            relative_path, content_hash, size_bytes = self.context.artifact_store.write_json(
                f"moodle/forums/{forum['id']}",
                "discussions",
                discussions,
            )
            artifact = create_raw_artifact(
                self.context.session,
                collector_run_id=run.id,
                source_object_id=forum_object.id,
                artifact_type="json",
                mime_type="application/json",
                storage_path=relative_path,
                content_hash=content_hash,
                size_bytes=size_bytes,
                source_url=forum_object.source_url,
                metadata_json={"collector": self.name, "forum_id": forum["id"]},
            )
            for discussion in discussions.get("discussions", []):
                self._upsert_discussion(course, forum, discussion, artifact.id)
                discussions_processed += 1
            processed += 1
        client.close()
        return {"forums_processed": processed, "discussions_processed": discussions_processed}

    def _upsert_discussion(self, course: Course, forum: dict[str, Any], discussion: dict[str, Any], source_artifact_id: int) -> None:
        payload = {"forum": forum, "discussion": discussion}
        source_object, _ = upsert_source_object(
            self.context.session,
            source_account_id=self.context.source_account.id,
            external_id=str(discussion["discussion"]),
            object_type="forum_discussion",
            course_id=course.id,
            parent_external_id=str(forum["id"]),
            source_url=discussion.get("discussionurl"),
            raw_payload=payload,
        )
        body_text = strip_html(discussion.get("message") or discussion.get("subject"))
        facts = extract_date_facts_from_text(body_text or "")
        item, state = upsert_normalized_item(
            self.context.session,
            source_object_id=source_object.id,
            course_id=course.id,
            item_type=forum_item_type(forum.get("type", "general")),
            title=discussion.get("name") or discussion.get("subject") or str(discussion["discussion"]),
            body_text=body_text,
            published_at=epoch_to_datetime(int(discussion["created"])) if discussion.get("created") else None,
            starts_at=None,
            due_at=None,
            primary_url=discussion.get("discussionurl"),
            raw_payload=payload,
            source_artifact_id=source_artifact_id,
            facts_payload=[
                {"fact_type": fact.fact_type, "value": fact.value, "extractor_type": fact.extractor_type}
                for fact in facts
            ],
        )
        replace_item_facts(self.context.session, item=item, facts=facts, source_artifact_id=source_artifact_id)
        if state.state in {"created", "updated"}:
            schedule_notifications_for_item(self.context.session, item, state)


class MoodleAssignmentsCollector(BaseCollector):
    name = "moodle_assignments"

    def collect(self, run) -> dict[str, Any]:
        client = MoodleServiceClient(self.context.settings, session=self.context.session, source_account=self.context.source_account)
        courses = self.context.session.scalars(select(Course).order_by(Course.id)).all()
        payload = client.get_assignments([int(course.external_id) for course in courses])
        relative_path, content_hash, size_bytes = self.context.artifact_store.write_json(
            "moodle/assignments",
            "assignments",
            payload,
        )
        create_raw_artifact(
            self.context.session,
            collector_run_id=run.id,
            source_object_id=None,
            artifact_type="json",
            mime_type="application/json",
            storage_path=relative_path,
            content_hash=content_hash,
            size_bytes=size_bytes,
            source_url=None,
            metadata_json={"collector": self.name},
        )
        processed = 0
        for course_payload in payload.get("courses", []):
            course = self.context.session.scalar(
                select(Course).where(Course.external_id == str(course_payload["id"]))
            )
            if course is None:
                continue
            for assignment in course_payload.get("assignments", []):
                source_object, _ = upsert_source_object(
                    self.context.session,
                    source_account_id=self.context.source_account.id,
                    external_id=str(assignment["id"]),
                    object_type="assignment",
                    course_id=course.id,
                    parent_external_id=course.external_id,
                    source_url=f"{self.context.settings.moodle_base_url}/mod/assign/view.php?id={assignment['cmid']}",
                    raw_payload=assignment,
                )
                body_text = strip_html(assignment.get("intro"))
                facts = extract_date_facts_from_text(body_text or "")
                if assignment.get("duedate"):
                    facts.append(
                        ExtractedFact(
                            fact_type="due_at",
                            value={"value": epoch_to_datetime(int(assignment["duedate"])).isoformat()},
                            confidence=1.0,
                            extractor_type="assignment_api",
                            source_span="duedate",
                        )
                    )
                item, state = upsert_normalized_item(
                    self.context.session,
                    source_object_id=source_object.id,
                    course_id=course.id,
                    item_type="assignment",
                    title=assignment.get("name") or str(assignment["id"]),
                    body_text=body_text,
                    published_at=epoch_to_datetime(int(assignment["allowsubmissionsfromdate"])) if assignment.get("allowsubmissionsfromdate") else None,
                    starts_at=epoch_to_datetime(int(assignment["allowsubmissionsfromdate"])) if assignment.get("allowsubmissionsfromdate") else None,
                    due_at=epoch_to_datetime(int(assignment["duedate"])) if assignment.get("duedate") else None,
                    primary_url=source_object.source_url,
                    raw_payload=assignment,
                    facts_payload=[
                        {"fact_type": fact.fact_type, "value": fact.value, "extractor_type": fact.extractor_type}
                        for fact in facts
                    ],
                )
                replace_item_facts(self.context.session, item=item, facts=facts, source_artifact_id=None)
                if state.state in {"created", "updated"}:
                    schedule_notifications_for_item(self.context.session, item, state)
                processed += 1
        client.close()
        return {"assignments_processed": processed}


class MoodleGradesCollector(BaseCollector):
    name = "moodle_grades"

    def collect(self, run) -> dict[str, Any]:
        client = MoodleServiceClient(self.context.settings, session=self.context.session, source_account=self.context.source_account)
        processed = 0
        for course in self.context.session.scalars(select(Course).order_by(Course.id)).all():
            payload = client.get_grade_items(int(course.external_id))
            relative_path, content_hash, size_bytes = self.context.artifact_store.write_json(
                f"moodle/grades/{course.external_id}",
                "grade_items",
                payload,
            )
            create_raw_artifact(
                self.context.session,
                collector_run_id=run.id,
                source_object_id=None,
                artifact_type="json",
                mime_type="application/json",
                storage_path=relative_path,
                content_hash=content_hash,
                size_bytes=size_bytes,
                source_url=course.course_url,
                metadata_json={"collector": self.name, "course_external_id": course.external_id},
            )
            for grade_item in payload.get("gradeItems", []):
                source_object, _ = upsert_source_object(
                    self.context.session,
                    source_account_id=self.context.source_account.id,
                    external_id=str(grade_item["id"]),
                    object_type="grade_item",
                    course_id=course.id,
                    parent_external_id=course.external_id,
                    source_url=course.course_url,
                    raw_payload=grade_item,
                )
                item, state = upsert_normalized_item(
                    self.context.session,
                    source_object_id=source_object.id,
                    course_id=course.id,
                    item_type="grade_item",
                    title=grade_item.get("itemname") or str(grade_item["id"]),
                    body_text=grade_item.get("category"),
                    published_at=None,
                    starts_at=None,
                    due_at=None,
                    primary_url=course.course_url,
                    raw_payload=grade_item,
                )
                if state.state in {"created", "updated"}:
                    schedule_notifications_for_item(self.context.session, item, state)
                processed += 1
        client.close()
        return {"grade_items_processed": processed}


class MoodleCalendarCollector(BaseCollector):
    name = "moodle_calendar"

    def collect(self, run) -> dict[str, Any]:
        client = MoodleServiceClient(self.context.settings, session=self.context.session, source_account=self.context.source_account)
        site_info = client.get_site_info()
        export_token = client.get_calendar_export_token()
        ics_text = client.get_calendar_export(user_id=int(site_info["userid"]), export_token=export_token)
        client.close()
        feed_payload = {"user_id": site_info["userid"], "export_token": export_token}
        source_object, _ = upsert_source_object(
            self.context.session,
            source_account_id=self.context.source_account.id,
            external_id=str(site_info["userid"]),
            object_type="calendar_feed",
            course_id=None,
            parent_external_id=None,
            source_url=f"{self.context.settings.moodle_base_url}/calendar/export_execute.php",
            raw_payload=feed_payload,
        )
        relative_path, content_hash, size_bytes = self.context.artifact_store.write_text(
            "moodle/calendar",
            "export",
            ics_text,
            suffix=".ics",
        )
        artifact = create_raw_artifact(
            self.context.session,
            collector_run_id=run.id,
            source_object_id=source_object.id,
            artifact_type="ics",
            mime_type="text/calendar",
            storage_path=relative_path,
            content_hash=content_hash,
            size_bytes=size_bytes,
            source_url=source_object.source_url,
            metadata_json={"collector": self.name},
        )
        events = parse_ics_events(ics_text)
        processed = 0
        for event in events:
            resolved_course = resolve_course_from_categories(self.context.session, event.get("categories") or [])
            source_object_event, _ = upsert_source_object(
                self.context.session,
                source_account_id=self.context.source_account.id,
                external_id=event["uid"],
                object_type="calendar_event",
                course_id=resolved_course.id if resolved_course else None,
                parent_external_id=str(site_info["userid"]),
                source_url=source_object.source_url,
                raw_payload=event,
            )
            facts = extract_date_facts_from_text(event["description"] or "")
            item, state = upsert_normalized_item(
                self.context.session,
                source_object_id=source_object_event.id,
                course_id=resolved_course.id if resolved_course else None,
                item_type="calendar_event",
                title=event["summary"],
                body_text=event["description"],
                published_at=epoch_to_datetime(int(datetime.now(UTC).timestamp())),
                starts_at=datetime.fromisoformat(event["starts_at"]) if event["starts_at"] else None,
                due_at=None,
                primary_url=source_object.source_url,
                raw_payload=event,
                source_artifact_id=artifact.id,
                facts_payload=[
                    {"fact_type": fact.fact_type, "value": fact.value, "extractor_type": fact.extractor_type}
                    for fact in facts
                ],
            )
            replace_item_facts(self.context.session, item=item, facts=facts, source_artifact_id=artifact.id)
            if state.state in {"created", "updated"}:
                schedule_notifications_for_item(self.context.session, item, state)
            processed += 1
        return {"events_processed": processed}


class MoodleFilesCollector(BaseCollector):
    name = "moodle_files"

    def collect(self, run) -> dict[str, Any]:
        client = MoodleServiceClient(self.context.settings, session=self.context.session, source_account=self.context.source_account)
        modules = self.context.session.scalars(
            select(SourceObject).where(SourceObject.object_type.in_(["resource", "folder", "page", "assign"]))
        ).all()
        candidates: list[tuple[SourceObject, dict[str, Any]]] = []
        for module in modules:
            contents = module.raw_payload.get("contents", []) or []
            for content in contents:
                url = content.get("fileurl")
                if url and "pluginfile.php" in url:
                    candidates.append((module, content))
        candidates.sort(key=lambda pair: (pair[0].external_id, pair[1].get("filename") or ""))
        total_candidates = len(candidates)
        metadata = self.context.source_account.metadata_json or {}
        start_index = int(metadata.get("moodle_files_cursor", 0)) if total_candidates else 0
        limit = self.context.settings.file_download_limit_per_run
        batch = candidates[start_index : start_index + limit]
        if len(batch) < limit and total_candidates > limit:
            batch.extend(candidates[: limit - len(batch)])
        next_index = (start_index + len(batch)) % total_candidates if total_candidates else 0

        downloaded = 0
        extracted = 0
        for module, content in batch:
            url = content.get("fileurl")
            filename = content.get("filename") or "file"
            bytes_content = client.download_file(url)
            suffix = Path(filename).suffix or ".bin"
            relative_path, content_hash, size_bytes = self.context.artifact_store.write_bytes(
                f"moodle/files/{module.external_id}",
                safe_filename(Path(filename).stem),
                bytes_content,
                suffix=suffix,
            )
            raw_payload = {
                "module_id": module.external_id,
                "content": content,
                "filename": filename,
            }
            file_external_id = f"{module.external_id}:{content.get('filepath') or '/'}:{filename}"
            file_object, _ = upsert_source_object(
                self.context.session,
                source_account_id=self.context.source_account.id,
                external_id=file_external_id,
                object_type="module_file",
                course_id=module.course_id,
                parent_external_id=module.external_id,
                source_url=url,
                raw_payload=raw_payload,
            )
            artifact = create_raw_artifact(
                self.context.session,
                collector_run_id=run.id,
                source_object_id=file_object.id,
                artifact_type="file",
                mime_type=content.get("mimetype") or "application/octet-stream",
                storage_path=relative_path,
                content_hash=content_hash,
                size_bytes=size_bytes,
                source_url=url,
                metadata_json={"collector": self.name, "filename": filename},
            )
            text, extraction_mode = extract_text_for_file(filename, content.get("mimetype") or "", bytes_content)
            review_status, review_reason = derive_review_status(filename, text)
            facts: list[ExtractedFact] = []
            if text:
                text_path, text_hash, text_size = self.context.artifact_store.write_text(
                    f"moodle/files/{module.external_id}",
                    safe_filename(Path(filename).stem) + "-text",
                    text,
                )
                create_raw_artifact(
                    self.context.session,
                    collector_run_id=run.id,
                    source_object_id=file_object.id,
                    artifact_type="extracted_text",
                    mime_type="text/plain",
                    storage_path=text_path,
                    content_hash=text_hash,
                    size_bytes=text_size,
                    source_url=url,
                    metadata_json={"collector": self.name, "extraction_mode": extraction_mode},
                    extraction_status="completed",
                )
                facts = extract_date_facts_from_text(text)
                extracted += 1
            elif extraction_mode == "failed":
                review_status = "needs_review"
                review_reason = "text_extraction_failed"

            due_candidates = [
                datetime.fromisoformat(fact.value["value"])
                for fact in facts
                if fact.fact_type == "due_at" and fact.value.get("value")
            ]
            start_candidates = [
                datetime.fromisoformat(fact.value["value"])
                for fact in facts
                if fact.fact_type in {"class_session_at", "starts_at"} and fact.value.get("value")
            ]
            item, state = upsert_normalized_item(
                self.context.session,
                source_object_id=file_object.id,
                course_id=module.course_id,
                item_type="material_file",
                title=filename,
                body_text=text,
                published_at=None,
                starts_at=start_candidates[0] if start_candidates else None,
                due_at=due_candidates[0] if due_candidates else None,
                primary_url=module.source_url,
                raw_payload=raw_payload,
                review_status=review_status,
                review_reason=review_reason,
                source_artifact_id=artifact.id,
                facts_payload=[
                    {"fact_type": fact.fact_type, "value": fact.value, "extractor_type": fact.extractor_type}
                    for fact in facts
                ],
            )
            replace_item_facts(self.context.session, item=item, facts=facts, source_artifact_id=artifact.id)
            if state.state in {"created", "updated"}:
                schedule_notifications_for_item(self.context.session, item, state)
            downloaded += 1
        client.close()
        metadata["moodle_files_cursor"] = next_index
        self.context.source_account.metadata_json = metadata
        run.checkpoint = {"start_index": start_index, "next_index": next_index, "total_candidates": total_candidates}
        return {
            "files_downloaded": downloaded,
            "files_with_text": extracted,
            "total_candidates": total_candidates,
            "next_index": next_index,
        }
