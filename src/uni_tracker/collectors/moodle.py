from __future__ import annotations

import html
import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from uni_tracker.collectors.base import BaseCollector
from uni_tracker.models import Course, ItemFact, ItemVersion, NormalizedItem, RawArtifact, SourceObject
from uni_tracker.services.moodle import MoodleServiceClient, epoch_to_datetime, stable_hash


HTML_RE = re.compile(r"<[^>]+>")
DATE_LABEL_RE = re.compile(r"(due|vence|entrega|exam|parcial|recuperatorio|fecha)", re.IGNORECASE)


def strip_html(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = HTML_RE.sub(" ", value)
    return re.sub(r"\s+", " ", html.unescape(cleaned)).strip() or None


def module_item_type(modname: str) -> str:
    mapping = {
        "assign": "assignment",
        "quiz": "quiz",
        "forum": "forum",
        "resource": "material",
        "folder": "material",
        "page": "material",
        "url": "material",
        "label": "material",
    }
    return mapping.get(modname, modname)


def extract_due_start_dates(module: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    due_at = None
    starts_at = None
    for item in module.get("dates", []) or []:
        label = item.get("label", "")
        timestamp = item.get("timestamp")
        if timestamp is None:
            continue
        if DATE_LABEL_RE.search(label):
            due_at = epoch_to_datetime(int(timestamp))
        elif starts_at is None:
            starts_at = epoch_to_datetime(int(timestamp))
    return due_at, starts_at


class MoodleCourseCatalogCollector(BaseCollector):
    name = "moodle_courses"

    def collect(self, run) -> dict[str, Any]:
        client = MoodleServiceClient(self.context.settings)
        try:
            courses = client.get_courses()
        finally:
            client.close()

        relative_path, content_hash, size_bytes = self.context.artifact_store.write_json(
            "moodle/course_catalog",
            "courses",
            courses,
        )
        self.context.session.add(
            RawArtifact(
                collector_run_id=run.id,
                source_object_id=None,
                artifact_type="json",
                mime_type="application/json",
                storage_path=relative_path,
                content_hash=content_hash,
                size_bytes=size_bytes,
                metadata_json={"collector": self.name, "item_count": len(courses)},
            )
        )

        processed = 0
        for payload in courses:
            external_id = str(payload["id"])
            course = self.context.session.scalar(
                select(Course).where(
                    Course.source_account_id == self.context.source_account.id,
                    Course.external_id == external_id,
                )
            )
            now = datetime.now(UTC)
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

            course_object = self.context.session.scalar(
                select(SourceObject).where(
                    SourceObject.source_account_id == self.context.source_account.id,
                    SourceObject.external_id == external_id,
                    SourceObject.object_type == "course",
                )
            )
            payload_hash = stable_hash(payload)
            if course_object is None:
                course_object = SourceObject(
                    source_account_id=self.context.source_account.id,
                    course_id=course.id,
                    external_id=external_id,
                    object_type="course",
                    parent_external_id=None,
                    source_url=payload.get("viewurl"),
                    current_hash=payload_hash,
                    raw_payload=payload,
                    first_seen_at=now,
                    last_seen_at=now,
                )
                self.context.session.add(course_object)
            else:
                course_object.course_id = course.id
                course_object.source_url = payload.get("viewurl")
                course_object.current_hash = payload_hash
                course_object.raw_payload = payload
                course_object.last_seen_at = now
                course_object.deleted_at = None

            processed += 1

        self.context.session.flush()
        return {"courses_processed": processed}


class MoodleCourseContentsCollector(BaseCollector):
    name = "moodle_contents"

    def collect(self, run) -> dict[str, Any]:
        courses = self.context.session.scalars(
            select(Course).where(Course.source_account_id == self.context.source_account.id)
        ).all()
        client = MoodleServiceClient(self.context.settings)
        processed_courses = 0
        processed_modules = 0
        created_items = 0
        updated_items = 0

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
                self.context.session.add(
                    RawArtifact(
                        collector_run_id=run.id,
                        source_object_id=course_object.id if course_object else None,
                        artifact_type="json",
                        mime_type="application/json",
                        storage_path=relative_path,
                        content_hash=content_hash,
                        size_bytes=size_bytes,
                        metadata_json={"collector": self.name, "course_external_id": course.external_id},
                    )
                )

                for section in contents:
                    for module in section.get("modules", []):
                        changed = self._upsert_module(course, module)
                        processed_modules += 1
                        if changed == "created":
                            created_items += 1
                        elif changed == "updated":
                            updated_items += 1

                processed_courses += 1
            self.context.session.flush()
            return {
                "courses_processed": processed_courses,
                "modules_processed": processed_modules,
                "items_created": created_items,
                "items_updated": updated_items,
            }
        finally:
            client.close()

    def _upsert_module(self, course: Course, module: dict[str, Any]) -> str:
        now = datetime.now(UTC)
        external_id = str(module["id"])
        object_type = module.get("modname", "module")
        payload_hash = stable_hash(module)

        source_object = self.context.session.scalar(
            select(SourceObject).where(
                SourceObject.source_account_id == self.context.source_account.id,
                SourceObject.external_id == external_id,
                SourceObject.object_type == object_type,
            )
        )
        if source_object is None:
            source_object = SourceObject(
                source_account_id=self.context.source_account.id,
                course_id=course.id,
                external_id=external_id,
                object_type=object_type,
                parent_external_id=course.external_id,
                source_url=module.get("url"),
                current_hash=payload_hash,
                raw_payload=module,
                first_seen_at=now,
                last_seen_at=now,
            )
            self.context.session.add(source_object)
            self.context.session.flush()
        else:
            source_object.course_id = course.id
            source_object.parent_external_id = course.external_id
            source_object.source_url = module.get("url")
            source_object.current_hash = payload_hash
            source_object.raw_payload = module
            source_object.last_seen_at = now
            source_object.deleted_at = None

        body_text = strip_html(module.get("description"))
        due_at, starts_at = extract_due_start_dates(module)
        normalized_payload = {
            "title": module.get("name") or external_id,
            "body_text": body_text,
            "published_at": None,
            "starts_at": starts_at.isoformat() if starts_at else None,
            "due_at": due_at.isoformat() if due_at else None,
            "primary_url": module.get("url"),
            "course_id": course.id,
            "item_type": module_item_type(object_type),
        }
        field_hash = stable_hash(normalized_payload)

        item = self.context.session.scalar(
            select(NormalizedItem).where(
                NormalizedItem.source_object_id == source_object.id,
                NormalizedItem.item_type == normalized_payload["item_type"],
            )
        )
        if item is None:
            item = NormalizedItem(
                source_object_id=source_object.id,
                course_id=course.id,
                item_type=normalized_payload["item_type"],
                title=normalized_payload["title"],
                body_text=body_text,
                published_at=None,
                starts_at=starts_at,
                due_at=due_at,
                urgency="normal",
                status="active",
                primary_url=module.get("url"),
                field_hash=field_hash,
                raw_payload=module,
            )
            self.context.session.add(item)
            self.context.session.flush()
            self._replace_facts(item, due_at, starts_at, None)
            return "created"

        if item.field_hash == field_hash:
            self._replace_facts(item, due_at, starts_at, None)
            return "unchanged"

        previous_values = {
            "title": item.title,
            "body_text": item.body_text,
            "starts_at": item.starts_at.isoformat() if item.starts_at else None,
            "due_at": item.due_at.isoformat() if item.due_at else None,
            "primary_url": item.primary_url,
        }
        new_values = {
            "title": normalized_payload["title"],
            "body_text": body_text,
            "starts_at": normalized_payload["starts_at"],
            "due_at": normalized_payload["due_at"],
            "primary_url": module.get("url"),
        }
        changed_fields = [field for field, value in new_values.items() if previous_values.get(field) != value]

        item.title = normalized_payload["title"]
        item.body_text = body_text
        item.starts_at = starts_at
        item.due_at = due_at
        item.primary_url = module.get("url")
        item.field_hash = field_hash
        item.raw_payload = module
        self.context.session.flush()

        version_number = len(item.versions) + 1
        self.context.session.add(
            ItemVersion(
                normalized_item_id=item.id,
                source_artifact_id=None,
                version_number=version_number,
                changed_fields=changed_fields,
                previous_values=previous_values,
                new_values=new_values,
            )
        )
        self._replace_facts(item, due_at, starts_at, None)
        return "updated"

    def _replace_facts(
        self,
        item: NormalizedItem,
        due_at: datetime | None,
        starts_at: datetime | None,
        source_artifact_id: int | None,
    ) -> None:
        for fact in list(item.facts):
            self.context.session.delete(fact)

        if due_at is not None:
            self.context.session.add(
                ItemFact(
                    normalized_item_id=item.id,
                    source_artifact_id=source_artifact_id,
                    fact_type="due_at",
                    value_json={"value": due_at.isoformat()},
                    confidence=1.0,
                    extractor_type="module_dates",
                    source_span="module.dates",
                )
            )
        if starts_at is not None:
            self.context.session.add(
                ItemFact(
                    normalized_item_id=item.id,
                    source_artifact_id=source_artifact_id,
                    fact_type="starts_at",
                    value_json={"value": starts_at.isoformat()},
                    confidence=1.0,
                    extractor_type="module_dates",
                    source_span="module.dates",
                )
            )
