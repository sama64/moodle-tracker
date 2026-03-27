from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from typing import Any

import httpx
from sqlalchemy.orm import Session

from uni_tracker.config import Settings
from uni_tracker.models import SourceAccount


class MoodleError(RuntimeError):
    pass


@dataclass
class MoodleServiceClient:
    settings: Settings
    session: Session | None = None
    source_account: SourceAccount | None = None

    def __post_init__(self) -> None:
        self._token: str | None = None
        self._http = httpx.Client(
            base_url=self.settings.moodle_base_url.rstrip("/"),
            follow_redirects=True,
            timeout=30.0,
        )

    def close(self) -> None:
        self._http.close()

    def token(self) -> str:
        if self._token is None:
            self._token = self._load_cached_token() or self._fetch_token()
        return self._token

    def call(self, function_name: str, **params: Any) -> Any:
        return self._call(function_name, params=params)

    def get_site_info(self) -> dict[str, Any]:
        payload = self.call("core_webservice_get_site_info")
        if not isinstance(payload, dict):
            raise MoodleError("Unexpected site info payload.")
        return payload

    def get_courses(self) -> list[dict[str, Any]]:
        site_info = self.get_site_info()
        payload = self.call("core_enrol_get_users_courses", userid=site_info["userid"])
        if not isinstance(payload, list):
            raise MoodleError("Unexpected course payload.")
        return payload

    def get_course_contents(self, course_id: int) -> list[dict[str, Any]]:
        payload = self.call("core_course_get_contents", courseid=course_id)
        if not isinstance(payload, list):
            raise MoodleError(f"Unexpected course contents payload for course {course_id}.")
        return payload

    def get_updates_since(self, course_id: int, since: datetime) -> dict[str, Any]:
        payload = self.call(
            "core_course_get_updates_since",
            courseid=course_id,
            since=int(since.timestamp()),
        )
        if not isinstance(payload, dict):
            raise MoodleError(f"Unexpected updates payload for course {course_id}.")
        return payload

    def get_forums_by_courses(self, course_ids: list[int]) -> list[dict[str, Any]]:
        payload = self._call(
            "mod_forum_get_forums_by_courses",
            params={"courseids": course_ids},
        )
        if not isinstance(payload, list):
            raise MoodleError("Unexpected forums payload.")
        return payload

    def get_forum_discussions(self, forum_id: int) -> dict[str, Any]:
        payload = self.call("mod_forum_get_forum_discussions", forumid=forum_id)
        if not isinstance(payload, dict):
            raise MoodleError("Unexpected forum discussions payload.")
        return payload

    def get_assignments(self, course_ids: list[int]) -> dict[str, Any]:
        payload = self._call("mod_assign_get_assignments", params={"courseids": course_ids})
        if not isinstance(payload, dict):
            raise MoodleError("Unexpected assignments payload.")
        return payload

    def get_grade_items(self, course_id: int) -> dict[str, Any]:
        payload = self.call("core_grades_get_gradeitems", courseid=course_id)
        if not isinstance(payload, dict):
            raise MoodleError("Unexpected grade items payload.")
        return payload

    def get_calendar_export_token(self) -> str:
        payload = self.call("core_calendar_get_calendar_export_token")
        if not isinstance(payload, dict) or not payload.get("token"):
            raise MoodleError(f"Unexpected calendar export token payload: {payload}")
        return str(payload["token"])

    def get_calendar_export(self, *, user_id: int, export_token: str, what: str = "all", period: str = "recentupcoming") -> str:
        response = self._get_with_retries(
            "/calendar/export_execute.php",
            params={
                "userid": user_id,
                "authtoken": export_token,
                "preset_what": what,
                "preset_time": period,
            },
        )
        return response.text

    def download_file(self, url: str) -> bytes:
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["token"] = self.token()
        tokenized = urlunparse(parsed._replace(query=urlencode(query)))
        response = self._get_with_retries(tokenized)
        return response.content

    def _fetch_token(self) -> str:
        response = self._get_with_retries(
            "/login/token.php",
            params={
                "username": self.settings.moodle_username,
                "password": self.settings.moodle_password,
                "service": self.settings.moodle_service,
            },
        )
        payload = response.json()
        token = payload.get("token")
        if not token:
            raise MoodleError(f"Failed to obtain Moodle token: {payload}")
        self._store_cached_token(str(token))
        return str(token)

    def _load_cached_token(self) -> str | None:
        if self.source_account is None:
            return None
        token = self.source_account.access_token
        fetched_at = self.source_account.access_token_fetched_at
        if not token or fetched_at is None:
            return None
        if _normalize_datetime(fetched_at) + timedelta(seconds=self.settings.moodle_token_ttl_seconds) <= datetime.now(UTC):
            return None
        return token

    def _store_cached_token(self, token: str) -> None:
        if self.source_account is None or self.session is None:
            return
        self.source_account.access_token = token
        self.source_account.access_token_fetched_at = datetime.now(UTC)
        self.session.flush()

    def invalidate_cached_token(self) -> None:
        self._token = None
        if self.source_account is None or self.session is None:
            return
        self.source_account.access_token = None
        self.source_account.access_token_fetched_at = None
        self.session.flush()

    def _call(self, function_name: str, *, params: dict[str, Any]) -> Any:
        for attempt in range(2):
            encoded = {
                "wstoken": self.token(),
                "wsfunction": function_name,
                "moodlewsrestformat": "json",
            }
            encoded.update(_flatten_params(params))
            response = self._get_with_retries("/webservice/rest/server.php", params=encoded)
            payload = response.json()
            if isinstance(payload, dict) and payload.get("exception"):
                message = payload.get("message", payload.get("exception"))
                if payload.get("errorcode") == "invalidtoken" and attempt == 0:
                    self.invalidate_cached_token()
                    continue
                raise MoodleError(f"{function_name} failed: {message}")
            return payload
        raise MoodleError(f"{function_name} failed after token refresh.")

    def _get_with_retries(self, url: str, *, params: dict[str, Any] | None = None) -> httpx.Response:
        delay_seconds = 1.0
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = self._http.get(url, params=params)
                response.raise_for_status()
                return response
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt == 2:
                    break
                time.sleep(delay_seconds)
                delay_seconds *= 2
        raise MoodleError(str(last_error) if last_error else f"Request failed: {url}")


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def epoch_to_datetime(epoch: int | None) -> datetime | None:
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC)


def _flatten_params(params: dict[str, Any], prefix: str | None = None) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in params.items():
        current_key = f"{prefix}[{key}]" if prefix else key
        if isinstance(value, dict):
            flattened.update(_flatten_params(value, current_key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, (dict, list)):
                    flattened.update(_flatten_params({str(index): item}, current_key))
                else:
                    flattened[f"{current_key}[{index}]"] = item
        else:
            flattened[current_key] = value
    return flattened


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
