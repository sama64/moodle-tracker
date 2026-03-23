from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from uni_tracker.config import Settings


class MoodleError(RuntimeError):
    pass


@dataclass
class MoodleServiceClient:
    settings: Settings

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
            response = self._http.get(
                "/login/token.php",
                params={
                    "username": self.settings.moodle_username,
                    "password": self.settings.moodle_password,
                    "service": self.settings.moodle_service,
                },
            )
            response.raise_for_status()
            payload = response.json()
            token = payload.get("token")
            if not token:
                raise MoodleError(f"Failed to obtain Moodle token: {payload}")
            self._token = token
        return self._token

    def call(self, function_name: str, **params: Any) -> Any:
        response = self._http.get(
            "/webservice/rest/server.php",
            params={
                "wstoken": self.token(),
                "wsfunction": function_name,
                "moodlewsrestformat": "json",
                **params,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("exception"):
            raise MoodleError(
                f"{function_name} failed: {payload.get('message', payload.get('exception'))}"
            )
        return payload

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


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def epoch_to_datetime(epoch: int | None) -> datetime | None:
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC)
