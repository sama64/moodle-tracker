#!/usr/bin/env python3
"""
Authenticated Moodle capability probe.

What it does:
- Logs in through the Moodle web form.
- Checks key UI pages that matter for collectors.
- Tries token auth against the Moodle mobile/web-service endpoint.
- Writes a report and HTML/JSON snapshots under artifacts/moodle_probe/.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from html.parser import HTMLParser
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
ARTIFACTS_DIR = ROOT / "artifacts" / "moodle_probe"


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[dict[str, str]] = []
        self._current_href: str | None = None
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href")
        if href:
            self._current_href = href
            self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or self._current_href is None:
            return
        text = " ".join(part.strip() for part in self._text_parts if part.strip())
        self.links.append({"href": self._current_href, "text": text})
        self._current_href = None
        self._text_parts = []


def strip_tags(html_text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html_text)).strip()


def extract_title(html_text: str) -> str | None:
    match = re.search(r"<title>(.*?)</title>", html_text, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return re.sub(r"\s+", " ", match.group(1)).strip()


def extract_logintoken(html_text: str) -> str | None:
    match = re.search(
        r'name="logintoken"\s+value="([^"]+)"',
        html_text,
        re.IGNORECASE,
    )
    return match.group(1) if match else None


def extract_user_id(html_text: str) -> int | None:
    match = re.search(r'"userId":\s*(\d+)', html_text)
    if not match:
        return None
    return int(match.group(1))


def detect_login_error(html_text: str) -> str | None:
    patterns = [
        r'<div[^>]+class="[^"]*alert[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]+id="loginerrormessage"[^>]*>(.*?)</div>',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
        if match:
            return strip_tags(match.group(1))
    return None


def normalize_url(base_url: str, href: str) -> str:
    return urllib.parse.urljoin(base_url, href)


@dataclass
class FetchResult:
    url: str
    status: int
    body: str
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def title(self) -> str | None:
        return extract_title(self.body)


class MoodleClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.cookie_jar = CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar)
        )
        self.opener.addheaders = [
            ("User-Agent", "uni-tracker-moodle-probe/0.1"),
        ]

    def fetch(
        self,
        path_or_url: str,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> FetchResult:
        url = (
            path_or_url
            if path_or_url.startswith("http://") or path_or_url.startswith("https://")
            else f"{self.base_url}{path_or_url}"
        )

        request_data = None
        if data is not None:
            request_data = urllib.parse.urlencode(data).encode("utf-8")

        req = urllib.request.Request(url, data=request_data, headers=headers or {})
        with self.opener.open(req, timeout=30) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            body = response.read().decode(charset, errors="replace")
            return FetchResult(
                url=response.geturl(),
                status=response.status,
                body=body,
                headers={k.lower(): v for k, v in response.headers.items()},
            )

    def login(self, username: str, password: str) -> tuple[bool, str | None]:
        login_page = self.fetch("/login/index.php")
        logintoken = extract_logintoken(login_page.body)
        payload = {
            "username": username,
            "password": password,
        }
        if logintoken:
            payload["logintoken"] = logintoken

        login_result = self.fetch("/login/index.php", data=payload)
        login_error = detect_login_error(login_result.body)

        my_page = self.fetch("/my/")
        if "/login/index.php" in my_page.url or 'id="page-login-index"' in my_page.body:
            return False, login_error or "Login did not reach /my/."

        return True, login_error


def save_artifact(relative_path: str, content: str) -> None:
    output_path = ARTIFACTS_DIR / relative_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")


def parse_links(base_url: str, html_text: str) -> list[dict[str, str]]:
    parser = LinkParser()
    parser.feed(html_text)
    seen: set[tuple[str, str]] = set()
    links: list[dict[str, str]] = []
    for item in parser.links:
        href = normalize_url(base_url, item["href"])
        text = item["text"]
        key = (href, text)
        if key in seen:
            continue
        seen.add(key)
        links.append({"href": href, "text": text})
    return links


def page_summary(result: FetchResult, base_url: str) -> dict[str, Any]:
    return {
        "url": result.url,
        "status": result.status,
        "title": result.title,
        "requires_login": "/login/index.php" in result.url or 'id="page-login-index"' in result.body,
        "interesting_markers": {
            "rss": bool(re.search(r"\brss\b", result.body, re.IGNORECASE)),
            "ical": bool(re.search(r"\b(ical|ics)\b", result.body, re.IGNORECASE)),
            "calendar_export": "calendar/export.php" in result.body,
            "token": bool(re.search(r"\btoken\b", result.body, re.IGNORECASE)),
        },
        "sample_links": parse_links(base_url, result.body)[:25],
    }


def detect_course_links(base_url: str, html_text: str) -> list[dict[str, Any]]:
    links = parse_links(base_url, html_text)
    courses: dict[str, dict[str, Any]] = {}
    for link in links:
        if "/course/view.php?id=" not in link["href"]:
            continue
        course_id_match = re.search(r"[?&]id=(\d+)", link["href"])
        if not course_id_match:
            continue
        course_id = course_id_match.group(1)
        if course_id not in courses:
            courses[course_id] = {
                "course_id": int(course_id),
                "url": link["href"],
                "name": link["text"] or f"course {course_id}",
            }
    return sorted(courses.values(), key=lambda item: item["course_id"])


def summarize_course(base_url: str, html_text: str) -> dict[str, Any]:
    activities: dict[str, int] = {}
    for mod_name in re.findall(r"/mod/([^/]+)/view\.php", html_text):
        activities[mod_name] = activities.get(mod_name, 0) + 1
    files = len(re.findall(r"/pluginfile\.php/", html_text))
    return {
        "title": extract_title(html_text),
        "activity_counts": dict(sorted(activities.items())),
        "pluginfile_links": files,
        "links": parse_links(base_url, html_text)[:30],
    }


def fetch_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=30) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset, errors="replace"))


def build_token_url(base_url: str, username: str, password: str, service: str) -> str:
    query = urllib.parse.urlencode(
        {
            "username": username,
            "password": password,
            "service": service,
        }
    )
    return f"{base_url}/login/token.php?{query}"


def build_rest_url(base_url: str, token: str, function: str, **params: Any) -> str:
    query: dict[str, Any] = {
        "wstoken": token,
        "wsfunction": function,
        "moodlewsrestformat": "json",
    }
    query.update(params)
    return f"{base_url}/webservice/rest/server.php?{urllib.parse.urlencode(query)}"


def main() -> int:
    env = load_env(ENV_PATH)
    base_url = env.get("MOODLE_BASE_URL", "").strip()
    username = env.get("MOODLE_USERNAME", "").strip()
    password = env.get("MOODLE_PASSWORD", "").strip()

    if not base_url or not username or not password:
        print(
            "Missing credentials. Copy .env.example to .env and fill "
            "MOODLE_BASE_URL, MOODLE_USERNAME, MOODLE_PASSWORD.",
            file=sys.stderr,
        )
        return 2

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {
        "generated_at_epoch": int(time.time()),
        "base_url": base_url,
        "username": username,
        "login": {},
        "pages": {},
        "courses": [],
        "token_auth": {},
    }

    client = MoodleClient(base_url)

    login_ok, login_error = client.login(username, password)
    report["login"] = {
        "success": login_ok,
        "error": login_error,
    }

    if not login_ok:
        save_artifact("report.json", json.dumps(report, indent=2, ensure_ascii=False))
        print("Login failed. See artifacts/moodle_probe/report.json", file=sys.stderr)
        return 1

    pages_to_check = {
        "dashboard": "/my/",
        "courses_overview": "/my/courses.php",
        "calendar_view": "/calendar/view.php",
        "calendar_export": "/calendar/export.php",
        "notifications": "/message/output/popup/notifications.php",
        "grades_overview": "/grade/report/overview/index.php",
        "preferences": "/user/preferences.php",
    }

    dashboard_html = ""

    for key, path in pages_to_check.items():
        try:
            result = client.fetch(path)
            report["pages"][key] = page_summary(result, base_url)
            save_artifact(f"pages/{key}.html", result.body)
            if key == "dashboard":
                dashboard_html = result.body
                report["login"]["user_id"] = extract_user_id(result.body)
        except urllib.error.URLError as exc:
            report["pages"][key] = {"error": str(exc)}

    course_links = detect_course_links(base_url, dashboard_html)
    if not course_links and isinstance(report["pages"].get("courses_overview"), dict):
        overview_path = ARTIFACTS_DIR / "pages" / "courses_overview.html"
        if overview_path.exists():
            course_links = detect_course_links(base_url, overview_path.read_text(encoding="utf-8"))

    for course in course_links[:5]:
        try:
            course_page = client.fetch(course["url"])
            course["summary"] = summarize_course(base_url, course_page.body)
            save_artifact(f"courses/{course['course_id']}.html", course_page.body)
        except urllib.error.URLError as exc:
            course["summary"] = {"error": str(exc)}
        report["courses"].append(course)

    token_result: dict[str, Any] = {
        "token_endpoint_present": True,
        "mobile_service": {},
    }
    try:
        token_json = fetch_json(build_token_url(base_url, username, password, "moodle_mobile_app"))
        token_result["mobile_service"]["token_response"] = token_json
        token = token_json.get("token")
        if token:
            site_info = fetch_json(
                build_rest_url(base_url, token, "core_webservice_get_site_info")
            )
            token_result["mobile_service"]["site_info"] = site_info
            user_id = site_info.get("userid")
            if user_id is not None:
                courses_json = fetch_json(
                    build_rest_url(
                        base_url,
                        token,
                        "core_enrol_get_users_courses",
                        userid=user_id,
                    )
                )
                token_result["mobile_service"]["courses"] = courses_json
    except Exception as exc:  # noqa: BLE001
        token_result["mobile_service"]["error"] = str(exc)

    report["token_auth"] = token_result

    save_artifact("report.json", json.dumps(report, indent=2, ensure_ascii=False))

    lines = [
        f"# Moodle probe report for {username}",
        "",
        f"- Base URL: {base_url}",
        f"- Login success: {report['login']['success']}",
    ]
    if report["login"].get("user_id") is not None:
        lines.append(f"- Moodle user id: {report['login']['user_id']}")

    lines.extend(
        [
            "",
            "## Page checks",
        ]
    )

    for key, value in report["pages"].items():
        if "error" in value:
            lines.append(f"- {key}: error: {value['error']}")
            continue
        lines.append(
            f"- {key}: status={value['status']}, title={value['title']}, "
            f"requires_login={value['requires_login']}, "
            f"markers={value['interesting_markers']}"
        )

    lines.extend(
        [
            "",
            "## Courses",
        ]
    )
    if report["courses"]:
        for course in report["courses"]:
            summary = course.get("summary", {})
            lines.append(
                f"- [{course['course_id']}] {course['name']}: "
                f"activity_counts={summary.get('activity_counts', {})}, "
                f"pluginfile_links={summary.get('pluginfile_links')}"
            )
    else:
        lines.append("- No course links detected from dashboard/courses overview.")

    lines.extend(
        [
            "",
            "## Token auth",
        ]
    )
    mobile_service = report["token_auth"].get("mobile_service", {})
    if "token_response" in mobile_service:
        token_response = mobile_service["token_response"]
        lines.append(
            f"- moodle_mobile_app token: {'token' in token_response and bool(token_response.get('token'))}"
        )
        if token_response.get("error"):
            lines.append(f"- token error: {token_response['error']}")
        if "site_info" in mobile_service:
            site_info = mobile_service["site_info"]
            functions = site_info.get("functions", [])
            lines.append(
                f"- site info returned: user={site_info.get('fullname')}, "
                f"functions={len(functions)}"
            )
        if "courses" in mobile_service:
            courses = mobile_service["courses"]
            count = len(courses) if isinstance(courses, list) else "unknown"
            lines.append(f"- API course count: {count}")
    elif "error" in mobile_service:
        lines.append(f"- token probe error: {mobile_service['error']}")

    report_md = "\n".join(lines) + "\n"
    save_artifact("report.md", report_md)
    print(report_md)
    print("Artifacts written to artifacts/moodle_probe/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
