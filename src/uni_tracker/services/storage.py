from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class ArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def write_json(self, relative_dir: str, stem: str, payload: Any) -> tuple[str, str, int]:
        content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        relative_path = Path(relative_dir) / f"{timestamp}-{stem}.json"
        output_path = self.root / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        return str(relative_path), content_hash, len(content.encode("utf-8"))
