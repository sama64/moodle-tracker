#!/usr/bin/env python3
"""
Backfill weak Moodle item briefs with a forced LLM refresh.

Usage examples:
  .venv/bin/python scripts/backfill_briefs.py --weak-only
  .venv/bin/python scripts/backfill_briefs.py --item-id 380
  .venv/bin/python scripts/backfill_briefs.py --all-weak --limit 25
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from sqlalchemy import select

from uni_tracker.db import SessionLocal
from uni_tracker.models import ItemBrief, NormalizedItem
from uni_tracker.services.briefs import is_item_brief_weak
from uni_tracker.services.llm import backfill_item_briefs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill weak item briefs.")
    parser.add_argument("--item-id", type=int, action="append", dest="item_ids", help="Target a specific item id. Can be repeated.")
    parser.add_argument("--limit", type=int, default=20, help="Maximum weak items to backfill when selecting automatically.")
    parser.add_argument("--weak-only", action="store_true", help="Only process currently weak briefs (default behavior).")
    parser.add_argument("--all-weak", action="store_true", help="Alias for --weak-only; kept for readability.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with SessionLocal() as session:
        if args.item_ids:
            items = [
                item
                for item in (session.get(NormalizedItem, item_id) for item_id in args.item_ids)
                if item is not None
            ]
        else:
            items = _select_weak_items(session, args.limit)

        if not items:
            print(json.dumps({"processed": 0, "skipped": 0, "selected": 0}, ensure_ascii=False))
            return 0

        result = backfill_item_briefs(session, items, force=True)
        session.commit()
        print(
            json.dumps(
                {
                    "selected": len(items),
                    "processed": result["processed"],
                    "skipped": result["skipped"],
                    "item_ids": [item.id for item in items],
                },
                ensure_ascii=False,
            )
        )
    return 0


def _select_weak_items(session, limit: int) -> list[NormalizedItem]:
    items = session.scalars(
        select(NormalizedItem)
        .join(ItemBrief, ItemBrief.normalized_item_id == NormalizedItem.id)
        .order_by(NormalizedItem.updated_at.desc())
    ).all()
    weak_items = [item for item in items if item.brief is not None and is_item_brief_weak(item, item.brief)]
    return weak_items[:limit]


if __name__ == "__main__":
    raise SystemExit(main())
