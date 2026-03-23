from __future__ import annotations

import argparse
import time

from apscheduler.schedulers.blocking import BlockingScheduler

from uni_tracker.config import get_settings
from uni_tracker.services.sync import run_all_collectors, run_collector


def _run_collectors_job() -> None:
    run_collector("moodle_courses")
    run_collector("moodle_contents")


def main() -> None:
    parser = argparse.ArgumentParser(description="uni-tracker worker")
    parser.add_argument("--once", action="store_true", help="Run collectors once and exit.")
    args = parser.parse_args()

    if args.once:
        run_all_collectors()
        return

    settings = get_settings()
    scheduler = BlockingScheduler(timezone="America/Argentina/Buenos_Aires")
    scheduler.add_job(run_collector, "interval", minutes=settings.sync_courses_interval_minutes, args=["moodle_courses"])
    scheduler.add_job(
        run_collector,
        "interval",
        minutes=settings.sync_contents_interval_minutes,
        args=["moodle_contents"],
    )

    _run_collectors_job()
    scheduler.start()


if __name__ == "__main__":
    main()
