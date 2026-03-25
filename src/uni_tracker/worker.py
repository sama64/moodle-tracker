from __future__ import annotations

import argparse

from apscheduler.schedulers.blocking import BlockingScheduler

from uni_tracker.config import get_settings
from uni_tracker.db import SessionLocal
from uni_tracker.services.llm import enrich_recent_items
from uni_tracker.services.notifications import dispatch_due_notifications, schedule_daily_digest
from uni_tracker.services.telegram_bot import poll_telegram_commands
from uni_tracker.services.sync import run_all_collectors, run_collector


def _poll_telegram_job() -> None:
    with SessionLocal() as session:
        poll_telegram_commands(session)
        session.commit()


def _run_collectors_job() -> None:
    run_collector("moodle_courses")
    run_collector("moodle_contents")
    run_collector("moodle_updates")
    run_collector("moodle_forums")
    run_collector("moodle_assignments")
    run_collector("moodle_grades")
    run_collector("moodle_calendar")
    run_collector("moodle_files")
    with SessionLocal() as session:
        schedule_daily_digest(session)
        dispatch_due_notifications(session)
        poll_telegram_commands(session)
        session.commit()
    with SessionLocal() as session:
        enrich_recent_items(session)
        session.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="uni-tracker worker")
    parser.add_argument("--once", action="store_true", help="Run collectors once and exit.")
    args = parser.parse_args()

    if args.once:
        run_all_collectors()
        with SessionLocal() as session:
            schedule_daily_digest(session)
            dispatch_due_notifications(session)
            poll_telegram_commands(session)
            session.commit()
        with SessionLocal() as session:
            enrich_recent_items(session)
            session.commit()
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
    scheduler.add_job(run_collector, "interval", minutes=20, args=["moodle_updates"])
    scheduler.add_job(run_collector, "interval", minutes=30, args=["moodle_forums"])
    scheduler.add_job(run_collector, "interval", minutes=60, args=["moodle_assignments"])
    scheduler.add_job(run_collector, "interval", minutes=180, args=["moodle_grades"])
    scheduler.add_job(run_collector, "interval", minutes=60, args=["moodle_calendar"])
    scheduler.add_job(run_collector, "interval", minutes=90, args=["moodle_files"])
    scheduler.add_job(
        _poll_telegram_job,
        "interval",
        seconds=settings.telegram_polling_interval_seconds,
    )
    scheduler.add_job(_run_collectors_job, "cron", hour=settings.daily_digest_hour, minute=0)

    _run_collectors_job()
    scheduler.start()


if __name__ == "__main__":
    main()
