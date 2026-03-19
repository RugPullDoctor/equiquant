import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from database import SessionLocal, init_db

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()


async def start_scheduler():
    init_db()
    logger.info("Database initialized")
    scheduler.add_job(run_morning_scrape, CronTrigger(hour=7, minute=30, timezone="America/Los_Angeles"),
                      id="morning_scrape", name="Morning Race Card Scrape")
    scheduler.start()
    logger.info("Scheduler started")


async def run_morning_scrape():
    logger.info("[Scheduler] Starting morning scrape")
    from scrapers.santa_anita import SantaAnitaScraper
    db = SessionLocal()
    try:
        sa = SantaAnitaScraper()
        result = await sa.scrape_race_card()
        logger.info(f"[Scheduler] {len(result.get('races', []))} races scraped")
    except Exception as e:
        logger.error(f"[Scheduler] Error: {e}")
    finally:
        db.close()
