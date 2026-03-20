from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session
from database import get_db, ScraperLog, Race, Horse
from datetime import date
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/run")
async def run_full_scrape(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    background_tasks.add_task(_scrape_pipeline, db)
    return {"status": "started", "message": "Scrape pipeline running in background"}


@router.get("/status")
async def scraper_status(db: Session = Depends(get_db)):
    logs = db.query(ScraperLog).order_by(ScraperLog.created_at.desc()).limit(10).all()
    return {
        "recent_runs": [
            {"source": l.source, "status": l.status, "records": l.records,
             "message": l.message, "at": l.created_at.isoformat()}
            for l in logs
        ]
    }


async def _scrape_pipeline(db: Session):
    try:
        from scrapers.santa_anita import SantaAnitaScraper
        logger.info("Pipeline: Starting Santa Anita scrape")

        sa = SantaAnitaScraper()
        result = await sa.scrape_race_card()
        logger.info(f"Pipeline: {len(result.get('races', []))} races found")

        today = result.get("date", date.today().isoformat())

        # CLEAR ALL EXISTING DATA FOR THIS DATE FIRST
        existing_races = db.query(Race).filter(Race.race_date == today).all()
        for race in existing_races:
            db.query(Horse).filter(Horse.race_id == race.id).delete()
        db.query(Race).filter(Race.race_date == today).delete()
        db.commit()
        logger.info(f"Pipeline: Cleared existing data for {today}")

        # SAVE FRESH DATA
        total_horses = 0
        for race_data in result.get("races", []):
            race = Race(
                race_date=today, track="Santa Anita",
                race_number=race_data["race_number"],
                race_name=race_data.get("race_name", f"Race {race_data['race_number']}"),
                distance=race_data.get("distance", ""),
                surface=race_data.get("surface", "Dirt"),
                purse=race_data.get("purse", 0),
                condition=race_data.get("condition", ""),
                post_time=race_data.get("post_time", ""),
                track_condition=result.get("track_condition", "Fast"),
                weather=result.get("weather", "Clear"),
            )
            db.add(race)
            db.flush()

            for entry in race_data.get("entries", []):
                horse = Horse(
                    race_id=race.id, race_date=today,
                    post_position=entry.get("post_position"),
                    horse_name=entry.get("horse_name", ""),
                    jockey=entry.get("jockey", ""),
                    trainer=entry.get("trainer", ""),
                    morning_line_odds=entry.get("morning_line", ""),
                    weight=entry.get("weight"),
                    scratched=entry.get("scratched", False),
                )
                db.add(horse)
                total_horses += 1

        db.commit()

        log = ScraperLog(
            source="santa_anita", status="success", records=total_horses,
            message=f"{len(result.get('races', []))} races scraped — old data cleared first"
        )
        db.add(log)
        db.commit()
        logger.info(f"Pipeline: Complete — {total_horses} horses saved")

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        db.rollback()
