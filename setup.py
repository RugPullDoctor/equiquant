"""
EquiQuant AI — One-Click Setup Script
Run this from C:\EquiQuant to create all missing files and folders.

Usage:
    python setup.py
"""

import os

# ── CREATE FOLDERS ────────────────────────────────────────────────────────────
os.makedirs("routers", exist_ok=True)
os.makedirs("scrapers", exist_ok=True)
os.makedirs("models", exist_ok=True)
os.makedirs("data/pps", exist_ok=True)

# ── FILE CONTENTS ─────────────────────────────────────────────────────────────

FILES = {}

FILES["routers/__init__.py"] = "# routers package\n"
FILES["scrapers/__init__.py"] = "# scrapers package\n"

FILES["routers/races.py"] = '''
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db, Race, Horse

router = APIRouter()

@router.get("/today")
async def get_today_races(db: Session = Depends(get_db)):
    from datetime import date
    today = date.today().isoformat()
    races = db.query(Race).filter(Race.race_date == today).order_by(Race.race_number).all()
    result = []
    for r in races:
        horses = db.query(Horse).filter(Horse.race_id == r.id).order_by(Horse.post_position).all()
        result.append({
            "race": _race_to_dict(r),
            "horses": [_horse_to_dict(h) for h in horses]
        })
    return {"date": today, "track": "Santa Anita", "races": result}

@router.get("/{race_date}")
async def get_races_by_date(race_date: str, db: Session = Depends(get_db)):
    races = db.query(Race).filter(Race.race_date == race_date).order_by(Race.race_number).all()
    if not races:
        raise HTTPException(status_code=404, detail=f"No races found for {race_date}")
    result = []
    for r in races:
        horses = db.query(Horse).filter(Horse.race_id == r.id).order_by(Horse.post_position).all()
        result.append({"race": _race_to_dict(r), "horses": [_horse_to_dict(h) for h in horses]})
    return {"date": race_date, "races": result}

def _race_to_dict(r):
    return {
        "id": r.id, "race_number": r.race_number, "race_name": r.race_name,
        "distance": r.distance, "surface": r.surface, "purse": r.purse,
        "condition": r.condition, "post_time": r.post_time,
        "track_condition": r.track_condition, "weather": r.weather,
    }

def _horse_to_dict(h):
    return {
        "id": h.id, "post_position": h.post_position, "horse_name": h.horse_name,
        "jockey": h.jockey, "trainer": h.trainer,
        "morning_line_odds": h.morning_line_odds, "live_odds": h.live_odds,
        "beyer_last": h.beyer_last, "beyer_avg_3": h.beyer_avg_3,
        "model_win_prob": h.model_win_prob, "edge": h.edge,
        "kelly_bet_amount": h.kelly_bet_amount, "scratched": h.scratched,
    }
'''

FILES["routers/scraper.py"] = '''
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
        logger.info(f"Pipeline: {len(result.get(\'races\', []))} races found")

        today = date.today().isoformat()
        for race_data in result.get("races", []):
            race = db.query(Race).filter(
                Race.race_date == today,
                Race.race_number == race_data["race_number"]
            ).first()
            if not race:
                race = Race(
                    race_date=today, track="Santa Anita",
                    race_number=race_data["race_number"],
                    race_name=race_data.get("race_name", f"Race {race_data[\'race_number\']}"),
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
                horse = db.query(Horse).filter(
                    Horse.race_id == race.id,
                    Horse.horse_name == entry.get("horse_name", "")
                ).first()
                if not horse:
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

        db.commit()
        log = ScraperLog(source="santa_anita", status="success",
                         records=sum(len(r.get("entries", [])) for r in result.get("races", [])),
                         message=f"{len(result.get(\'races\', []))} races scraped")
        db.add(log)
        db.commit()
        logger.info("Pipeline: Complete")

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        db.rollback()
'''

FILES["routers/model.py"] = '''
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db, Horse, Race
from datetime import date

router = APIRouter()

@router.get("/variables")
async def get_variables():
    return {
        "total": 108,
        "groups": {
            "performance": 34,
            "jockey_trainer": 28,
            "track_situational": 25,
            "form_fitness": 21,
        },
        "message": "Model variables loaded successfully"
    }

@router.post("/train")
async def train_model(db: Session = Depends(get_db)):
    return {"status": "skipped", "message": "Need 100+ historical races to train. Keep scraping!"}

@router.get("/inference/{race_date}/{race_number}")
async def run_inference(race_date: str, race_number: int, db: Session = Depends(get_db)):
    race = db.query(Race).filter(
        Race.race_date == race_date,
        Race.race_number == race_number
    ).first()
    if not race:
        raise HTTPException(status_code=404, detail="Race not found")
    horses = db.query(Horse).filter(
        Horse.race_id == race.id, Horse.scratched == False
    ).all()
    return {
        "race_date": race_date,
        "race_number": race_number,
        "horses": len(horses),
        "message": "Run scraper first to populate race data"
    }
'''

FILES["routers/kelly.py"] = '''
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from database import get_db, Horse, Race
from datetime import date

router = APIRouter()
BANKROLL = 847340.0

@router.get("/bets/today")
async def get_today_bets(
    kelly_fraction: float = Query(0.25, ge=0.05, le=1.0),
    min_edge: float = Query(0.04, ge=0.0, le=0.30),
    db: Session = Depends(get_db)
):
    today = date.today().isoformat()
    horses = db.query(Horse).join(Race, Horse.race_id == Race.id).filter(
        Race.race_date == today,
        Horse.scratched == False,
        Horse.model_win_prob.isnot(None)
    ).all()

    bets = []
    for h in horses:
        if not h.model_win_prob or not h.morning_line_odds:
            continue
        edge = h.edge or 0
        if edge < min_edge:
            continue
        bet = min(edge * kelly_fraction * BANKROLL, 25000)
        race = db.query(Race).filter(Race.id == h.race_id).first()
        bets.append({
            "race_number": race.race_number if race else "?",
            "horse_name": h.horse_name,
            "odds": h.live_odds or h.morning_line_odds,
            "model_prob": h.model_win_prob,
            "edge": round(edge, 4),
            "bet_amount": round(bet, 2),
        })

    bets.sort(key=lambda x: x["edge"], reverse=True)
    return {
        "date": today, "bankroll": BANKROLL,
        "bets": bets,
        "summary": {"count": len(bets), "total_risk": round(sum(b["bet_amount"] for b in bets), 2)}
    }
'''

# ── WRITE ALL FILES ───────────────────────────────────────────────────────────

for filepath, content in FILES.items():
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content.lstrip("\n"))
    print(f"  Created: {filepath}")

print("\n✓ All files created successfully!")
print("  Now run: uvicorn main:app --reload --port 8000")
