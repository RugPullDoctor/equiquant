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
