from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db, Race, Horse
from datetime import date

router = APIRouter()


def get_best_date(db: Session) -> str:
    """
    Return today's date if races exist, otherwise the nearest future date with races.
    This handles cases where entries are posted for tomorrow before today's races.
    """
    today = date.today().isoformat()

    # First try today
    count = db.query(Race).filter(Race.race_date == today).count()
    if count > 0:
        return today

    # Find the nearest date with races (past 1 day or future 3 days)
    from datetime import timedelta
    for delta in [1, -1, 2, 3]:
        check = (date.today() + timedelta(days=delta)).isoformat()
        count = db.query(Race).filter(Race.race_date == check).count()
        if count > 0:
            return check

    # Fall back to most recent date in DB
    latest = db.query(Race.race_date).order_by(Race.race_date.desc()).first()
    return latest[0] if latest else today


@router.get("/today")
async def get_today_races(db: Session = Depends(get_db)):
    race_date = get_best_date(db)
    races = db.query(Race).filter(Race.race_date == race_date).order_by(Race.race_number).all()
    result = []
    for r in races:
        horses = db.query(Horse).filter(Horse.race_id == r.id).order_by(Horse.post_position).all()
        result.append({
            "race": _race_to_dict(r),
            "horses": [_horse_to_dict(h) for h in horses]
        })
    return {"date": race_date, "track": "Santa Anita", "races": result}


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
