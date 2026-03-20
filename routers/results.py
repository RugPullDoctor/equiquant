"""
Results Router — exposes race results, P&L, and model performance
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from database import get_db, Race, Horse, RaceResult, Bet
from datetime import date, timedelta

router = APIRouter()


@router.get("/today")
async def get_today_results(db: Session = Depends(get_db)):
    """Get results for all completed races today."""
    race_date = date.today().isoformat()
    results = db.query(RaceResult).filter(RaceResult.race_date == race_date).all()

    out = []
    for r in results:
        race = db.query(Race).filter(Race.id == r.race_id).first()
        out.append({
            "race_number":  race.race_number if race else "?",
            "race_name":    race.race_name if race else "",
            "winner":       r.winner,
            "place":        r.place,
            "show":         r.show,
            "win_payout":   r.win_payout,
            "place_payout": r.place_payout,
            "show_payout":  r.show_payout,
            "recorded_at":  r.created_at.isoformat() if r.created_at else "",
        })
    return {"date": race_date, "results": out, "races_complete": len(out)}


@router.get("/pnl")
async def get_pnl(days: int = 30, db: Session = Depends(get_db)):
    """Get profit/loss summary for the last N days."""
    since = (date.today() - timedelta(days=days)).isoformat()
    bets = db.query(Bet).filter(
        Bet.race_date >= since,
        Bet.result.isnot(None)
    ).all()

    total_wagered = sum(b.amount for b in bets)
    total_payout  = sum(b.payout or 0 for b in bets)
    total_pl      = sum(b.profit_loss or 0 for b in bets)
    wins          = sum(1 for b in bets if b.result == "WIN")
    losses        = sum(1 for b in bets if b.result == "LOSS")
    roi           = round(total_pl / total_wagered * 100, 2) if total_wagered > 0 else 0

    # Group by date
    by_date = {}
    for b in bets:
        d = b.race_date
        if d not in by_date:
            by_date[d] = {"date":d,"wagered":0,"payout":0,"pl":0,"bets":0,"wins":0}
        by_date[d]["wagered"]  += b.amount
        by_date[d]["payout"]   += b.payout or 0
        by_date[d]["pl"]       += b.profit_loss or 0
        by_date[d]["bets"]     += 1
        by_date[d]["wins"]     += 1 if b.result=="WIN" else 0

    # Running bankroll
    bankroll = 1000.0
    daily = sorted(by_date.values(), key=lambda x: x["date"])
    for d in daily:
        bankroll += d["pl"]
        d["bankroll"] = round(bankroll, 2)
        d["roi"]      = round(d["pl"]/d["wagered"]*100, 2) if d["wagered"] > 0 else 0

    return {
        "days":          days,
        "total_bets":    len(bets),
        "wins":          wins,
        "losses":        losses,
        "win_rate":      round(wins/len(bets)*100,2) if bets else 0,
        "total_wagered": round(total_wagered, 2),
        "total_payout":  round(total_payout, 2),
        "total_pl":      round(total_pl, 2),
        "roi_pct":       roi,
        "current_bankroll": round(1000.0 + total_pl, 2),
        "daily":         daily,
    }


@router.get("/model-performance")
async def get_model_performance(db: Session = Depends(get_db)):
    """How well is the model calibrated? Predicted prob vs actual win rate."""
    bets = db.query(Bet).filter(Bet.result.isnot(None)).all()
    if not bets:
        return {"message": "No completed bets yet"}

    # Bucket by model probability
    buckets = {"0-10":[],"10-20":[],"20-30":[],"30-40":[],"40-50":[],"50+%":[]}
    for b in bets:
        p = (b.model_prob or 0) * 100
        won = 1 if b.result == "WIN" else 0
        if p < 10:   buckets["0-10"].append(won)
        elif p < 20: buckets["10-20"].append(won)
        elif p < 30: buckets["20-30"].append(won)
        elif p < 40: buckets["30-40"].append(won)
        elif p < 50: buckets["40-50"].append(won)
        else:        buckets["50+%"].append(won)

    calibration = []
    for bucket, results in buckets.items():
        if results:
            calibration.append({
                "bucket": bucket,
                "bets":   len(results),
                "actual_win_rate": round(sum(results)/len(results)*100, 1),
            })

    avg_edge = sum(b.edge or 0 for b in bets) / len(bets) if bets else 0

    return {
        "total_bets":    len(bets),
        "avg_edge":      round(avg_edge * 100, 2),
        "calibration":   calibration,
        "message":       "Calibration shows how well model probabilities match real outcomes"
    }
