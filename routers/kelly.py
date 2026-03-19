from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from database import get_db, Horse, Race
from datetime import date, timedelta

router = APIRouter()

BANKROLL   = 1000.0  # Real starting bankroll
KELLY_FRAC = 0.50     # 50% fractional Kelly
MIN_EDGE   = 0.035    # 3.5% minimum edge


def get_best_date(db: Session) -> str:
    for delta in range(5):
        for sign in [1, -1]:
            d = (date.today() + timedelta(days=delta * sign)).isoformat()
            if db.query(Race).filter(Race.race_date == d).count() > 0:
                return d
    return date.today().isoformat()


def odds_to_decimal(odds):
    try:
        n, d = str(odds).split("/")
        return (float(n) + float(d)) / float(d)
    except:
        return 10.0


def odds_to_prob(odds):
    try:
        n, d = str(odds).split("/")
        return float(d) / (float(n) + float(d))
    except:
        return 0.10


@router.get("/bets/today")
async def get_today_bets(
    kelly_fraction: float = Query(KELLY_FRAC, ge=0.05, le=1.0),
    min_edge: float = Query(MIN_EDGE, ge=0.0, le=0.30),
    db: Session = Depends(get_db)
):
    race_date = get_best_date(db)

    horses = (
        db.query(Horse)
        .join(Race, Horse.race_id == Race.id)
        .filter(
            Race.race_date == race_date,
            Horse.scratched == False,
            Horse.model_win_prob.isnot(None),
            Horse.edge.isnot(None),
        )
        .all()
    )

    bets = []
    for h in horses:
        edge = h.edge or 0
        if edge < min_edge:
            continue

        odds       = h.live_odds or h.morning_line_odds or "9/2"
        model_prob = h.model_win_prob or 0
        track_prob = odds_to_prob(odds)

        dec     = odds_to_decimal(odds)
        b       = dec - 1.0
        kf_full = max(0, (b * model_prob - (1 - model_prob)) / b) if b > 0 else 0
        kf_frac = kf_full * kelly_fraction
        bet     = round(kf_frac * BANKROLL, 2)  # No cap

        race = db.query(Race).filter(Race.id == h.race_id).first()

        bets.append({
            "race_number":   race.race_number if race else "?",
            "race_name":     race.race_name if race else "",
            "post_time":     race.post_time if race else "",
            "distance":      race.distance if race else "",
            "surface":       race.surface if race else "",
            "horse_name":    h.horse_name,
            "jockey":        h.jockey,
            "trainer":       h.trainer,
            "post_position": h.post_position,
            "odds":          odds,
            "model_prob":    round(model_prob, 4),
            "place_prob":    round(h.model_place_prob or 0, 4),
            "show_prob":     round(h.model_show_prob or 0, 4),
            "track_prob":    round(track_prob, 4),
            "edge":          round(edge, 4),
            "edge_pct":      round(edge * 100, 2),
            "kelly_full_pct":round(kf_full * 100, 2),
            "kelly_frac_pct":round(kf_frac * 100, 2),
            "bet_amount":    bet,
            "confidence":    "HIGH" if edge >= 0.08 else "MEDIUM" if edge >= 0.05 else "LOW",
        })

    bets.sort(key=lambda x: x["edge"], reverse=True)
    total_risk = sum(b["bet_amount"] for b in bets)

    return {
        "date":           race_date,
        "kelly_fraction": kelly_fraction,
        "min_edge":       min_edge,
        "bankroll":       BANKROLL,
        "bets":           bets,
        "summary": {
            "count":        len(bets),
            "total_risk":   round(total_risk, 2),
            "pct_bankroll": round(total_risk / BANKROLL * 100, 2),
            "avg_edge":     round(sum(b["edge"] for b in bets) / len(bets) * 100, 2) if bets else 0,
            "high_conf":    sum(1 for b in bets if b["confidence"] == "HIGH"),
            "med_conf":     sum(1 for b in bets if b["confidence"] == "MEDIUM"),
            "low_conf":     sum(1 for b in bets if b["confidence"] == "LOW"),
        }
    }
