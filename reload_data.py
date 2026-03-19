"""
EquiQuant — Reload Data with Fixed Scraper
Clears today's bad data and re-scrapes with the corrected parser.
Run from C:\EquiQuant:  python reload_data.py
"""

import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal, Race, Horse, ScraperLog, init_db
from scrapers.santa_anita import SantaAnitaScraper
from datetime import date
import numpy as np

init_db()
db = SessionLocal()

async def main():
    # Step 1: Clear today's bad data
    today = date.today().isoformat()
    print(f"Clearing bad data for {today}...")

    deleted_horses = db.query(Horse).filter(Horse.race_date == today).delete()
    deleted_races = db.query(Race).filter(Race.race_date == today).delete()
    db.commit()
    print(f"  Deleted {deleted_races} races, {deleted_horses} horses")

    # Step 2: Re-scrape with fixed scraper
    print("\nRunning fixed scraper...")
    scraper = SantaAnitaScraper()
    result = await scraper.scrape_race_card()

    print(f"  Status:  {'OK' if result['success'] else 'FAILED'}")
    print(f"  Date:    {result['date']}")
    print(f"  Races:   {len(result['races'])}")
    print(f"  Track:   {result['track_condition']}")

    if result.get("error"):
        print(f"  Error:   {result['error']}")

    # Step 3: Save to database
    race_date = result["date"]
    total_horses = 0

    for race_data in result["races"]:
        race = Race(
            race_date=race_date,
            track="Santa Anita Park",
            race_number=race_data["race_number"],
            race_name=race_data.get("race_name", f"Race {race_data['race_number']}"),
            distance=race_data.get("distance", "6F"),
            surface=race_data.get("surface", "Dirt"),
            purse=race_data.get("purse", 65000),
            condition=race_data.get("condition", ""),
            post_time=race_data.get("post_time", ""),
            track_condition=result.get("track_condition", "Fast"),
            weather=result.get("weather", "Clear"),
        )
        db.add(race)
        db.flush()

        for entry in race_data.get("entries", []):
            # Use win_pct from stats table as jockey_win_pct proxy
            win_pct = entry.get("_win_pct", 0.15)
            earnings = entry.get("_earnings", 0)

            # Estimate beyer from earnings (rough proxy until FreePPs connected)
            beyer = min(95, max(75, 75 + (earnings / 10000)))

            horse = Horse(
                race_id=race.id,
                race_date=race_date,
                post_position=entry.get("post_position"),
                horse_name=entry.get("horse_name", ""),
                jockey=entry.get("jockey", "TBD"),
                trainer=entry.get("trainer", "TBD"),
                morning_line_odds=entry.get("morning_line", "9/2"),
                weight=entry.get("weight", 122),
                scratched=entry.get("scratched", False),
                beyer_last=round(beyer, 1),
                beyer_avg_3=round(beyer * 0.97, 1),
                jockey_win_pct_90d=win_pct,
                trainer_win_pct_90d=win_pct * 0.8,
                days_since_last=30,
                field_size=len(race_data["entries"]),
            )
            db.add(horse)
            total_horses += 1

        print(f"  Saved Race {race_data['race_number']}: {len(race_data['entries'])} horses")

    db.commit()

    # Step 4: Log the run
    log = ScraperLog(
        source="santa_anita_fixed",
        status="success" if result["success"] else "error",
        records=total_horses,
        message=f"{len(result['races'])} races, {total_horses} horses — {result['track_condition']} track",
        duration_ms=result.get("duration_ms", 0),
    )
    db.add(log)
    db.commit()

    # Step 5: Run model inference
    print(f"\nRunning model inference on {total_horses} horses...")

    PP_BIAS = [0.152, 0.147, 0.139, 0.130, 0.119, 0.108,
               0.093, 0.079, 0.066, 0.057, 0.051, 0.043]

    def odds_to_prob(odds):
        try:
            if "/" in str(odds):
                n, d = odds.split("/")
                return float(d) / (float(n) + float(d))
            return 0.10
        except:
            return 0.10

    def odds_to_decimal(odds):
        try:
            if "/" in str(odds):
                n, d = odds.split("/")
                return (float(n) + float(d)) / float(d)
            return 10.0
        except:
            return 10.0

    BANKROLL = 847340.0
    races_db = db.query(Race).filter(Race.race_date == race_date).all()

    for race in races_db:
        horses_db = db.query(Horse).filter(
            Horse.race_id == race.id, Horse.scratched == False
        ).all()
        if not horses_db:
            continue

        # Compute scores
        scores = []
        for h in horses_db:
            pp = h.post_position or 1
            pp_bias = PP_BIAS[min(pp - 1, len(PP_BIAS) - 1)]
            score = (2.1 * pp_bias +
                     1.24 * (h.jockey_win_pct_90d or 0.15) +
                     1.18 * (h.trainer_win_pct_90d or 0.12) +
                     0.048 * (h.beyer_last or 85))
            scores.append(score)

        # Softmax
        max_s = max(scores)
        exp_s = [np.exp(s - max_s) for s in scores]
        total = sum(exp_s)
        probs = [e / total for e in exp_s]

        print(f"\n  Race {race.race_number} — {race.race_name}")
        for h, prob in zip(horses_db, probs):
            odds = h.morning_line_odds or "9/2"
            track_p = odds_to_prob(odds)
            edge = prob - track_p
            dec = odds_to_decimal(odds)
            b = dec - 1.0
            kf = max(0, (b * prob - (1 - prob)) / b) if b > 0 else 0
            bet = min(kf * 0.25 * BANKROLL, 25000) if edge > 0.04 else 0

            h.model_win_prob = round(prob, 4)
            h.edge = round(edge, 4)
            h.kelly_fraction = round(kf * 0.25, 4)
            h.kelly_bet_amount = round(bet, 2)

            flag = " ← BET" if edge > 0.04 else ""
            print(f"    PP{h.post_position} {(h.horse_name or 'Unknown'):30s} "
                  f"{prob*100:5.1f}% | edge:{edge*100:+5.1f}%{flag}")

    db.commit()

    # Summary
    edge_bets = db.query(Horse).filter(
        Horse.race_date == race_date, Horse.edge > 0.04
    ).all()

    print(f"\n{'='*50}")
    print(f"Date:       {race_date}")
    print(f"Races:      {len(result['races'])}")
    print(f"Horses:     {total_horses}")
    print(f"Edge bets:  {len(edge_bets)}")
    print(f"\nRefresh your dashboard at http://localhost:8000")

    db.close()

asyncio.run(main())
