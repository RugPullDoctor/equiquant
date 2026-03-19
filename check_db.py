"""
EquiQuant — Database Diagnostic
Shows exactly what model values are saved in the DB right now.
Run from C:\EquiQuant:  python check_db.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal, Race, Horse, ScraperLog, init_db
from datetime import date, timedelta

init_db()
db = SessionLocal()

print("\nEquiQuant — Database Diagnostic")
print("=" * 70)

# Find all race dates in DB
all_dates = db.query(Race.race_date).distinct().all()
print(f"\nRace dates in database: {[d[0] for d in all_dates]}")

# Find best date
race_date = None
for delta in range(5):
    for sign in [1, -1]:
        d = (date.today() + timedelta(days=delta*sign)).isoformat()
        if db.query(Race).filter(Race.race_date == d).count() > 0:
            race_date = d; break
    if race_date: break

print(f"Best race date found:    {race_date}")
print(f"Today:                   {date.today().isoformat()}")

if not race_date:
    print("No races found!")
    db.close()
    exit()

races = db.query(Race).filter(Race.race_date == race_date).all()
print(f"\nRaces loaded: {len(races)}")

# Show first race in detail
for race in races[:3]:
    horses = db.query(Horse).filter(Horse.race_id == race.id).order_by(Horse.post_position).all()
    print(f"\nRace {race.race_number} — {race.race_name}")
    print(f"  {'PP':<4}{'Horse':<26}{'ML':<8}{'Model%':>8}{'Edge%':>8}{'Kelly$':>10}{'Place%':>8}")
    print(f"  {'-'*72}")
    for h in horses:
        print(f"  {h.post_position:<4}{(h.horse_name or ''):<26}"
              f"{(h.morning_line_odds or '?'):<8}"
              f"{(h.model_win_prob or 0)*100:>7.1f}%"
              f"{(h.edge or 0)*100:>+7.1f}%"
              f"  ${(h.kelly_bet_amount or 0):>8,.0f}"
              f"  {(h.model_place_prob or 0)*100:>5.1f}%")

# Check scraper logs
print(f"\nLast 5 scraper runs:")
logs = db.query(ScraperLog).order_by(ScraperLog.created_at.desc()).limit(5).all()
for l in logs:
    print(f"  [{l.created_at}] {l.source} — {l.status} — {l.message}")

db.close()
print("\nDiagnostic complete.")
