"""
EquiQuant — Update Bankroll to $1,000
Run from C:\EquiQuant:  python update_bankroll.py
"""

import os, re

BANKROLL = 1000.0
FILES_TO_UPDATE = [
    "routers/kelly.py",
    "full_model.py",
    "tune_model.py",
    "recalc_kelly.py",
    "reload_equibase.py",
    "fix_data.py",
]

print(f"\nEquiQuant — Updating bankroll to ${BANKROLL:,.0f}")
print("=" * 50)

for filepath in FILES_TO_UPDATE:
    if not os.path.exists(filepath):
        print(f"  SKIP (not found): {filepath}")
        continue

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    original = content

    # Replace all bankroll assignments
    content = re.sub(r'BANKROLL\s*=\s*[\d_,]+\.?\d*', f'BANKROLL   = {BANKROLL}', content)
    content = re.sub(r'bankroll\s*=\s*[\d_,]+\.?\d*', f'bankroll   = {BANKROLL}', content)
    content = re.sub(r'"bankroll":\s*[\d_,]+\.?\d*', f'"bankroll": {BANKROLL}', content)

    # Replace hardcoded dollar amounts in display strings
    content = content.replace('$10,000 bankroll', '$1,000 bankroll')
    content = content.replace('$10,000', '$1,000')
    content = content.replace('847340', '1000')
    content = content.replace('847,340', '1,000')

    if content != original:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  ✓ Updated: {filepath}")
    else:
        print(f"  — No changes: {filepath}")

# Also update the frontend dashboard
frontend = "frontend/index.html"
if os.path.exists(frontend):
    with open(frontend, "r", encoding="utf-8") as f:
        content = f.read()
    content = content.replace('$10,000', '$1,000')
    content = content.replace('$847,340', '$1,000')
    content = content.replace('10000', '1000')
    content = content.replace('847340', '1000')
    with open(frontend, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  ✓ Updated: {frontend}")

print(f"\nRecalculating Kelly bets with ${BANKROLL:,.0f} bankroll...")
print("-" * 50)

# Recalculate and show new bet sizes
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal, Race, Horse, init_db
from datetime import date, timedelta
import numpy as np

init_db()
db = SessionLocal()

KELLY_FRAC = 0.50
MIN_EDGE   = 0.035

def odds_to_decimal(odds):
    try:
        n, d = str(odds).split("/")
        return (float(n) + float(d)) / float(d)
    except: return 10.0

def odds_to_prob(odds):
    try:
        n, d = str(odds).split("/")
        return float(d) / (float(n) + float(d))
    except: return 0.10

# Find best race date
race_date = None
for delta in range(5):
    for sign in [1, -1]:
        d = (date.today() + timedelta(days=delta * sign)).isoformat()
        if db.query(Race).filter(Race.race_date == d).count() > 0:
            race_date = d; break
    if race_date: break

if not race_date:
    print("No races found in database.")
    db.close()
    exit()

races = db.query(Race).filter(Race.race_date == race_date).all()
all_bets = []

for race in races:
    horses = db.query(Horse).filter(
        Horse.race_id == race.id,
        Horse.scratched == False,
        Horse.model_win_prob.isnot(None),
    ).order_by(Horse.post_position).all()

    for h in horses:
        prob = h.model_win_prob or 0
        edge = h.edge or 0
        odds = h.morning_line_odds or "9/2"

        dec     = odds_to_decimal(odds)
        b       = dec - 1.0
        kf_full = max(0, (b * prob - (1 - prob)) / b) if b > 0 else 0
        kf_frac = kf_full * KELLY_FRAC
        bet     = round(kf_frac * BANKROLL, 2) if edge >= MIN_EDGE else 0

        h.kelly_fraction   = round(kf_frac, 4)
        h.kelly_bet_amount = bet

        if bet > 0:
            all_bets.append({
                "race": race.race_number, "horse": h.horse_name,
                "odds": odds, "model": prob, "edge": edge,
                "kf_full": kf_full, "kf_frac": kf_frac, "bet": bet
            })

db.commit()

print(f"\nDate: {race_date} | Bankroll: ${BANKROLL:,.0f} | Kelly: {KELLY_FRAC*100:.0f}%")
print()

if all_bets:
    print(f"  {'Race':<6}{'Horse':<28}{'Odds':<8}{'Edge':>7}{'Full K':>8}{'50%K':>7}{'Bet $':>10}")
    print(f"  {'-'*74}")
    total = 0
    for b in sorted(all_bets, key=lambda x: -x["edge"]):
        print(f"  R{b['race']:<5}{b['horse']:<28}{b['odds']:<8}"
              f"  {b['edge']*100:>+4.1f}%"
              f"  {b['kf_full']*100:>5.1f}%"
              f"  {b['kf_frac']*100:>4.1f}%"
              f"  ${b['bet']:>7.2f}")
        total += b["bet"]
    print(f"  {'-'*74}")
    print(f"\n  Bets:       {len(all_bets)}")
    print(f"  Total risk: ${total:.2f}")
    print(f"  % bankroll: {total/BANKROLL*100:.1f}%")
    print(f"  Remaining:  ${BANKROLL - total:.2f}")
else:
    print("  No bets above 3.5% edge threshold.")

db.close()
print(f"\nRefresh http://localhost:8000 to see updated dashboard")
