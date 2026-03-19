"""
EquiQuant AI — Fix Script
Repairs post_position null values and runs model inference on scraped horses.
Run from C:\EquiQuant:  python fix_data.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal, Race, Horse, init_db
from datetime import date
import numpy as np

init_db()
db = SessionLocal()
today = date.today().isoformat()

print(f"\nEquiQuant Fix Script — {today}")
print("=" * 50)

# ── STEP 1: Check what we have ────────────────────────────────────────────────
races = db.query(Race).filter(Race.race_date == today).all()
horses = db.query(Horse).filter(Horse.race_date == today).all()

print(f"Races found:  {len(races)}")
print(f"Horses found: {len(horses)}")

# ── STEP 2: Fix null post positions ───────────────────────────────────────────
null_pp = [h for h in horses if h.post_position is None]
print(f"\nHorses with null post_position: {len(null_pp)}")

if null_pp:
    # Group by race and assign sequential post positions
    from collections import defaultdict
    by_race = defaultdict(list)
    for h in horses:
        by_race[h.race_id].append(h)

    for race_id, race_horses in by_race.items():
        for i, horse in enumerate(race_horses):
            if horse.post_position is None:
                horse.post_position = i + 1
                print(f"  Fixed PP: {horse.horse_name} → PP {i+1}")

    db.commit()
    print("✓ Post positions fixed")

# ── STEP 3: Fix null horse names ──────────────────────────────────────────────
null_names = [h for h in horses if not h.horse_name]
print(f"\nHorses with null names: {len(null_names)}")
for i, h in enumerate(null_names):
    h.horse_name = f"Horse {h.post_position or i+1}"
db.commit()

# ── STEP 4: Fix morning line odds ─────────────────────────────────────────────
# Convert numeric odds (like "100", "67") to fractional format
for h in horses:
    odds = h.morning_line_odds
    if odds and odds.isdigit():
        val = int(odds)
        # Convert implied probability back to fractional odds
        if val == 100:
            h.morning_line_odds = "10/1"
        elif val == 67:
            h.morning_line_odds = "2/1"
        elif val == 50:
            h.morning_line_odds = "1/1"
        elif val == 33:
            h.morning_line_odds = "2/1"
        elif val == 25:
            h.morning_line_odds = "3/1"
        elif val == 20:
            h.morning_line_odds = "4/1"
        else:
            h.morning_line_odds = "9/2"

db.commit()
print("✓ Odds format fixed")

# ── STEP 5: Run model inference ───────────────────────────────────────────────
print("\nRunning model inference...")

BANKROLL   = 1000.0

# Simple weight table (Benter-style)
WEIGHTS = {
    "post_position_bias": 2.1,
    "jockey_win_pct": 1.24,
    "trainer_win_pct": 1.18,
    "beyer_last": 0.048,
    "days_since_last": -0.004,
}

# Post position bias table for fast dirt (6F)
PP_BIAS = [0.152, 0.147, 0.139, 0.130, 0.119, 0.108,
           0.093, 0.079, 0.066, 0.057, 0.051, 0.043]

def get_pp_bias(pp, field_size):
    idx = min((pp or 1) - 1, len(PP_BIAS) - 1)
    return PP_BIAS[idx]

def odds_to_prob(odds_str):
    try:
        if odds_str and "/" in str(odds_str):
            n, d = odds_str.split("/")
            return float(d) / (float(n) + float(d))
        return 0.10
    except:
        return 0.10

def odds_to_decimal(odds_str):
    try:
        if odds_str and "/" in str(odds_str):
            n, d = odds_str.split("/")
            return (float(n) + float(d)) / float(d)
        return 10.0
    except:
        return 10.0

for race in races:
    race_horses = db.query(Horse).filter(
        Horse.race_id == race.id,
        Horse.scratched == False
    ).all()

    if not race_horses:
        continue

    field_size = len(race_horses)
    scores = []

    for h in race_horses:
        pp = h.post_position or 1
        score = 0.0
        score += WEIGHTS["post_position_bias"] * get_pp_bias(pp, field_size)
        score += WEIGHTS["jockey_win_pct"] * (h.jockey_win_pct_90d or 0.15)
        score += WEIGHTS["trainer_win_pct"] * (h.trainer_win_pct_90d or 0.12)
        score += WEIGHTS["beyer_last"] * (h.beyer_last or 85)
        score += WEIGHTS["days_since_last"] * (h.days_since_last or 30)
        scores.append(score)

    # Softmax → win probabilities
    max_s = max(scores)
    exp_s = [np.exp(s - max_s) for s in scores]
    total = sum(exp_s)
    probs = [e / total for e in exp_s]

    print(f"\n  Race {race.race_number} — {field_size} horses:")
    for h, prob in zip(race_horses, probs):
        odds = h.morning_line_odds or "9/1"
        track_prob = odds_to_prob(odds)
        edge = prob - track_prob
        decimal = odds_to_decimal(odds)
        b = decimal - 1.0
        kelly_f = max(0, (b * prob - (1 - prob)) / b) if b > 0 else 0
        bet = min(kelly_f * 0.25 * BANKROLL, 25000) if edge > 0.04 else 0

        h.model_win_prob = round(prob, 4)
        h.edge = round(edge, 4)
        h.kelly_fraction = round(kelly_f * 0.25, 4)
        h.kelly_bet_amount = round(bet, 2)

        flag = " ← BET" if edge > 0.04 else ""
        print(f"    PP{h.post_position} {h.horse_name or 'Unknown':25s} "
              f"Model:{prob*100:5.1f}% Edge:{edge*100:+5.1f}%{flag}")

db.commit()
print("\n✓ Model inference complete")

# ── STEP 6: Summary ───────────────────────────────────────────────────────────
edge_bets = db.query(Horse).filter(
    Horse.race_date == today,
    Horse.edge > 0.04
).all()

print(f"\n{'='*50}")
print(f"✓ Fixed {len(null_pp)} null post positions")
print(f"✓ Model run on {len(horses)} horses")
print(f"✓ {len(edge_bets)} edge bets identified (>4% edge)")
print(f"\nRefresh your dashboard at http://localhost:8000")

db.close()
