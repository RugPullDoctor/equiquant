"""
EquiQuant — Recalculate Kelly Bets
Updates all bet sizes using new bankroll/Kelly settings.
No re-scraping needed — just recalculates from existing model probabilities.

Run from C:\EquiQuant:  python recalc_kelly.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal, Race, Horse, init_db
from datetime import date, timedelta

init_db()
db = SessionLocal()

# ── YOUR SETTINGS ─────────────────────────────────────────────────────────────
BANKROLL   = 1000.0   # $1,000 starting bankroll
KELLY_FRAC = 0.50      # 50% fractional Kelly
MIN_EDGE   = 0.035     # 3.5% minimum edge threshold
# No max bet cap

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

print(f"\nEquiQuant — Kelly Recalculation")
print(f"Bankroll:     ${BANKROLL:,.0f}")
print(f"Kelly Frac:   {KELLY_FRAC*100:.0f}%")
print(f"Min Edge:     {MIN_EDGE*100:.1f}%")
print(f"Race date:    {race_date}")
print("=" * 70)

races    = db.query(Race).filter(Race.race_date == race_date).all()
all_bets = []

for race in races:
    horses = db.query(Horse).filter(
        Horse.race_id == race.id,
        Horse.scratched == False,
        Horse.model_win_prob.isnot(None),
    ).order_by(Horse.post_position).all()

    if not horses: continue

    race_bets = []
    for h in horses:
        prob = h.model_win_prob or 0
        edge = h.edge or 0
        odds = h.morning_line_odds or "9/2"

        dec     = odds_to_decimal(odds)
        b       = dec - 1.0
        kf_full = max(0, (b * prob - (1 - prob)) / b) if b > 0 else 0
        kf_frac = kf_full * KELLY_FRAC
        bet     = round(kf_frac * BANKROLL, 2) if edge >= MIN_EDGE else 0

        # Save updated bet to DB
        h.kelly_fraction   = round(kf_frac, 4)
        h.kelly_bet_amount = bet

        if bet > 0:
            race_bets.append({
                "race": race.race_number,
                "horse": h.horse_name,
                "jockey": h.jockey,
                "odds": odds,
                "model": prob,
                "track": odds_to_prob(odds),
                "edge": edge,
                "kf_full": kf_full,
                "kf_frac": kf_frac,
                "bet": bet,
            })
            all_bets.append(race_bets[-1])

    if race_bets:
        print(f"\nRace {race.race_number} — {race.race_name}")
        print(f"  {'Horse':<26}{'Odds':<8}{'Model':>7}{'Track':>7}{'Edge':>8}{'Full K':>8}{'50%K':>8}{'Bet $':>10}")
        print(f"  {'-'*80}")
        for b in race_bets:
            print(f"  {b['horse']:<26}{b['odds']:<8}"
                  f"{b['model']*100:>6.1f}%{b['track']*100:>6.1f}%"
                  f"  {b['edge']*100:>+5.1f}%"
                  f"  {b['kf_full']*100:>5.1f}%"
                  f"  {b['kf_frac']*100:>5.1f}%"
                  f"  ${b['bet']:>7,.2f}")

db.commit()

# ── SUMMARY ───────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"RECOMMENDED BETS — {race_date}")
print(f"{'='*70}")

if all_bets:
    print(f"\n  {'Race':<6}{'Horse':<26}{'Odds':<8}{'Edge':>7}{'Full K':>8}{'50%K':>7}{'Bet $':>10}")
    print(f"  {'-'*72}")
    total = 0
    for b in sorted(all_bets, key=lambda x: -x["edge"]):
        print(f"  R{b['race']:<5}{b['horse']:<26}{b['odds']:<8}"
              f"  {b['edge']*100:>+4.1f}%"
              f"  {b['kf_full']*100:>5.1f}%"
              f"  {b['kf_frac']*100:>4.1f}%"
              f"  ${b['bet']:>7,.2f}")
        total += b["bet"]
    print(f"  {'-'*72}")
    print(f"\n  Bets:          {len(all_bets)}")
    print(f"  Total risk:    ${total:,.2f}")
    print(f"  % of bankroll: {total/BANKROLL*100:.1f}%")
    print(f"  Bankroll left: ${BANKROLL - total:,.2f}")
    print(f"\n  Kelly math example (Sabino Canyon at 3/1):")
    print(f"    b = 4.0 - 1 = 3.0  (net decimal odds)")
    print(f"    p = model win prob")
    print(f"    Full Kelly = (b×p - q) / b")
    print(f"    50% Kelly  = Full Kelly × 0.50")
    print(f"    Bet $      = 50% Kelly × ${BANKROLL:,.0f} bankroll")
else:
    print(f"\n  No bets above {MIN_EDGE*100:.1f}% edge threshold today.")

print(f"\nRefresh http://localhost:8000 to see updated dashboard")
db.close()
