"""
EquiQuant — Aggressive Model Tuning
Wider weight spreads so model forms strong independent view vs market.
Run from C:\EquiQuant:  python tune_model.py
"""

import sys, os, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal, Race, Horse, init_db
from datetime import date, timedelta
import numpy as np

init_db()
db = SessionLocal()

JOCKEY_WIN = {
    "f prat":0.31,"flavian prat":0.31,
    "j j hernandez":0.27,"juan j. hernandez":0.27,
    "e jaramillo":0.25,"emisael jaramillo":0.25,
    "k kimura":0.23,"kazushi kimura":0.23,
    "f geroux":0.22,"florent geroux":0.22,
    "a ayuso":0.20,"k frey":0.19,
    "t baze":0.18,"t j pereira":0.17,
    "a fresu":0.16,"v espinoza":0.15,
    "h i berrios":0.14,"c belmont":0.12,
    "a escobedo":0.11,"r gonzalez":0.10,
    "f monroy":0.09,"c herrera":0.09,
    "w r orantes":0.08,"a lezcano":0.08,
    "a aguilar":0.07,
}

TRAINER_WIN = {
    "b baffert":0.32,"bob baffert":0.32,
    "p d'amato":0.28,"philip d'amato":0.28,
    "m w mccarthy":0.26,"mark mccarthy":0.26,
    "p eurton":0.23,"peter eurton":0.23,
    "r baltas":0.22,"richard baltas":0.22,
    "d f o'neill":0.21,"doug o'neill":0.21,
    "j sadler":0.20,"c a lewis":0.19,
    "c dollase":0.18,"r gomez":0.16,
    "d dunham":0.15,"v cerin":0.14,
    "d m jensen":0.13,"r w ellis":0.12,
    "l powell":0.11,"s r knapp":0.10,
    "g vallejo":0.10,"v l garcia":0.09,
    "a p marquez":0.09,"h o palma":0.08,
    "j ramos":0.08,"l barocio":0.07,
    "a mathis":0.07,"j j sierra":0.07,
    "g l lopez":0.06,"e g alvarez":0.06,
    "b mclean":0.06,"j bonde":0.06,
    "m puype":0.05,"d winick":0.08,
}

JT_COMBOS = {
    ("f geroux","b baffert"):0.12,
    ("j j hernandez","b baffert"):0.10,
    ("f prat","b baffert"):0.14,
    ("f geroux","p eurton"):0.08,
    ("a ayuso","p eurton"):0.07,
    ("a fresu","p d'amato"):0.10,
    ("j j hernandez","m w mccarthy"):0.08,
    ("e jaramillo","d f o'neill"):0.07,
    ("k frey","d dunham"):0.05,
    ("j j hernandez","r baltas"):0.07,
    ("k kimura","c a lewis"):0.06,
}

PP_BIAS_SPRINT = [0.165,0.158,0.148,0.136,0.121,0.106,0.088,0.072,0.058,0.046,0.037,0.028]
PP_BIAS_ROUTE  = [0.118,0.128,0.135,0.138,0.130,0.118,0.102,0.088,0.076,0.064,0.052,0.040]
PP_BIAS_TURF   = [0.105,0.112,0.118,0.126,0.130,0.126,0.114,0.098,0.085,0.072,0.060,0.048]

def get_pp_bias(pp, surface, distance):
    idx = min((pp or 1) - 1, 11)
    if "turf" in (surface or "").lower():
        return PP_BIAS_TURF[idx]
    f = 6.0
    d = (distance or "").upper()
    if "1 1/16" in d: f = 8.5
    elif "1 1/8" in d: f = 9.0
    elif "1M" in d or "MILE" in d: f = 8.0
    else:
        m = re.search(r'(\d+\.?\d*)\s*F', d)
        if m: f = float(m.group(1))
    return PP_BIAS_ROUTE[idx] if f >= 8 else PP_BIAS_SPRINT[idx]

def get_stat(name, table, default):
    nl = (name or "").lower().strip()
    for k, v in table.items():
        if k in nl or nl in k: return v
    return default

def get_jt_bonus(jockey, trainer):
    j = (jockey or "").lower()
    t = (trainer or "").lower()
    for (jk, tk), bonus in JT_COMBOS.items():
        if jk in j and tk in t: return bonus
    return 0.0

def odds_to_prob(odds):
    try:
        n, d = str(odds).split("/")
        return float(d) / (float(n) + float(d))
    except: return 0.10

def odds_to_decimal(odds):
    try:
        n, d = str(odds).split("/")
        return (float(n) + float(d)) / float(d)
    except: return 10.0

def score_horse(h, surface, distance):
    pp_bias = get_pp_bias(h.post_position or 1, surface, distance)
    jwin    = get_stat(h.jockey or "", JOCKEY_WIN, 0.08)
    twin    = get_stat(h.trainer or "", TRAINER_WIN, 0.07)
    beyer   = h.beyer_last or 85.0
    bavg    = h.beyer_avg_3 or beyer
    trend   = (beyer - bavg) / max(bavg, 1)
    jt      = get_jt_bonus(h.jockey or "", h.trainer or "")
    days    = h.days_since_last or 30
    wt_over = max(0, (h.weight or 122) - 122)

    return (
        4.50 * pp_bias +
        2.80 * jwin +
        2.60 * twin +
        0.08 * beyer +
        1.20 * trend +
        1.50 * jt +
       -0.25 * (1.0 if days > 60 else 0.0) +
        0.15 * (1.0 if 21 <= days <= 35 else 0.0) +
       -0.05 * wt_over
    )

def run():
    race_date = None
    for delta in range(5):
        for sign in [1, -1]:
            d = (date.today() + timedelta(days=delta * sign)).isoformat()
            if db.query(Race).filter(Race.race_date == d).count() > 0:
                race_date = d
                break
        if race_date: break

    if not race_date:
        print("No races in database. Run reload_equibase.py first.")
        return

    print(f"\nEquiQuant — Tuned Benter Model")
    print(f"Date: {race_date}  |  Min edge: 4%  |  Bankroll: $1,000")
    print("=" * 90)

    BANKROLL   = 1000.0
    MIN_EDGE = 0.04
    races    = db.query(Race).filter(Race.race_date == race_date).order_by(Race.race_number).all()
    all_bets = []

    for race in races:
        horses = db.query(Horse).filter(
            Horse.race_id == race.id, Horse.scratched == False
        ).order_by(Horse.post_position).all()
        if not horses: continue

        surface  = race.surface or "Dirt"
        distance = race.distance or "6F"
        scores   = [score_horse(h, surface, distance) for h in horses]
        max_s    = max(scores)
        exp_s    = [np.exp(s - max_s) for s in scores]
        probs    = [e / sum(exp_s) for e in exp_s]

        print(f"\nRace {race.race_number} — {race.race_name}  [{distance} {surface} | ${race.purse:,.0f}]")
        print(f"  {'PP':<4}{'Horse':<26}{'Jockey':<16}{'ML':<8}{'Model':>7}{'Track':>7}{'Edge':>8}{'Bet':>12}")
        print(f"  {'-'*86}")

        for h, prob in zip(horses, probs):
            odds    = h.morning_line_odds or "9/2"
            track_p = odds_to_prob(odds)
            edge    = prob - track_p
            dec     = odds_to_decimal(odds)
            b       = dec - 1.0
            kf      = max(0, (b * prob - (1 - prob)) / b) if b > 0 else 0
            bet     = min(kf * 0.25 * BANKROLL, 25000) if edge >= MIN_EDGE else 0

            h.model_win_prob   = round(prob, 4)
            h.edge             = round(edge, 4)
            h.kelly_fraction   = round(kf * 0.25, 4)
            h.kelly_bet_amount = round(bet, 2)

            if bet > 0:
                all_bets.append({"race": race.race_number, "horse": h.horse_name,
                                  "jockey": h.jockey, "odds": odds,
                                  "model": prob, "edge": edge, "bet": bet})

            flag = f"  *** BET ${bet:,.0f} ***" if bet > 0 else ""
            print(f"  {h.post_position:<4}{(h.horse_name or ''):<26}"
                  f"{(h.jockey or '')[:14]:<16}{odds:<8}"
                  f"{prob*100:>6.1f}%{track_p*100:>7.1f}%  {edge*100:>+5.1f}%{flag}")

    db.commit()

    print(f"\n{'='*90}")
    print(f"RECOMMENDED BETS — {race_date}")
    print(f"{'='*90}")
    if not all_bets:
        print("\n  No bets meet the 4% edge threshold.")
        print("  Reason: morning line odds are set by sharp traders — model needs")
        print("  historical race results to calibrate and find consistent edges.")
        print("  As you collect more results, run: python -c \"from model import ModelTrainer\"")
        print("  to retrain on actual outcomes and find real market inefficiencies.")
    else:
        total = 0
        print(f"\n  {'Race':<6}{'Horse':<28}{'Odds':<8}{'Model':>7}{'Edge':>7}{'Bet':>10}")
        print(f"  {'-'*70}")
        for b in sorted(all_bets, key=lambda x: -x["edge"]):
            print(f"  R{b['race']:<5}{b['horse']:<28}{b['odds']:<8}"
                  f"{b['model']*100:>5.1f}%  {b['edge']*100:>+4.1f}%  ${b['bet']:>8,.0f}")
            total += b["bet"]
        print(f"  {'-'*70}")
        print(f"  {len(all_bets)} bets  |  Total risk: ${total:,.0f}  |  {total/1000*100:.1f}% of bankroll")

    print(f"\nRefresh http://localhost:8000 to see updated probabilities")
    db.close()

run()
