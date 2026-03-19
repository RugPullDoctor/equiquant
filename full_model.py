"""
EquiQuant — Full Pipeline Model (Benter Architecture)
Implements every node from the Analytics Warehouse diagram:
  Data → Feature Engineering → Prediction Models → Bet Evaluation →
  Execution Plan → Bet Placement → Post-Race Review

Run from C:\EquiQuant:  python full_model.py
"""

import sys, os, re, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal, Race, Horse, Bet, RaceResult, init_db
from datetime import date, timedelta
import numpy as np

init_db()
db = SessionLocal()

BANKROLL   = 1000.0
MIN_EDGE     = 0.04
KELLY_FRAC   = 0.25
MAX_BET      = 25000.0
MAX_RISK_PCT = 0.15   # never risk more than 15% of bankroll in one day

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — DATA (Analytics Warehouse inputs)
# ══════════════════════════════════════════════════════════════════════════════

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
    ("f prat","b baffert"):0.14,
    ("f geroux","b baffert"):0.12,
    ("j j hernandez","b baffert"):0.10,
    ("a fresu","p d'amato"):0.10,
    ("j j hernandez","m w mccarthy"):0.08,
    ("f geroux","p eurton"):0.08,
    ("j j hernandez","r baltas"):0.07,
    ("a ayuso","p eurton"):0.07,
    ("e jaramillo","d f o'neill"):0.07,
    ("k frey","d dunham"):0.05,
    ("k kimura","c a lewis"):0.06,
}

PP_BIAS_SPRINT = [0.165,0.158,0.148,0.136,0.121,0.106,0.088,0.072,0.058,0.046,0.037,0.028]
PP_BIAS_ROUTE  = [0.118,0.128,0.135,0.138,0.130,0.118,0.102,0.088,0.076,0.064,0.052,0.040]
PP_BIAS_TURF   = [0.105,0.112,0.118,0.126,0.130,0.126,0.114,0.098,0.085,0.072,0.060,0.048]


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — FEATURE ENGINEERING
# Six groups from diagram: Horse Form, Trainer/Jockey, Pace Scenario,
# Surface/Distance, Class Move, Market Movement
# ══════════════════════════════════════════════════════════════════════════════

def get_stat(name, table, default):
    nl = (name or "").lower().strip()
    for k, v in table.items():
        if k in nl or nl in k: return v
    return default

def get_pp_bias(pp, surface, distance):
    idx = min((pp or 1) - 1, 11)
    if "turf" in (surface or "").lower(): return PP_BIAS_TURF[idx]
    f = 6.0
    d = (distance or "").upper()
    if "1 1/16" in d: f = 8.5
    elif "1 1/8" in d: f = 9.0
    elif "1M" in d or "MILE" in d: f = 8.0
    else:
        m = re.search(r'(\d+\.?\d*)\s*F', d)
        if m: f = float(m.group(1))
    return PP_BIAS_ROUTE[idx] if f >= 8 else PP_BIAS_SPRINT[idx]

def get_jt_bonus(jockey, trainer):
    j = (jockey or "").lower()
    t = (trainer or "").lower()
    for (jk, tk), bonus in JT_COMBOS.items():
        if jk in j and tk in t: return bonus
    return 0.0

def engineer_features(h, race, all_horses):
    """
    Compute all 6 feature groups from the diagram.
    Returns a dict of named features used for scoring and reporting.
    """
    surface  = (race.surface or "Dirt")
    distance = (race.distance or "6F")
    purse    = (race.purse or 65000)
    field    = len(all_horses)

    # ── GROUP 1: Horse Form Features ─────────────────────────────────────────
    beyer      = h.beyer_last or 85.0
    beyer_avg  = h.beyer_avg_3 or beyer
    beyer_trend = (beyer - beyer_avg) / max(beyer_avg, 1)   # % improvement
    days        = h.days_since_last or 30
    layoff      = days > 60
    fresh       = 21 <= days <= 35
    wt_over     = max(0, (h.weight or 122) - 122)

    # ── GROUP 2: Trainer / Jockey Features ───────────────────────────────────
    jwin  = get_stat(h.jockey or "", JOCKEY_WIN, 0.08)
    twin  = get_stat(h.trainer or "", TRAINER_WIN, 0.07)
    jt    = get_jt_bonus(h.jockey or "", h.trainer or "")

    # ── GROUP 3: Pace Scenario Features ──────────────────────────────────────
    e1    = h.pace_e1 or 0.0
    lp    = h.pace_lp or 0.0
    # Rank early pace among field (1 = fastest)
    all_e1 = sorted([x.pace_e1 or 0 for x in all_horses], reverse=True)
    e1_rank = (all_e1.index(e1) + 1) / max(field, 1) if e1 in all_e1 else 0.5
    # Lone speed bonus: if this horse is >5pts faster than next
    lone_speed = 1.0 if (len(all_e1) > 1 and e1 > 0 and
                         e1 == all_e1[0] and e1 - all_e1[1] > 5) else 0.0

    # ── GROUP 4: Surface / Distance Features ─────────────────────────────────
    pp_bias    = get_pp_bias(h.post_position or 1, surface, distance)
    surf_dirt  = 1.0 if "dirt" in surface.lower() else 0.0
    surf_turf  = 1.0 if "turf" in surface.lower() else 0.0
    surf_switch = 1.0 if (h.surface_switch or False) else 0.0
    dist_switch = 1.0 if (h.distance_switch or False) else 0.0

    # ── GROUP 5: Class Move Features ─────────────────────────────────────────
    # Estimate class from purse vs typical SA levels
    typical_purse = {"Mdn": 40000, "Clm": 30000, "Alw": 75000, "Stk": 150000}
    cond = (race.condition or "").lower()
    typical = 65000
    for k, v in typical_purse.items():
        if k.lower() in cond: typical = v; break
    class_delta  = (purse - typical) / max(typical, 1)
    class_drop   = 1.0 if class_delta < -0.15 else 0.0
    class_rise   = 1.0 if class_delta > 0.20  else 0.0

    # ── GROUP 6: Market Movement Features ────────────────────────────────────
    # Morning line implied probability (market prior)
    ml_prob      = odds_to_prob(h.morning_line_odds or "9/2")
    # Market rank (1 = ML favourite)
    all_mlp = sorted([odds_to_prob(x.morning_line_odds or "9/2")
                      for x in all_horses], reverse=True)
    market_rank  = (all_mlp.index(ml_prob) + 1) / max(field, 1)
    overlay_flag = 0.0   # set later after model prob computed

    return {
        # Horse form
        "beyer": beyer, "beyer_trend": beyer_trend,
        "days": days, "layoff": layoff, "fresh": fresh, "wt_over": wt_over,
        # Trainer/Jockey
        "jwin": jwin, "twin": twin, "jt": jt,
        # Pace scenario
        "e1": e1, "lp": lp, "e1_rank": e1_rank, "lone_speed": lone_speed,
        # Surface/Distance
        "pp_bias": pp_bias, "surf_switch": surf_switch, "dist_switch": dist_switch,
        # Class move
        "class_delta": class_delta, "class_drop": class_drop, "class_rise": class_rise,
        # Market movement
        "ml_prob": ml_prob, "market_rank": market_rank,
    }

def score_from_features(f):
    """
    Weighted sum of features → raw logit score.
    Weights tuned to produce 15-30% spread across typical 8-horse field.
    """
    return (
        # Surface/Distance (strongest signal at SA)
        4.50 * f["pp_bias"] +
        # Trainer/Jockey
        2.80 * f["jwin"] +
        2.60 * f["twin"] +
        1.50 * f["jt"] +
        # Horse Form
        0.08 * f["beyer"] +
        1.20 * f["beyer_trend"] +
       -0.25 * (1.0 if f["layoff"] else 0.0) +
        0.15 * (1.0 if f["fresh"] else 0.0) +
       -0.05 * f["wt_over"] +
        # Pace scenario
        0.80 * f["lone_speed"] +
       -0.40 * f["e1_rank"] +       # lower rank (faster) = positive
        # Class move
        0.30 * f["class_drop"] +    # dropping in class = easier spot
       -0.20 * f["class_rise"] +    # rising in class = tougher spot
        # Surface/distance switch penalty
       -0.35 * f["surf_switch"] +
       -0.20 * f["dist_switch"]
    )


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 3 — PREDICTION MODELS
# Win probability, Place/Show probability, Fair odds line
# ══════════════════════════════════════════════════════════════════════════════

def softmax_probs(scores):
    max_s = max(scores)
    exp_s = [np.exp(s - max_s) for s in scores]
    total = sum(exp_s)
    return [e / total for e in exp_s]

def harville_place_show(win_probs):
    """
    Harville formula: P(horse i places) and P(horse i shows).
    P(i 2nd) = sum over j!=i of [P(j wins) * P(i wins | j won)]
    """
    n = len(win_probs)
    place_probs = []
    show_probs  = []

    for i in range(n):
        # Place: P(i 1st) + P(i 2nd)
        p_place = win_probs[i]
        p_show  = win_probs[i]

        for j in range(n):
            if j == i: continue
            rem_j = 1 - win_probs[j]
            if rem_j <= 0: continue
            p_i_given_j = win_probs[i] / rem_j
            p_place += win_probs[j] * p_i_given_j

            for k in range(n):
                if k == i or k == j: continue
                rem_jk = 1 - win_probs[j] - win_probs[k]
                if rem_jk <= 0: continue
                p_i_given_jk = win_probs[i] / rem_jk
                p_show += win_probs[j] * (win_probs[k] / rem_j) * p_i_given_jk

        place_probs.append(min(p_place, 0.99))
        show_probs.append(min(p_show, 0.99))

    return place_probs, show_probs

def fair_odds_line(win_prob):
    """Convert win probability to fair decimal odds and fractional."""
    if win_prob <= 0: return "N/A", 999.0
    decimal = 1.0 / win_prob
    num = round(decimal - 1, 1)
    # Express as nearest fractional
    for n, d in [(1,5),(1,4),(1,3),(1,2),(3,5),(4,5),(1,1),(6,5),(7,5),(2,1),
                  (5,2),(3,1),(4,1),(5,1),(6,1),(8,1),(10,1),(12,1),(15,1),(20,1),(30,1)]:
        if abs((n/d) - (decimal-1)) < 0.3:
            return f"{n}/{d}", decimal
    return f"{int(num)}/1", decimal


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 4 — BET EVALUATION
# Compare fair odds vs market, estimate edge, apply bankroll rules,
# filter by liquidity/pool size, select bet type
# ══════════════════════════════════════════════════════════════════════════════

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

def kelly_bet(win_prob, odds, bankroll, frac=KELLY_FRAC, max_bet=MAX_BET):
    dec = odds_to_decimal(odds)
    b   = dec - 1.0
    kf  = max(0, (b * win_prob - (1 - win_prob)) / b) if b > 0 else 0
    return min(kf * frac * bankroll, max_bet), kf

def evaluate_bet(win_prob, place_prob, show_prob, odds, fair_decimal, pool_size=None):
    """
    Diagram node: Bet Evaluation
    Returns recommended bet type and sizing.
    """
    track_p  = odds_to_prob(odds)
    win_edge = win_prob - track_p

    # Filter by liquidity (skip if pool likely too small)
    min_pool = 5000
    if pool_size and pool_size < min_pool:
        return {"type": "SKIP", "reason": "Pool too small", "edge": win_edge}

    # Select bet type based on edge profile
    if win_edge >= MIN_EDGE:
        bet_amt, kf = kelly_bet(win_prob, odds, BANKROLL)
        return {
            "type": "WIN",
            "edge": win_edge,
            "kelly_full": kf,
            "kelly_frac": kf * KELLY_FRAC,
            "bet_amount": bet_amt,
            "fair_odds": fair_decimal,
            "market_odds": odds_to_decimal(odds),
        }
    elif win_edge >= 0.02 and place_prob > 0.45:
        # Low win edge but strong place probability → Place bet
        return {"type": "PLACE", "edge": win_edge, "bet_amount": 500, "note": "Place overlay"}
    elif win_edge < -0.05 and win_prob < 0.05:
        return {"type": "NO BET", "edge": win_edge, "reason": "Negative edge"}
    else:
        return {"type": "WATCH", "edge": win_edge, "reason": "Edge below threshold"}


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 5 — EXECUTION PLAN
# Win bets, Exacta/Trifecta tickets, Multi-race, No bet, Skip race
# ══════════════════════════════════════════════════════════════════════════════

def build_execution_plan(race_results):
    """
    Given model outputs for a race, build the full execution plan.
    Includes win bets + exotic recommendations.
    """
    plan = {"win_bets": [], "exotic_bets": [], "multi_race": [], "action": "NO BET"}

    # Sort by edge
    ranked = sorted(race_results, key=lambda x: x.get("edge", 0), reverse=True)
    top_picks = [r for r in ranked if r.get("bet_type") == "WIN"]

    if not top_picks:
        plan["action"] = "NO BET"
        return plan

    plan["action"] = "BET"
    plan["win_bets"] = top_picks

    # Exacta: box top 2 edge horses
    if len(top_picks) >= 2:
        e1, e2 = top_picks[0], top_picks[1]
        exacta_ev = e1["win_prob"] * (e2["win_prob"] / (1 - e1["win_prob"]))
        plan["exotic_bets"].append({
            "type": "EXACTA BOX",
            "horses": [e1["horse"], e2["horse"]],
            "ev": round(exacta_ev, 3),
            "suggested_amount": 20,
        })

    # Trifecta: key top pick on top, box next 3
    if len(top_picks) >= 1 and len(ranked) >= 4:
        plan["exotic_bets"].append({
            "type": "TRIFECTA KEY",
            "key": top_picks[0]["horse"],
            "with": [r["horse"] for r in ranked[1:4]],
            "suggested_amount": 10,
        })

    return plan


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 6 — POST-RACE REVIEW
# Record actual odds, Record outcome, Measure EV vs ROI, Update model
# ══════════════════════════════════════════════════════════════════════════════

def post_race_review(race_date):
    """
    Diagram node: Post-Race Review
    Loads results, computes EV vs realized ROI, logs for model retraining.
    """
    results = db.query(RaceResult).filter(RaceResult.race_date == race_date).all()
    bets    = db.query(Bet).filter(Bet.race_date == race_date).all()

    if not bets:
        return {"message": "No bets recorded for this date yet."}

    total_staked = sum(b.amount for b in bets)
    total_return = sum(b.payout for b in bets)
    roi = (total_return - total_staked) / total_staked * 100 if total_staked else 0

    # EV = sum(model_prob * payout - stake)
    ev = sum((b.model_prob or 0) * (b.payout or 0) - b.amount for b in bets)

    return {
        "date": race_date,
        "bets": len(bets),
        "total_staked": round(total_staked, 2),
        "total_return": round(total_return, 2),
        "roi_pct": round(roi, 2),
        "expected_value": round(ev, 2),
        "ev_vs_roi": round(ev - (total_return - total_staked), 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_full_pipeline():
    # Find best race date
    race_date = None
    for delta in range(5):
        for sign in [1, -1]:
            d = (date.today() + timedelta(days=delta * sign)).isoformat()
            if db.query(Race).filter(Race.race_date == d).count() > 0:
                race_date = d; break
        if race_date: break

    if not race_date:
        print("No races in database. Run reload_equibase.py first.")
        return

    print(f"\n{'='*90}")
    print(f"  EQUIQUANT AI — FULL PIPELINE RUN")
    print(f"  Analytics Warehouse → Feature Eng → Model → Bet Eval → Execution Plan")
    print(f"  Race date: {race_date}  |  Bankroll: ${BANKROLL:,.0f}  |  Kelly: {KELLY_FRAC*100:.0f}%")
    print(f"{'='*90}")

    races     = db.query(Race).filter(Race.race_date == race_date).order_by(Race.race_number).all()
    all_bets  = []
    daily_risk = 0.0

    for race in races:
        horses = db.query(Horse).filter(
            Horse.race_id == race.id, Horse.scratched == False
        ).order_by(Horse.post_position).all()
        if not horses: continue

        # ── Layer 2: Feature Engineering ─────────────────────────────────────
        features = [engineer_features(h, race, horses) for h in horses]

        # ── Layer 3: Prediction Models ────────────────────────────────────────
        scores      = [score_from_features(f) for f in features]
        win_probs   = softmax_probs(scores)
        place_probs, show_probs = harville_place_show(win_probs)

        # ── Layer 4: Bet Evaluation ───────────────────────────────────────────
        race_results = []
        for h, f, wp, pp, sp in zip(horses, features, win_probs, place_probs, show_probs):
            odds        = h.morning_line_odds or "9/2"
            fair_frac, fair_dec = fair_odds_line(wp)
            evaluation  = evaluate_bet(wp, pp, sp, odds, fair_dec)
            track_p     = odds_to_prob(odds)
            edge        = wp - track_p

            # Save all model outputs to DB
            h.model_win_prob   = round(wp, 4)
            h.model_place_prob = round(pp, 4)
            h.model_show_prob  = round(sp, 4)
            h.edge             = round(edge, 4)
            h.kelly_fraction   = round(evaluation.get("kelly_frac", 0), 4)
            h.kelly_bet_amount = round(evaluation.get("bet_amount", 0), 2)

            race_results.append({
                "horse":      h.horse_name,
                "jockey":     h.jockey,
                "trainer":    h.trainer,
                "pp":         h.post_position,
                "odds":       odds,
                "win_prob":   wp,
                "place_prob": pp,
                "show_prob":  sp,
                "track_prob": track_p,
                "edge":       edge,
                "fair_odds":  fair_frac,
                "bet_type":   evaluation.get("type","NO BET"),
                "bet_amount": evaluation.get("bet_amount", 0),
                "kelly_frac": evaluation.get("kelly_frac", 0),
                "features":   f,
            })

        db.commit()

        # ── Layer 5: Execution Plan ───────────────────────────────────────────
        plan = build_execution_plan(race_results)

        # ── Print Race Report ─────────────────────────────────────────────────
        print(f"\nRACE {race.race_number} — {race.race_name}")
        print(f"  {race.distance} {race.surface} | ${race.purse:,.0f} | {race.condition} | {race.post_time}")
        print(f"\n  {'PP':<4}{'Horse':<26}{'Jockey/Trainer':<24}{'ML':<7}{'Win%':>6}{'Plc%':>6}{'Shw%':>6}{'Fair':>7}{'Edge':>8}{'Action':>12}")
        print(f"  {'-'*95}")

        for r in sorted(race_results, key=lambda x: -x["win_prob"]):
            jt_str = f"{(r['jockey'] or '')[:11]}/{(r['trainer'] or '')[:10]}"
            action = r["bet_type"]
            amt    = f"${r['bet_amount']:,.0f}" if r["bet_amount"] > 0 else ""
            action_str = f"{action} {amt}".strip()

            print(f"  {r['pp']:<4}{r['horse']:<26}{jt_str:<24}{r['odds']:<7}"
                  f"{r['win_prob']*100:>5.1f}%{r['place_prob']*100:>5.1f}%"
                  f"{r['show_prob']*100:>5.1f}%  {r['fair_odds']:>6}"
                  f"  {r['edge']*100:>+5.1f}%  {action_str}")

        # Print execution plan
        if plan["win_bets"]:
            print(f"\n  ★ EXECUTION PLAN:")
            for b in plan["win_bets"]:
                print(f"    WIN  {b['horse']:<26} ${b['bet_amount']:>8,.0f}  "
                      f"(edge: {b['edge']*100:+.1f}%  fair: {b['fair_odds']}  ML: {b['odds']})")
                all_bets.append(b)
                daily_risk += b["bet_amount"]

            for e in plan["exotic_bets"]:
                horses_str = " / ".join(e["horses"]) if "horses" in e else e.get("key","") + " KEY"
                print(f"    {e['type']:<16} {horses_str:<30} ${e['suggested_amount']}")

        else:
            print(f"\n  → No bet — edge below {MIN_EDGE*100:.0f}% threshold")

        # Daily risk cap check
        if daily_risk >= BANKROLL * MAX_RISK_PCT:
            print(f"\n  ⚠ Daily risk cap reached (${daily_risk:,.0f} = {daily_risk/BANKROLL*100:.1f}% of bankroll)")
            print(f"  Remaining races: SKIP")
            break

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*90}")
    print(f"  DAILY EXECUTION SUMMARY — {race_date}")
    print(f"{'='*90}")

    if all_bets:
        print(f"\n  {'Race':<6}{'Horse':<28}{'Odds':<8}{'Model%':>8}{'Edge':>8}{'Bet $':>10}{'Fair Odds':>10}")
        print(f"  {'-'*80}")
        for b in sorted(all_bets, key=lambda x: -x["edge"]):
            print(f"  R{b.get('race',b.get('pp','?')):<5}{b['horse']:<28}{b['odds']:<8}"
                  f"{b['win_prob']*100:>6.1f}%  {b['edge']*100:>+5.1f}%"
                  f"  ${b['bet_amount']:>8,.0f}  {b['fair_odds']:>8}")
        print(f"  {'-'*80}")
        print(f"  Total bets:   {len(all_bets)}")
        print(f"  Total risk:   ${daily_risk:,.0f}")
        print(f"  % bankroll:   {daily_risk/BANKROLL*100:.1f}%")
        print(f"  Expected ROI: {sum(b['edge'] for b in all_bets)/len(all_bets)*100:+.1f}% avg edge")
    else:
        print(f"\n  No bets today — model found no positive edges vs morning line.")
        print(f"  This is expected early on. As live tote odds open and deviate")
        print(f"  from ML, edges will appear. Re-run after pools open (post time -30min).")

    # ── Post-Race Review (if results available) ───────────────────────────────
    review = post_race_review(race_date)
    if review.get("bets"):
        print(f"\n  POST-RACE REVIEW: ROI={review['roi_pct']:+.1f}%  "
              f"EV=${review['expected_value']:,.0f}  "
              f"Return=${review['total_return']:,.0f}")

    print(f"\n  Refresh http://localhost:8000 to see full dashboard")
    db.close()

run_full_pipeline()
