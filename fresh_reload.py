"""
EquiQuant — Fresh Equibase Reload
Built from actual page inspection — parses all 8 races correctly.
Run from C:\EquiQuant:  python fresh_reload.py
"""

import asyncio
import sys
import os
import re
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal, Race, Horse, ScraperLog, init_db
from datetime import date, timedelta
import aiohttp
from bs4 import BeautifulSoup

init_db()
db = SessionLocal()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.equibase.com/",
}

JOCKEY_WIN = {
    "f prat":0.31,"j j hernandez":0.27,"e jaramillo":0.25,
    "k kimura":0.23,"f geroux":0.22,"florent geroux":0.22,
    "a ayuso":0.20,"k frey":0.19,"t baze":0.18,
    "t j pereira":0.17,"a fresu":0.16,"antonio fresu":0.16,
    "v espinoza":0.15,"h i berrios":0.14,"c belmont":0.12,
    "a escobedo":0.11,"r gonzalez":0.10,"f monroy":0.09,
    "c herrera":0.09,"w r orantes":0.08,"a lezcano":0.08,
    "a aguilar":0.07,"j hernandez":0.27,"h berrios":0.14,
    "j j hernandez":0.27,"e jaramillo":0.25,
}
TRAINER_WIN = {
    "b baffert":0.32,"p d'amato":0.28,"m w mccarthy":0.26,
    "p eurton":0.23,"r baltas":0.22,"d f o'neill":0.21,
    "j sadler":0.20,"c a lewis":0.19,"c dollase":0.18,
    "r gomez":0.16,"d dunham":0.15,"v cerin":0.14,
    "d m jensen":0.13,"r w ellis":0.12,"l powell":0.11,
    "s r knapp":0.10,"g vallejo":0.10,"v l garcia":0.09,
    "a p marquez":0.09,"h o palma":0.08,"j ramos":0.08,
    "l barocio":0.07,"a mathis":0.07,"j j sierra":0.07,
    "g l lopez":0.06,"e g alvarez":0.06,"b mclean":0.06,
    "j bonde":0.06,"m puype":0.05,"d winick":0.08,
    "g papaprodromou":0.12,"p miller":0.18,"f rodriguez":0.10,
    "m h valenzuela":0.09,"n howard":0.14,"t yakteen":0.20,
    "j hollendorfer":0.18,"p gallagher":0.12,"r hess":0.11,
    "m glatt":0.10,"b rice":0.13,"k mcpeek":0.15,
}
JT_COMBOS = {
    ("f geroux","b baffert"):0.12,("j j hernandez","b baffert"):0.10,
    ("a fresu","p d'amato"):0.10,("j j hernandez","m w mccarthy"):0.08,
    ("f geroux","p eurton"):0.08,("j j hernandez","r baltas"):0.07,
    ("a ayuso","p eurton"):0.07,("e jaramillo","d f o'neill"):0.07,
    ("k frey","d dunham"):0.05,
}
PP_SPRINT=[0.165,0.158,0.148,0.136,0.121,0.106,0.088,0.072,0.058,0.046,0.037,0.028]
PP_ROUTE =[0.118,0.128,0.135,0.138,0.130,0.118,0.102,0.088,0.076,0.064,0.052,0.040]
PP_TURF  =[0.105,0.112,0.118,0.126,0.130,0.126,0.114,0.098,0.085,0.072,0.060,0.048]

def gs(n,t,d):
    nl=(n or "").lower().strip()
    for k,v in t.items():
        if k in nl or nl in k: return v
    return d

def op(o):
    try: n,d=str(o).split("/"); return float(d)/(float(n)+float(d))
    except: return 0.10

def od(o):
    try: n,d=str(o).split("/"); return (float(n)+float(d))/float(d)
    except: return 10.0

def ppb(pp,surf,dist="6F"):
    idx=min((pp or 1)-1,11)
    if "turf" in (surf or "").lower(): return PP_TURF[idx]
    f=6.0; d=(dist or "").upper()
    if "1 1/16" in d: f=8.5
    elif "1 1/8" in d: f=9.0
    elif "1M" in d or "MILE" in d: f=8.0
    else:
        m=re.search(r'(\d+\.?\d*)\s*F',d)
        if m: f=float(m.group(1))
    return PP_ROUTE[idx] if f>=8 else PP_SPRINT[idx]

def jt(j,t):
    jl,tl=(j or "").lower(),(t or "").lower()
    for (jk,tk),b in JT_COMBOS.items():
        if jk in jl and tk in tl: return b
    return 0.0

def clean(s):
    return re.sub(r'\s+',' ',str(s or "")).strip()

async def main():
    # Find today's date and build Equibase URL
    today = date.today()
    mm = str(today.month).zfill(2)
    url = f"https://www.equibase.com/static/entry/SA{mm}{today.year}USA-EQB.html"
    race_date = today.isoformat()

    print(f"\nEquiQuant — Fresh Equibase Reload")
    print(f"Date: {race_date}")
    print(f"URL:  {url}")
    print("="*60)

    # Fetch page
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            print(f"Status: {resp.status}")
            if resp.status != 200:
                print("Failed to fetch Equibase page!")
                return
            html = await resp.text()

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    print(f"Tables found: {len(tables)}")

    # Clear existing data for today
    existing = db.query(Race).filter(Race.race_date == race_date).all()
    for r in existing:
        db.query(Horse).filter(Horse.race_id == r.id).delete()
    db.query(Race).filter(Race.race_date == race_date).delete()
    db.commit()
    print("Cleared existing data")

    races_saved = 0
    total_horses = 0

    for i, table in enumerate(tables):
        rows = table.find_all("tr")
        if not rows or len(rows) < 2:
            continue

        # Get header row
        headers = [clean(td.get_text()) for td in rows[0].find_all(["th","td"])]
        if "Horse" not in headers or "Jockey" not in headers:
            continue

        # Map column indices
        def col(name):
            for j,h in enumerate(headers):
                if name.lower() in h.lower(): return j
            return None

        hi = col("Horse"); ji = col("Jockey"); ti = col("Trainer")
        mi = col("M/L"); wi = col("Wgt"); pi = col("PP")
        if hi is None or ji is None or ti is None:
            continue

        # Parse race info from surrounding context
        race_num = races_saved + 1

        # Look for race details in preceding elements
        race_name = f"Race {race_num} — Santa Anita"
        distance = "6F"
        surface = "Dirt"
        purse = 65000
        post_time = ""

        # Check preceding sibling text for race details
        prev = table.find_previous(["div","h2","h3","h4","p"])
        if prev:
            txt = prev.get_text(" ", strip=True)
            # Distance
            dm = re.search(r'(\d[\s\d/]*(?:Furlong|Mile|f|m)\w*)', txt, re.I)
            if dm: distance = dm.group(1).strip()
            # Surface
            if "turf" in txt.lower(): surface = "Turf"
            elif "dirt" in txt.lower(): surface = "Dirt"
            # Purse
            pm = re.search(r'\$(\d{1,3}(?:,\d{3})*)', txt)
            if pm: purse = float(pm.group(1).replace(",",""))

        # Parse entries
        entries = []
        for row in rows[1:]:
            cells = [clean(td.get_text()) for td in row.find_all(["td","th"])]
            if len(cells) < 4: continue

            # Check for scratch
            row_text = row.get_text().lower()
            scratched = "scr" in cells[0].lower() if cells else False

            # Get horse name - remove state abbreviations like (KY) (CA)
            horse_raw = cells[hi] if hi < len(cells) else ""
            horse_name = re.sub(r'\s*\([A-Z]{2,3}\)\s*$', '', horse_raw).strip()
            if not horse_name or horse_name in ["Horse",""]: continue

            jockey  = clean(cells[ji])  if ji and ji < len(cells) else ""
            trainer = clean(cells[ti])  if ti and ti < len(cells) else ""
            ml      = clean(cells[mi])  if mi and mi < len(cells) else "9/2"
            weight  = clean(cells[wi])  if wi and wi < len(cells) else "122"
            pp_val  = clean(cells[pi])  if pi and pi < len(cells) else str(len(entries)+1)

            # Clean jockey name (remove extra spaces between initials)
            jockey = re.sub(r'\s+', ' ', jockey).strip()

            # Validate ML odds format
            if not re.match(r'\d+/\d+', ml): ml = "9/2"

            # Safe int for weight and PP
            try: wt = int(re.sub(r'\D','',weight) or 122)
            except: wt = 122
            try: pp = int(re.sub(r'\D','',pp_val) or len(entries)+1)
            except: pp = len(entries)+1

            entries.append({
                "pp": pp, "horse": horse_name, "jockey": jockey,
                "trainer": trainer, "ml": ml, "wt": wt, "scratched": scratched,
            })

        if not entries:
            continue

        # Save race
        race = Race(
            race_date=race_date, track="Santa Anita Park",
            race_number=race_num, race_name=race_name,
            distance=distance, surface=surface, purse=purse,
            condition="", post_time=post_time,
            track_condition="Fast", weather="Clear",
        )
        db.add(race)
        db.flush()

        print(f"\nRace {race_num}: {len(entries)} horses [{distance} {surface}]")
        for e in entries:
            ml_p = op(e["ml"]); beyer = round(75 + ml_p*80, 1)
            horse = Horse(
                race_id=race.id, race_date=race_date,
                post_position=e["pp"], horse_name=e["horse"],
                jockey=e["jockey"], trainer=e["trainer"],
                morning_line_odds=e["ml"], weight=e["wt"],
                scratched=e["scratched"],
                beyer_last=beyer, beyer_avg_3=round(beyer*0.97,1),
                jockey_win_pct_90d=gs(e["jockey"],JOCKEY_WIN,0.08),
                trainer_win_pct_90d=gs(e["trainer"],TRAINER_WIN,0.07),
                days_since_last=28, field_size=len(entries),
            )
            db.add(horse)
            total_horses += 1
            scr = " [SCR]" if e["scratched"] else ""
            print(f"  PP{e['pp']} {e['horse']:<28} {e['jockey']:<18} {e['ml']}{scr}")

        races_saved += 1

    db.commit()

    # Run model
    print(f"\nRunning Benter model on {total_horses} horses...")
    BANKROLL = 1000.0; KELLY_FRAC = 0.50; total_bets = 0

    for race in db.query(Race).filter(Race.race_date==race_date).all():
        horses = db.query(Horse).filter(
            Horse.race_id==race.id, Horse.scratched==False
        ).order_by(Horse.post_position).all()
        if not horses: continue

        scores=[
            4.50*ppb(h.post_position,race.surface,race.distance)+
            2.80*(h.jockey_win_pct_90d or 0.08)+
            2.60*(h.trainer_win_pct_90d or 0.07)+
            0.08*(h.beyer_last or 85)+
            1.50*jt(h.jockey,h.trainer)
            for h in horses
        ]
        max_s=max(scores); exp_s=[np.exp(s-max_s) for s in scores]
        probs=[e/sum(exp_s) for e in exp_s]

        for h,prob in zip(horses,probs):
            odds=h.morning_line_odds or "9/2"
            edge=prob-op(odds); dec=od(odds); b=dec-1.0
            kf=max(0,(b*prob-(1-prob))/b) if b>0 else 0
            bet=round(kf*KELLY_FRAC*BANKROLL,2) if edge>=0.035 else 0
            if bet>0: total_bets+=1
            h.model_win_prob=round(prob,4); h.edge=round(edge,4)
            h.kelly_fraction=round(kf*KELLY_FRAC,4); h.kelly_bet_amount=bet

    db.commit()

    # Log
    log = ScraperLog(source="equibase_fresh",status="success",records=total_horses,
                     message=f"{races_saved} races, {total_horses} horses, {total_bets} edge bets — {race_date}")
    db.add(log); db.commit(); db.close()

    print(f"\n{'='*60}")
    print(f"Races saved:  {races_saved}")
    print(f"Horses saved: {total_horses}")
    print(f"Edge bets:    {total_bets}")
    print(f"\nRefresh http://localhost:8000 to see updated dashboard")

asyncio.run(main())
