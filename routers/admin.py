"""
Admin Router — triggers full Equibase reload
Uses PT timezone and correct SA{MM}{DD}{YYYY} URL format
"""
from fastapi import APIRouter, BackgroundTasks
import logging
import re
import numpy as np
import pytz
import datetime

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/reload")
async def trigger_reload(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_fresh_reload)
    return {"status": "started", "message": "Fresh Equibase reload running — refresh in 20 seconds"}


@router.post("/runmodel")
async def trigger_model(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_model_sync)
    return {"status": "started", "message": "Model running — refresh in 10 seconds"}


JOCKEY_WIN = {
    "f prat":0.31,"j j hernandez":0.27,"e jaramillo":0.25,"k kimura":0.23,
    "f geroux":0.22,"a ayuso":0.20,"k frey":0.19,"t baze":0.18,
    "t j pereira":0.17,"a fresu":0.16,"v espinoza":0.15,"h i berrios":0.14,
    "c belmont":0.12,"a escobedo":0.11,"r gonzalez":0.10,"f monroy":0.09,
    "c herrera":0.09,"w r orantes":0.08,"a lezcano":0.08,"a aguilar":0.07,
    "v del cid":0.09,"a l bautista":0.08,"m e smith":0.09,
}
TRAINER_WIN = {
    "b baffert":0.32,"p d'amato":0.28,"m w mccarthy":0.26,"p eurton":0.23,
    "r baltas":0.22,"d f o'neill":0.21,"j sadler":0.20,"c a lewis":0.19,
    "c dollase":0.18,"r gomez":0.16,"d dunham":0.15,"v cerin":0.14,
    "d m jensen":0.13,"r w ellis":0.12,"l powell":0.11,"s r knapp":0.10,
    "g vallejo":0.10,"v l garcia":0.09,"a p marquez":0.09,"h o palma":0.08,
    "j ramos":0.08,"l barocio":0.07,"a mathis":0.07,"j j sierra":0.07,
    "g l lopez":0.06,"e g alvarez":0.06,"b mclean":0.06,"j bonde":0.06,
    "m puype":0.05,"g haley":0.08,"s miyadi":0.07,"t yakteen":0.20,
    "n d drysdale":0.14,"n drysdale":0.14,
}
PP_SPRINT=[0.165,0.158,0.148,0.136,0.121,0.106,0.088,0.072,0.058,0.046,0.037,0.028]
PP_ROUTE =[0.118,0.128,0.135,0.138,0.130,0.118,0.102,0.088,0.076,0.064,0.052,0.040]
PP_TURF  =[0.105,0.112,0.118,0.126,0.130,0.126,0.114,0.098,0.085,0.072,0.060,0.048]


def get_today_pt():
    PT = pytz.timezone("America/Los_Angeles")
    return datetime.datetime.now(PT).date()

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
    C={("f geroux","b baffert"):0.12,("j j hernandez","b baffert"):0.10,
       ("a fresu","p d'amato"):0.10,("j j hernandez","m w mccarthy"):0.08,
       ("f geroux","p eurton"):0.08,("e jaramillo","d f o'neill"):0.07}
    for (jk,tk),b in C.items():
        if jk in jl and tk in tl: return b
    return 0.0
def clean(s): return re.sub(r'\s+',' ',str(s or "")).strip()


async def _run_fresh_reload():
    try:
        import aiohttp
        from bs4 import BeautifulSoup
        from database import SessionLocal, Race, Horse, ScraperLog, init_db

        init_db()
        db = SessionLocal()

        # Use PT timezone — Railway runs UTC
        today = get_today_pt()
        mm = str(today.month).zfill(2)
        dd = str(today.day).zfill(2)
        yyyy = today.year
        url = f"https://www.equibase.com/static/entry/SA{mm}{dd}{yyyy}USA-EQB.html"
        race_date = today.isoformat()

        logger.info(f"Admin reload: fetching {url} for {race_date}")

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.equibase.com/",
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    logger.error(f"Admin: Equibase returned {resp.status} for {url}")
                    db.close()
                    return
                html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")
        valid = [t for t in tables if "Horse" in t.get_text() and "Jockey" in t.get_text()]
        logger.info(f"Admin: {len(valid)} entry tables found")

        if not valid:
            logger.error(f"Admin: no entry tables found at {url}")
            db.close()
            return

        # Clear existing data
        for r in db.query(Race).filter(Race.race_date == race_date).all():
            db.query(Horse).filter(Horse.race_id == r.id).delete()
        db.query(Race).filter(Race.race_date == race_date).delete()
        db.query(Horse).delete()
        db.query(Race).delete()
        db.commit()

        races_saved = 0
        total_horses = 0

        for table in tables:
            rows = table.find_all("tr")
            if not rows or len(rows) < 2: continue
            hs = [clean(td.get_text()) for td in rows[0].find_all(["th","td"])]
            if "Horse" not in hs or "Jockey" not in hs: continue

            def c(n):
                for j,h in enumerate(hs):
                    if n.lower() in h.lower(): return j
                return None

            hi=c("Horse"); ji=c("Jockey"); ti=c("Trainer")
            mi=c("M/L"); wi=c("Wgt"); pi=c("PP")
            if hi is None or ji is None or ti is None: continue

            entries = []
            for row in rows[1:]:
                cs = [clean(td.get_text()) for td in row.find_all(["td","th"])]
                if len(cs) < 4: continue
                scr = "scr" in (cs[0] or "").lower()
                hn = re.sub(r'\s*\([A-Z]{2,3}\)\s*$','', cs[hi] if hi<len(cs) else "").strip()
                if not hn or hn == "Horse": continue
                jk = clean(cs[ji]) if ji and ji<len(cs) else ""
                tr = clean(cs[ti]) if ti and ti<len(cs) else ""
                ml = clean(cs[mi]) if mi and mi<len(cs) else "9/2"
                wt_s = clean(cs[wi]) if wi and wi<len(cs) else "122"
                pp_s = clean(cs[pi]) if pi and pi<len(cs) else str(len(entries)+1)
                if not re.match(r'\d+/\d+', ml): ml = "9/2"
                try: wt = int(re.sub(r'\D','', wt_s) or 122)
                except: wt = 122
                try: pp = int(re.sub(r'\D','', pp_s) or len(entries)+1)
                except: pp = len(entries)+1
                entries.append({"pp":pp,"horse":hn,"jockey":jk,"trainer":tr,
                                "ml":ml,"wt":wt,"scr":scr})

            if not entries: continue

            rn = races_saved + 1
            prev = table.find_previous(["div","h2","h3","h4","p"])
            dist="6F"; surf="Dirt"; purse=65000
            if prev:
                txt = prev.get_text(" ", strip=True)
                dm = re.search(r'(\d[\s\d/]*(?:Furlong|Mile|f|m)\w*)', txt, re.I)
                if dm: dist = dm.group(1).strip()
                if "turf" in txt.lower(): surf = "Turf"
                pm = re.search(r'\$(\d{1,3}(?:,\d{3})*)', txt)
                if pm: purse = float(pm.group(1).replace(",",""))

            race = Race(
                race_date=race_date, track="Santa Anita Park",
                race_number=rn, race_name=f"Race {rn} — Santa Anita",
                distance=dist, surface=surf, purse=purse,
                condition="", post_time="", track_condition="Fast", weather="Clear",
            )
            db.add(race); db.flush()

            for e in entries:
                ml_p = op(e["ml"]); byr = round(75+ml_p*80, 1)
                h = Horse(
                    race_id=race.id, race_date=race_date,
                    post_position=e["pp"], horse_name=e["horse"],
                    jockey=e["jockey"], trainer=e["trainer"],
                    morning_line_odds=e["ml"], weight=e["wt"],
                    scratched=e["scr"],
                    beyer_last=byr, beyer_avg_3=round(byr*0.97,1),
                    jockey_win_pct_90d=gs(e["jockey"],JOCKEY_WIN,0.08),
                    trainer_win_pct_90d=gs(e["trainer"],TRAINER_WIN,0.07),
                    days_since_last=28, field_size=len(entries),
                )
                db.add(h); total_horses += 1
            races_saved += 1
            logger.info(f"Admin: Race {rn} — {len(entries)} horses saved")

        db.commit()

        if races_saved == 0:
            logger.error("Admin: 0 races parsed!")
            db.close(); return

        # Run Benter model
        BANKROLL=1000.0; KELLY_FRAC=0.50
        for race in db.query(Race).filter(Race.race_date==race_date).all():
            horses = db.query(Horse).filter(
                Horse.race_id==race.id, Horse.scratched==False
            ).order_by(Horse.post_position).all()
            if not horses: continue
            sc = [4.50*ppb(h.post_position,race.surface,race.distance)+2.80*(h.jockey_win_pct_90d or 0.08)+2.60*(h.trainer_win_pct_90d or 0.07)+0.08*(h.beyer_last or 85)+1.50*jt(h.jockey,h.trainer) for h in horses]
            mx=max(sc); ex=[np.exp(s-mx) for s in sc]; pr=[e/sum(ex) for e in ex]
            for h,p in zip(horses,pr):
                o=h.morning_line_odds or "9/2"; e=p-op(o)
                d=od(o); b=d-1.0; kf=max(0,(b*p-(1-p))/b) if b>0 else 0
                bt=round(kf*KELLY_FRAC*BANKROLL,2) if e>=0.035 else 0
                h.model_win_prob=round(p,4); h.edge=round(e,4)
                h.kelly_fraction=round(kf*KELLY_FRAC,4); h.kelly_bet_amount=bt
        db.commit()

        log = ScraperLog(
            source="admin_equibase", status="success", records=total_horses,
            message=f"{races_saved} races, {total_horses} horses — {race_date} — {url}"
        )
        db.add(log); db.commit(); db.close()
        logger.info(f"Admin reload complete: {races_saved} races, {total_horses} horses for {race_date}")

    except Exception as e:
        logger.error(f"Admin reload error: {e}")
        import traceback; logger.error(traceback.format_exc())


def _run_model_sync():
    try:
        from full_model import run_full_pipeline
        run_full_pipeline()
    except Exception as e:
        logger.error(f"Model error: {e}")
