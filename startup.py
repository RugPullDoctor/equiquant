"""
EquiQuant — Force Reseed Startup
ALWAYS clears and reloads from Equibase on boot.
"""
import logging, numpy as np, re, aiohttp
from bs4 import BeautifulSoup
from datetime import date

logger = logging.getLogger(__name__)

JOCKEY_WIN = {
    "f prat":0.31,"j j hernandez":0.27,"e jaramillo":0.25,"k kimura":0.23,
    "f geroux":0.22,"a ayuso":0.20,"k frey":0.19,"t baze":0.18,
    "t j pereira":0.17,"a fresu":0.16,"v espinoza":0.15,"h i berrios":0.14,
    "c belmont":0.12,"a escobedo":0.11,"r gonzalez":0.10,"f monroy":0.09,
    "c herrera":0.09,"w r orantes":0.08,"a lezcano":0.08,"a aguilar":0.07,
    "v del cid":0.09,"a l bautista":0.08,"j rosario":0.25,
}
TRAINER_WIN = {
    "b baffert":0.32,"p d'amato":0.28,"m w mccarthy":0.26,"p eurton":0.23,
    "r baltas":0.22,"d f o'neill":0.21,"j sadler":0.20,"c a lewis":0.19,
    "c dollase":0.18,"r gomez":0.16,"d dunham":0.15,"v cerin":0.14,
    "d m jensen":0.13,"r w ellis":0.12,"l powell":0.11,"s r knapp":0.10,
    "g vallejo":0.10,"v l garcia":0.09,"a p marquez":0.09,"h o palma":0.08,
    "j ramos":0.08,"l barocio":0.07,"a mathis":0.07,"j j sierra":0.07,
    "g l lopez":0.06,"e g alvarez":0.06,"b mclean":0.06,"j bonde":0.06,
    "m puype":0.05,"g haley":0.08,"s miyadi":0.07,"t yakteen":0.20,"n howard":0.14,
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
    C={("f geroux","b baffert"):0.12,("j j hernandez","b baffert"):0.10,
       ("a fresu","p d'amato"):0.10,("j j hernandez","m w mccarthy"):0.08,
       ("f geroux","p eurton"):0.08,("e jaramillo","d f o'neill"):0.07}
    for (jk,tk),b in C.items():
        if jk in jl and tk in tl: return b
    return 0.0
def clean(s): return re.sub(r'\s+',' ',str(s or "")).strip()


async def startup_load():
    """Always wipes and reloads from Equibase."""
    logger.info("Startup: fetching fresh Equibase data...")
    from database import SessionLocal, Race, Horse, ScraperLog, init_db
    init_db()
    db = SessionLocal()
    today = date.today()
    mm = str(today.month).zfill(2)
    url = f"https://www.equibase.com/static/entry/SA{mm}{dd}{today.year}USA-EQB.html"
    race_date = today.isoformat()
    hdrs = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36","Referer":"https://www.equibase.com/"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url,headers=hdrs,timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    logger.warning(f"Equibase {resp.status} — skipping")
                    db.close(); return
                html = await resp.text()
    except Exception as e:
        logger.error(f"Startup fetch error: {e}"); db.close(); return

    soup = BeautifulSoup(html,"html.parser")
    tables = soup.find_all("table")
    logger.info(f"Startup: {len(tables)} tables, parsing entries...")

    # ALWAYS clear everything first
    db.query(Horse).delete()
    db.query(Race).delete()
    db.commit()

    races_saved=0; total_horses=0

    for table in tables:
        rows=table.find_all("tr")
        if not rows or len(rows)<2: continue
        hs=[clean(td.get_text()) for td in rows[0].find_all(["th","td"])]
        if "Horse" not in hs or "Jockey" not in hs: continue
        def c(n):
            for j,h in enumerate(hs):
                if n.lower() in h.lower(): return j
            return None
        hi=c("Horse"); ji=c("Jockey"); ti=c("Trainer")
        mi=c("M/L"); wi=c("Wgt"); pi=c("PP")
        if hi is None or ji is None or ti is None: continue
        entries=[]
        for row in rows[1:]:
            cs=[clean(td.get_text()) for td in row.find_all(["td","th"])]
            if len(cs)<4: continue
            scr="scr" in (cs[0] or "").lower()
            hn=re.sub(r'\s*\([A-Z]{2,3}\)\s*$','',cs[hi] if hi<len(cs) else "").strip()
            if not hn or hn=="Horse": continue
            jk=clean(cs[ji]) if ji and ji<len(cs) else ""
            tr=clean(cs[ti]) if ti and ti<len(cs) else ""
            ml=clean(cs[mi]) if mi and mi<len(cs) else "9/2"
            wt_s=clean(cs[wi]) if wi and wi<len(cs) else "122"
            pp_s=clean(cs[pi]) if pi and pi<len(cs) else str(len(entries)+1)
            if not re.match(r'\d+/\d+',ml): ml="9/2"
            try: wt=int(re.sub(r'\D','',wt_s) or 122)
            except: wt=122
            try: pp=int(re.sub(r'\D','',pp_s) or len(entries)+1)
            except: pp=len(entries)+1
            entries.append({"pp":pp,"horse":hn,"jockey":jk,"trainer":tr,"ml":ml,"wt":wt,"scr":scr})
        if not entries: continue
        rn=races_saved+1
        prev=table.find_previous(["div","h2","h3","h4","p"])
        dist="6F"; surf="Dirt"; purse=65000
        if prev:
            txt=prev.get_text(" ",strip=True)
            dm=re.search(r'(\d[\s\d/]*(?:Furlong|Mile|f|m)\w*)',txt,re.I)
            if dm: dist=dm.group(1).strip()
            if "turf" in txt.lower(): surf="Turf"
            pm=re.search(r'\$(\d{1,3}(?:,\d{3})*)',txt)
            if pm: purse=float(pm.group(1).replace(",",""))
        race=Race(race_date=race_date,track="Santa Anita Park",
                  race_number=rn,race_name=f"Race {rn} — Santa Anita",
                  distance=dist,surface=surf,purse=purse,
                  condition="",post_time="",track_condition="Fast",weather="Clear")
        db.add(race); db.flush()
        for e in entries:
            ml_p=op(e["ml"]); byr=round(75+ml_p*80,1)
            h=Horse(race_id=race.id,race_date=race_date,
                    post_position=e["pp"],horse_name=e["horse"],
                    jockey=e["jockey"],trainer=e["trainer"],
                    morning_line_odds=e["ml"],weight=e["wt"],
                    scratched=e["scr"],
                    beyer_last=byr,beyer_avg_3=round(byr*0.97,1),
                    jockey_win_pct_90d=gs(e["jockey"],JOCKEY_WIN,0.08),
                    trainer_win_pct_90d=gs(e["trainer"],TRAINER_WIN,0.07),
                    days_since_last=28,field_size=len(entries))
            db.add(h); total_horses+=1
        races_saved+=1
        logger.info(f"Startup: Race {rn} — {len(entries)} horses")

    db.commit()
    if races_saved==0:
        logger.warning("Startup: 0 races parsed!"); db.close(); return

    # Run model
    BANKROLL=1000.0; KELLY_FRAC=0.50
    for race in db.query(Race).filter(Race.race_date==race_date).all():
        horses=db.query(Horse).filter(Horse.race_id==race.id,Horse.scratched==False).order_by(Horse.post_position).all()
        if not horses: continue
        sc=[4.50*ppb(h.post_position,race.surface,race.distance)+2.80*(h.jockey_win_pct_90d or 0.08)+2.60*(h.trainer_win_pct_90d or 0.07)+0.08*(h.beyer_last or 85)+1.50*jt(h.jockey,h.trainer) for h in horses]
        mx=max(sc); ex=[np.exp(s-mx) for s in sc]; pr=[e/sum(ex) for e in ex]
        for h,p in zip(horses,pr):
            o=h.morning_line_odds or "9/2"; e=p-op(o)
            d=od(o); b=d-1.0; kf=max(0,(b*p-(1-p))/b) if b>0 else 0
            bt=round(kf*KELLY_FRAC*BANKROLL,2) if e>=0.035 else 0
            h.model_win_prob=round(p,4); h.edge=round(e,4)
            h.kelly_fraction=round(kf*KELLY_FRAC,4); h.kelly_bet_amount=bt
    db.commit()

    log=ScraperLog(source="startup_equibase",status="success",records=total_horses,
                   message=f"{races_saved} races, {total_horses} horses — {race_date}")
    db.add(log); db.commit(); db.close()
    logger.info(f"Startup done: {races_saved} races, {total_horses} horses for {race_date}")
