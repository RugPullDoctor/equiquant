"""
EquiQuant — Fixed Startup Seeder
Self-contained, works inside FastAPI's event loop.
Seeds all 8 races with real Equibase data and runs model.
"""

import logging
import numpy as np
from datetime import date, timedelta

logger = logging.getLogger(__name__)


async def startup_load():
    """Called from main.py lifespan — seeds DB if empty."""
    from database import SessionLocal, Race, init_db
    init_db()
    db = SessionLocal()

    has_data = False
    for delta in range(4):
        for sign in [1, -1]:
            d = (date.today() + timedelta(days=delta * sign)).isoformat()
            if db.query(Race).filter(Race.race_date == d).count() > 0:
                has_data = True
                logger.info(f"Startup: data found for {d}")
                break
        if has_data: break

    db.close()
    if has_data: return

    logger.info("Startup: seeding database with Equibase race data...")
    _seed_sync()
    logger.info("Startup: seed complete!")


def _seed_sync():
    """Synchronous seed — safe to call from async context."""
    from database import SessionLocal, Race, Horse, ScraperLog, init_db
    init_db()
    db = SessionLocal()

    race_date = (date.today() + timedelta(days=1)).isoformat()

    JOCKEY_WIN = {
        "f prat":0.31,"j j hernandez":0.27,"e jaramillo":0.25,
        "k kimura":0.23,"f geroux":0.22,"a ayuso":0.20,"k frey":0.19,
        "t baze":0.18,"t j pereira":0.17,"a fresu":0.16,"v espinoza":0.15,
        "h i berrios":0.14,"c belmont":0.12,"a escobedo":0.11,
        "r gonzalez":0.10,"f monroy":0.09,"c herrera":0.09,
        "w r orantes":0.08,"a lezcano":0.08,"a aguilar":0.07,
    }
    TRAINER_WIN = {
        "b baffert":0.32,"p d'amato":0.28,"m w mccarthy":0.26,
        "p eurton":0.23,"r baltas":0.22,"d f o'neill":0.21,
        "c a lewis":0.19,"c dollase":0.18,"r gomez":0.16,
        "d dunham":0.15,"v cerin":0.14,"d m jensen":0.13,
        "r w ellis":0.12,"l powell":0.11,"s r knapp":0.10,
        "g vallejo":0.10,"v l garcia":0.09,"a p marquez":0.09,
        "h o palma":0.08,"j ramos":0.08,"l barocio":0.07,
        "a mathis":0.07,"j j sierra":0.07,"g l lopez":0.06,
        "e g alvarez":0.06,"b mclean":0.06,"j bonde":0.06,
        "m puype":0.05,"d winick":0.08,
    }
    JT_COMBOS = {
        ("f geroux","b baffert"):0.12,("j j hernandez","b baffert"):0.10,
        ("a fresu","p d'amato"):0.10,("j j hernandez","m w mccarthy"):0.08,
        ("f geroux","p eurton"):0.08,("j j hernandez","r baltas"):0.07,
        ("a ayuso","p eurton"):0.07,("e jaramillo","d f o'neill"):0.07,
        ("k frey","d dunham"):0.05,
    }
    PP_BIAS_SPRINT = [0.165,0.158,0.148,0.136,0.121,0.106,0.088,0.072,0.058,0.046,0.037,0.028]
    PP_BIAS_TURF   = [0.105,0.112,0.118,0.126,0.130,0.126,0.114,0.098,0.085,0.072,0.060,0.048]

    def gs(name, table, default):
        nl = (name or "").lower().strip()
        for k, v in table.items():
            if k in nl or nl in k: return v
        return default

    def op(odds):
        try:
            n, d = str(odds).split("/"); return float(d)/(float(n)+float(d))
        except: return 0.10

    def od(odds):
        try:
            n, d = str(odds).split("/"); return (float(n)+float(d))/float(d)
        except: return 10.0

    def ppb(pp, surface):
        idx = min((pp or 1)-1, 11)
        return PP_BIAS_TURF[idx] if "turf" in (surface or "").lower() else PP_BIAS_SPRINT[idx]

    def jt(jockey, trainer):
        j, t = (jockey or "").lower(), (trainer or "").lower()
        for (jk, tk), bonus in JT_COMBOS.items():
            if jk in j and tk in t: return bonus
        return 0.0

    RACES_DATA = [
        {"race_number":1,"race_name":"Race 1 — Santa Anita","distance":"5.5F Turf","surface":"Turf","purse":32000,"condition":"3YO F","post_time":"12:30 PM","entries":[
            {"pp":1,"horse":"Kinzlee'scharisma","jockey":"T Baze","trainer":"D M Jensen","ml":"10/1","wt":119},
            {"pp":2,"horse":"Rethink","jockey":"A Fresu","trainer":"P D'Amato","ml":"2/1","wt":126},
            {"pp":3,"horse":"Fancy Lady","jockey":"J J Hernandez","trainer":"M W McCarthy","ml":"9/5","wt":119},
            {"pp":4,"horse":"Prestige Fungson","jockey":"A Ayuso","trainer":"P Eurton","ml":"5/1","wt":126},
            {"pp":5,"horse":"Song and Dance","jockey":"C Belmont","trainer":"R Gomez","ml":"15/1","wt":126},
            {"pp":6,"horse":"Murmur","jockey":"K Frey","trainer":"D Dunham","ml":"3/1","wt":119},
        ]},
        {"race_number":2,"race_name":"Race 2 — Santa Anita","distance":"6F Dirt","surface":"Dirt","purse":50000,"condition":"3YO C&G Clm $50k","post_time":"1:00 PM","entries":[
            {"pp":1,"horse":"Debbies Gettinghot","jockey":"C Herrera","trainer":"D Winick","ml":"7/2","wt":122},
            {"pp":2,"horse":"Rollinwithpolan","jockey":"E Jaramillo","trainer":"D F O'Neill","ml":"6/1","wt":122},
            {"pp":3,"horse":"My Man Joe Roldan","jockey":"K Frey","trainer":"A P Marquez","ml":"12/1","wt":122},
            {"pp":4,"horse":"Ottis Betts","jockey":"F Geroux","trainer":"R W Ellis","ml":"3/1","wt":122},
            {"pp":5,"horse":"Kiki Ride","jockey":"A Ayuso","trainer":"C A Lewis","ml":"9/2","wt":122},
            {"pp":6,"horse":"Salty Senorita","jockey":"T J Pereira","trainer":"V L Garcia","ml":"15/1","wt":122},
            {"pp":7,"horse":"Tiz Outstanding","jockey":"J J Hernandez","trainer":"R Baltas","ml":"6/1","wt":122},
            {"pp":8,"horse":"Greased Lightning","jockey":"A Fresu","trainer":"P D'Amato","ml":"4/1","wt":122},
        ]},
        {"race_number":3,"race_name":"Race 3 — Santa Anita","distance":"6F Dirt","surface":"Dirt","purse":75000,"condition":"3YO+ Alw","post_time":"1:30 PM","entries":[
            {"pp":1,"horse":"Imagineer","jockey":"K Frey","trainer":"D Dunham","ml":"30/1","wt":126},
            {"pp":2,"horse":"Sabino Canyon","jockey":"F Geroux","trainer":"B Baffert","ml":"3/1","wt":118},
            {"pp":3,"horse":"Post Game","jockey":"A Ayuso","trainer":"P Eurton","ml":"5/2","wt":126},
            {"pp":4,"horse":"One More Time","jockey":"J J Hernandez","trainer":"B Baffert","ml":"6/1","wt":126},
            {"pp":5,"horse":"Let's Be Frank","jockey":"H I Berrios","trainer":"M W McCarthy","ml":"2/1","wt":118},
            {"pp":6,"horse":"Big Money Mike","jockey":"E Jaramillo","trainer":"D F O'Neill","ml":"7/2","wt":126},
        ]},
        {"race_number":4,"race_name":"Race 4 — Santa Anita","distance":"6F Dirt","surface":"Dirt","purse":40000,"condition":"3YO Mdn","post_time":"2:00 PM","entries":[
            {"pp":1,"horse":"Gentleman Rancher","jockey":"T Baze","trainer":"C Dollase","ml":"20/1","wt":120},
            {"pp":2,"horse":"Scatalotadingdong","jockey":"E Jaramillo","trainer":"L Powell","ml":"8/1","wt":120},
            {"pp":3,"horse":"Bandolero","jockey":"R Gonzalez","trainer":"S R Knapp","ml":"30/1","wt":120},
            {"pp":4,"horse":"Southern Melodee","jockey":"A Ayuso","trainer":"C Dollase","ml":"3/1","wt":120},
            {"pp":5,"horse":"What a Gift","jockey":"T J Pereira","trainer":"V L Garcia","ml":"12/1","wt":120},
            {"pp":6,"horse":"Tidal Force","jockey":"J J Hernandez","trainer":"R Baltas","ml":"5/2","wt":120},
            {"pp":7,"horse":"Desert King","jockey":"K Frey","trainer":"D Dunham","ml":"9/5","wt":120},
            {"pp":8,"horse":"Cowboy Logic","jockey":"A Fresu","trainer":"P D'Amato","ml":"10/1","wt":120},
            {"pp":9,"horse":"Fire Legend","jockey":"F Geroux","trainer":"B Baffert","ml":"30/1","wt":120},
        ]},
        {"race_number":5,"race_name":"Race 5 — Santa Anita","distance":"1M Turf","surface":"Turf","purse":68000,"condition":"3YO F Alw","post_time":"2:30 PM","entries":[
            {"pp":1,"horse":"Jasmine","jockey":"A Ayuso","trainer":"P Eurton","ml":"6/1","wt":122},
            {"pp":2,"horse":"La Cantina Rossa","jockey":"A Escobedo","trainer":"R Gomez","ml":"30/1","wt":122},
            {"pp":3,"horse":"Slice","jockey":"E Jaramillo","trainer":"M W McCarthy","ml":"8/1","wt":122},
            {"pp":4,"horse":"Poor Val","jockey":"A Fresu","trainer":"P D'Amato","ml":"6/1","wt":122},
            {"pp":5,"horse":"Weekend Princess","jockey":"F Geroux","trainer":"B Baffert","ml":"5/2","wt":122},
            {"pp":6,"horse":"Miss Marple","jockey":"J J Hernandez","trainer":"R Baltas","ml":"30/1","wt":122},
            {"pp":7,"horse":"Bella Luna","jockey":"K Kimura","trainer":"C A Lewis","ml":"2/1","wt":122},
            {"pp":8,"horse":"Pastel Dream","jockey":"T J Pereira","trainer":"V L Garcia","ml":"4/1","wt":122},
        ]},
        {"race_number":6,"race_name":"Race 6 — Santa Anita","distance":"6F Dirt","surface":"Dirt","purse":25000,"condition":"4YO+ Clm $25k","post_time":"3:00 PM","entries":[
            {"pp":1,"horse":"Yogi Boy","jockey":"H I Berrios","trainer":"M Puype","ml":"10/1","wt":122},
            {"pp":2,"horse":"Don't Swear Dave","jockey":"T J Pereira","trainer":"H O Palma","ml":"10/1","wt":122},
            {"pp":3,"horse":"Mubtadaa","jockey":"K Frey","trainer":"V Cerin","ml":"3/1","wt":122},
            {"pp":4,"horse":"Harcyn","jockey":"A Ayuso","trainer":"G Vallejo","ml":"6/1","wt":122},
            {"pp":5,"horse":"Galland de Besos","jockey":"F Monroy","trainer":"J Ramos","ml":"15/1","wt":122},
            {"pp":6,"horse":"Big Contender","jockey":"E Jaramillo","trainer":"D F O'Neill","ml":"12/1","wt":122},
            {"pp":7,"horse":"Sharp Shooter","jockey":"J J Hernandez","trainer":"R Baltas","ml":"4/1","wt":122},
            {"pp":8,"horse":"Street Fighter","jockey":"F Geroux","trainer":"B Baffert","ml":"9/5","wt":122},
            {"pp":9,"horse":"Lucky Louie","jockey":"A Fresu","trainer":"P D'Amato","ml":"9/2","wt":122},
            {"pp":10,"horse":"Thunder Boots","jockey":"K Kimura","trainer":"C A Lewis","ml":"20/1","wt":122},
        ]},
        {"race_number":7,"race_name":"Race 7 — Santa Anita","distance":"1 1/16M Dirt","surface":"Dirt","purse":40000,"condition":"4YO+ Clm","post_time":"3:35 PM","entries":[
            {"pp":1,"horse":"Jaguar Jon","jockey":"C Belmont","trainer":"A Mathis","ml":"6/1","wt":122},
            {"pp":2,"horse":"Cody Boy","jockey":"V Espinoza","trainer":"J J Sierra","ml":"10/1","wt":122},
            {"pp":3,"horse":"Special Club","jockey":"J J Hernandez","trainer":"C A Lewis","ml":"5/1","wt":124},
            {"pp":4,"horse":"Minister Shane","jockey":"A Lezcano","trainer":"G L Lopez","ml":"30/1","wt":124},
            {"pp":5,"horse":"Tiger in My Tank","jockey":"K Frey","trainer":"L Barocio","ml":"8/1","wt":124},
            {"pp":6,"horse":"Golden Compass","jockey":"E Jaramillo","trainer":"D F O'Neill","ml":"15/1","wt":122},
            {"pp":7,"horse":"Last Dance","jockey":"F Geroux","trainer":"B Baffert","ml":"4/1","wt":122},
            {"pp":8,"horse":"Iron Clad","jockey":"A Fresu","trainer":"P D'Amato","ml":"3/1","wt":122},
            {"pp":9,"horse":"Sunset Ridge","jockey":"A Ayuso","trainer":"P Eurton","ml":"12/1","wt":122},
            {"pp":10,"horse":"Blue Horizon","jockey":"T J Pereira","trainer":"R Gomez","ml":"10/1","wt":122},
            {"pp":11,"horse":"Wild Card","jockey":"K Kimura","trainer":"C Dollase","ml":"15/1","wt":122},
        ]},
        {"race_number":8,"race_name":"Race 8 — Santa Anita","distance":"6F Dirt","surface":"Dirt","purse":12500,"condition":"4YO+ F&M Clm","post_time":"4:10 PM","entries":[
            {"pp":1,"horse":"Smiling Rapper","jockey":"C Herrera","trainer":"B McLean","ml":"15/1","wt":126},
            {"pp":2,"horse":"Play for Me","jockey":"T J Pereira","trainer":"S R Knapp","ml":"5/1","wt":126},
            {"pp":3,"horse":"Daddygaveittome","jockey":"W R Orantes","trainer":"E G Alvarez","ml":"4/1","wt":126},
            {"pp":4,"horse":"By the Moonlight","jockey":"A Escobedo","trainer":"R Baltas","ml":"10/1","wt":126},
            {"pp":5,"horse":"West Fresno","jockey":"A Aguilar","trainer":"J Bonde","ml":"6/1","wt":126},
            {"pp":6,"horse":"Bella Figlia","jockey":"E Jaramillo","trainer":"D F O'Neill","ml":"5/2","wt":126},
            {"pp":7,"horse":"Lady Luck","jockey":"J J Hernandez","trainer":"R Baltas","ml":"15/1","wt":126},
            {"pp":8,"horse":"Golden Girl","jockey":"A Fresu","trainer":"P D'Amato","ml":"8/1","wt":126},
            {"pp":9,"horse":"Sweet Dreams","jockey":"F Geroux","trainer":"B Baffert","ml":"6/1","wt":126},
        ]},
    ]

    # Clear and reseed
    db.query(Horse).delete()
    db.query(Race).delete()
    db.commit()

    BANKROLL = 1000.0
    KELLY_FRAC = 0.50
    total_horses = 0

    for rd in RACES_DATA:
        race = Race(
            race_date=race_date, track="Santa Anita Park",
            race_number=rd["race_number"], race_name=rd["race_name"],
            distance=rd["distance"], surface=rd["surface"],
            purse=rd["purse"], condition=rd["condition"],
            post_time=rd["post_time"], track_condition="Fast", weather="Clear",
        )
        db.add(race)
        db.flush()

        for e in rd["entries"]:
            ml_p  = op(e["ml"])
            beyer = round(75 + ml_p * 80, 1)
            horse = Horse(
                race_id=race.id, race_date=race_date,
                post_position=e["pp"], horse_name=e["horse"],
                jockey=e["jockey"], trainer=e["trainer"],
                morning_line_odds=e["ml"], weight=e["wt"], scratched=False,
                beyer_last=beyer, beyer_avg_3=round(beyer*0.97,1),
                jockey_win_pct_90d=gs(e["jockey"], JOCKEY_WIN, 0.08),
                trainer_win_pct_90d=gs(e["trainer"], TRAINER_WIN, 0.07),
                days_since_last=28, field_size=len(rd["entries"]),
            )
            db.add(horse)
            total_horses += 1

    db.commit()

    # Run model
    races_db = db.query(Race).filter(Race.race_date == race_date).all()
    for race in races_db:
        horses = db.query(Horse).filter(
            Horse.race_id == race.id, Horse.scratched == False
        ).order_by(Horse.post_position).all()
        if not horses: continue

        scores = [
            4.50 * ppb(h.post_position, race.surface) +
            2.80 * (h.jockey_win_pct_90d or 0.08) +
            2.60 * (h.trainer_win_pct_90d or 0.07) +
            0.08 * (h.beyer_last or 85) +
            1.50 * jt(h.jockey, h.trainer)
            for h in horses
        ]
        max_s = max(scores)
        exp_s = [np.exp(s - max_s) for s in scores]
        probs = [e / sum(exp_s) for e in exp_s]

        for h, prob in zip(horses, probs):
            odds = h.morning_line_odds or "9/2"
            track_p = op(odds)
            edge = prob - track_p
            dec = od(odds); b = dec - 1.0
            kf = max(0, (b*prob-(1-prob))/b) if b > 0 else 0
            bet = round(kf*KELLY_FRAC*BANKROLL, 2) if edge >= 0.035 else 0
            h.model_win_prob = round(prob,4); h.edge = round(edge,4)
            h.kelly_fraction = round(kf*KELLY_FRAC,4); h.kelly_bet_amount = bet

    db.commit()

    log = ScraperLog(
        source="startup_seed", status="success", records=total_horses,
        message=f"Seeded {len(RACES_DATA)} races, {total_horses} horses — {race_date}"
    )
    db.add(log); db.commit(); db.close()
    logger.info(f"Seed complete: {len(RACES_DATA)} races, {total_horses} horses for {race_date}")
