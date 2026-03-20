"""
EquiQuant — Smart Scheduler
- Loads race card on any Fri/Sat/Sun race day at 7:30 AM PT
- Refreshes live odds from Equibase every 3 minutes during racing hours
- Auto-scrapes results from Equibase after each race
"""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import date, datetime, timedelta
import logging
import pytz

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler(timezone="America/Los_Angeles")
PT = pytz.timezone("America/Los_Angeles")


def is_race_day(d: date = None) -> bool:
    """Returns True if today is a Santa Anita race day (Fri/Sat/Sun)."""
    if d is None:
        d = datetime.now(PT).date()
    return d.weekday() in (4, 5, 6)  # 4=Fri, 5=Sat, 6=Sun


def is_racing_hours() -> bool:
    """Returns True during Santa Anita racing hours (11 AM - 7 PM PT)."""
    now = datetime.now(PT)
    return now.hour >= 11 and now.hour < 19


async def start_scheduler():
    from database import init_db
    init_db()

    # Morning card load — 7:30 AM PT on race days
    scheduler.add_job(
        morning_race_load,
        CronTrigger(day_of_week="fri,sat,sun", hour=7, minute=30, timezone=PT),
        id="morning_load",
        name="Morning Race Card Load",
        replace_existing=True,
    )

    # Live odds refresh — every 3 minutes during racing hours on race days
    scheduler.add_job(
        refresh_live_odds,
        "interval",
        minutes=3,
        id="live_odds",
        name="Live Odds Refresh",
        replace_existing=True,
    )

    # Results scraper — every 12 minutes during racing hours on race days
    # (races are ~25 min apart, we check every 12 to catch results quickly)
    scheduler.add_job(
        scrape_results,
        "interval",
        minutes=12,
        id="results_scraper",
        name="Auto Results Scraper",
        replace_existing=True,
    )

    # Edge recalculator — runs after odds refresh to update Kelly bets
    scheduler.add_job(
        recalculate_edges,
        "interval",
        minutes=3,
        id="edge_recalc",
        name="Edge Recalculator",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started — Fri/Sat/Sun race days, live odds every 3min")


async def morning_race_load():
    """Load today's full race card from Equibase at 7:30 AM PT."""
    if not is_race_day():
        logger.info("Scheduler: not a race day — skipping morning load")
        return

    logger.info("Scheduler: morning race card load starting...")
    try:
        from startup import startup_load
        await startup_load()
        logger.info("Scheduler: morning load complete")
    except Exception as e:
        logger.error(f"Scheduler morning load error: {e}")


async def refresh_live_odds():
    """Refresh LiveOdds column from Equibase every 3 minutes during racing."""
    if not is_race_day() or not is_racing_hours():
        return

    try:
        import aiohttp, re
        from bs4 import BeautifulSoup
        from database import SessionLocal, Race, Horse, init_db

        init_db()
        db = SessionLocal()
        today = datetime.now(PT).date()
        race_date = today.isoformat()
        mm = str(today.month).zfill(2)
        url = f"https://www.equibase.com/static/entry/SA{mm}{today.year}USA-EQB.html"
        hdrs = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36","Referer":"https://www.equibase.com/"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=hdrs, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    db.close(); return
                html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")
        updated = 0

        for table in tables:
            rows = table.find_all("tr")
            if not rows or len(rows) < 2: continue
            hs = [re.sub(r'\s+',' ',td.get_text().strip()) for td in rows[0].find_all(["th","td"])]
            if "Horse" not in hs or "LiveOdds" not in hs: continue

            hi = next((j for j,h in enumerate(hs) if "Horse" in h), None)
            li = next((j for j,h in enumerate(hs) if "LiveOdds" in h), None)
            if hi is None or li is None: continue

            for row in rows[1:]:
                cs = [re.sub(r'\s+',' ',td.get_text().strip()) for td in row.find_all(["td","th"])]
                if len(cs) <= max(hi, li): continue
                horse_raw = cs[hi]
                horse_name = re.sub(r'\s*\([A-Z]{2,3}\)\s*$', '', horse_raw).strip()
                live_odds = cs[li].strip()
                if not horse_name or not live_odds: continue

                # Update in database
                horse = db.query(Horse).filter(
                    Horse.horse_name == horse_name,
                    Horse.race_date == race_date
                ).first()
                if horse and live_odds and re.match(r'\d+/\d+', live_odds):
                    horse.live_odds = live_odds
                    updated += 1

        db.commit()
        db.close()
        if updated > 0:
            logger.info(f"Live odds: updated {updated} horses")
            # Recalculate edges with new odds
            await recalculate_edges()

    except Exception as e:
        logger.error(f"Live odds refresh error: {e}")


async def recalculate_edges():
    """Recalculate model edge and Kelly bets using latest live odds."""
    if not is_race_day(): return

    try:
        import numpy as np, re
        from database import SessionLocal, Race, Horse, init_db

        init_db()
        db = SessionLocal()
        today = datetime.now(PT).date().isoformat()

        BANKROLL = 1000.0
        KELLY_FRAC = 0.50

        def op(o):
            try: n,d=str(o).split("/"); return float(d)/(float(n)+float(d))
            except: return 0.10
        def od(o):
            try: n,d=str(o).split("/"); return (float(n)+float(d))/float(d)
            except: return 10.0

        races = db.query(Race).filter(Race.race_date == today).all()
        for race in races:
            horses = db.query(Horse).filter(
                Horse.race_id == race.id, Horse.scratched == False,
                Horse.model_win_prob.isnot(None)
            ).all()
            if not horses: continue

            for h in horses:
                prob = h.model_win_prob or 0
                # Use live odds if available, fall back to morning line
                odds = h.live_odds or h.morning_line_odds or "9/2"
                track_p = op(odds)
                edge = prob - track_p
                dec = od(odds); b = dec - 1.0
                kf = max(0, (b*prob-(1-prob))/b) if b > 0 else 0
                bet = round(kf*KELLY_FRAC*BANKROLL, 2) if edge >= 0.035 else 0
                h.edge = round(edge, 4)
                h.kelly_fraction = round(kf*KELLY_FRAC, 4)
                h.kelly_bet_amount = bet

        db.commit()
        db.close()

    except Exception as e:
        logger.error(f"Edge recalc error: {e}")


async def scrape_results():
    """
    Auto-scrape race results from Equibase results page.
    Runs every 12 minutes during racing hours.
    Checks for any completed races and logs win/place/show + payouts.
    """
    if not is_race_day() or not is_racing_hours():
        return

    try:
        import aiohttp, re
        from bs4 import BeautifulSoup
        from database import SessionLocal, Race, Horse, RaceResult, Bet, init_db

        init_db()
        db = SessionLocal()
        today = datetime.now(PT).date()
        race_date = today.isoformat()
        mm = str(today.month).zfill(2)

        # Equibase results URL
        url = f"https://www.equibase.com/static/result/SA{mm}{today.year}USA-EQB.html"
        hdrs = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36","Referer":"https://www.equibase.com/"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=hdrs, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    db.close(); return
                html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")

        # Find all result tables
        races_db = {r.race_number: r for r in db.query(Race).filter(Race.race_date == race_date).all()}
        results_saved = 0

        # Parse result tables — Equibase uses class "results-running-line" or similar
        result_sections = soup.find_all(["div","section"], class_=re.compile(r'result|race', re.I))

        for section in result_sections:
            # Find race number
            rn_match = re.search(r'RACE\s*(\d+)', section.get_text(), re.I)
            if not rn_match: continue
            race_num = int(rn_match.group(1))

            # Check if we already have results for this race
            if db.query(RaceResult).filter(
                RaceResult.race_date == race_date,
                RaceResult.race_id == races_db.get(race_num, type('', (), {'id': -1})).id
            ).first():
                continue  # Already logged

            race = races_db.get(race_num)
            if not race: continue

            # Parse finishing order
            rows = section.find_all("tr")
            finishers = []
            for row in rows:
                cells = [c.get_text(strip=True) for c in row.find_all(["td","th"])]
                if len(cells) < 3: continue
                # Look for rows with finish position 1, 2, 3
                if cells[0] in ["1","2","3"]:
                    horse_name = re.sub(r'\s*\([A-Z]{2,3}\)\s*$','',cells[1]).strip()
                    finishers.append((int(cells[0]), horse_name))

            if len(finishers) < 3: continue

            # Extract payouts
            win_pay = place_pay = show_pay = 0.0
            payout_text = section.get_text()
            win_m = re.search(r'Win[^\d]*(\d+\.\d+)', payout_text, re.I)
            place_m = re.search(r'Place[^\d]*(\d+\.\d+)', payout_text, re.I)
            show_m = re.search(r'Show[^\d]*(\d+\.\d+)', payout_text, re.I)
            if win_m: win_pay = float(win_m.group(1))
            if place_m: place_pay = float(place_m.group(1))
            if show_m: show_pay = float(show_m.group(1))

            winner   = next((h for p,h in finishers if p==1), "")
            place_h  = next((h for p,h in finishers if p==2), "")
            show_h   = next((h for p,h in finishers if p==3), "")

            # Save result
            result = RaceResult(
                race_id=race.id, race_date=race_date,
                winner=winner, place=place_h, show=show_h,
                win_payout=win_pay, place_payout=place_pay, show_payout=show_pay,
            )
            db.add(result)

            # Update bet records — mark as won/lost
            bets = db.query(Bet).filter(
                Bet.race_id == race.id, Bet.result == None
            ).all()
            for bet in bets:
                if bet.horse_name == winner:
                    bet.result = "WIN"
                    bet.payout = round(bet.amount * win_pay / 2, 2)  # $2 base
                    bet.profit_loss = round(bet.payout - bet.amount, 2)
                elif bet.horse_name in [place_h, show_h]:
                    bet.result = "PLACE" if bet.horse_name == place_h else "SHOW"
                    pay = place_pay if bet.horse_name == place_h else show_pay
                    bet.payout = round(bet.amount * pay / 2, 2)
                    bet.profit_loss = round(bet.payout - bet.amount, 2)
                else:
                    bet.result = "LOSS"
                    bet.payout = 0.0
                    bet.profit_loss = -bet.amount

            db.commit()
            results_saved += 1
            logger.info(f"Results: Race {race_num} — Winner: {winner} (${win_pay})")

        db.close()
        if results_saved > 0:
            logger.info(f"Results: saved {results_saved} race results")

    except Exception as e:
        logger.error(f"Results scraper error: {e}")
