"""
EquiQuant — Equibase Scraper
Fetches full race entries with jockey, trainer, weight, and morning line odds
from equibase.com — the official supplier of racing data.

Table columns: P#, PP, Horse, VS, A/S, Med, [Claim$], Jockey, Wgt, Trainer, M/L, LiveOdds
"""

import asyncio
import aiohttp
import logging
import re
import time
from datetime import datetime, date, timedelta
from typing import Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.equibase.com/",
}

# Equibase URL pattern: SA = Santa Anita, MMDDYYYY, USA, EQB
# e.g. https://www.equibase.com/static/entry/SA032026USA-EQB.html
EQUIBASE_BASE = "https://www.equibase.com/static/entry"
TRACK_CODE = "SA"


class EquibaseScraper:
    """
    Scrapes full race entries from Equibase.
    Returns complete horse data including jockey, trainer, ML odds.
    """

    def __init__(self, delay: float = 1.5):
        self.delay = delay

    def _build_url(self, target_date: date) -> str:
        date_str = target_date.strftime("%m%d%Y")  # MMDDYYYY
        # Remove leading zero from month for Equibase format
        month = str(target_date.month)
        day = str(target_date.day).zfill(2)
        year = str(target_date.year)
        return f"{EQUIBASE_BASE}/{TRACK_CODE}{month}{year}USA-EQB.html"

    async def fetch_page(self, session, url):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    return await resp.text()
                logger.warning(f"HTTP {resp.status} — {url}")
                return None
        except Exception as e:
            logger.error(f"Fetch error: {e}")
            return None

    async def scrape_race_card(self, target_date: Optional[date] = None) -> dict:
        if target_date is None:
            target_date = date.today()

        start = time.time()
        result = {
            "source": "equibase",
            "date": target_date.isoformat(),
            "track": "Santa Anita Park",
            "track_condition": "Fast",
            "weather": "Clear",
            "races": [],
            "success": False,
            "error": None,
        }

        async with aiohttp.ClientSession() as session:
            # Try target date and next 2 days
            for delta in range(3):
                check_date = target_date + timedelta(days=delta)
                url = self._build_url(check_date)
                logger.info(f"[Equibase] Trying: {url}")

                html = await self.fetch_page(session, url)
                if html and len(html) > 10000:
                    soup = BeautifulSoup(html, "html.parser")
                    title = soup.find("title")
                    if title and "Santa Anita" in title.get_text():
                        result["date"] = check_date.isoformat()
                        races = self._parse_equibase_page(soup)
                        if races:
                            result["races"] = races
                            result["success"] = True
                            logger.info(f"[Equibase] Found {len(races)} races for {check_date}")
                            break

                await asyncio.sleep(self.delay)

        result["duration_ms"] = int((time.time() - start) * 1000)
        return result

    def _parse_equibase_page(self, soup) -> list:
        """
        Parse all race tables from Equibase entries page.
        Each table is one race. Columns vary slightly (with/without Claim$).
        """
        races = []
        tables = soup.find_all("table")

        # Find race headers — look for race number/name sections above each table
        race_headers = self._extract_race_headers(soup)

        for i, table in enumerate(tables):
            rows = table.find_all("tr")
            if not rows:
                continue

            # Check header row for expected columns
            header_cells = [td.get_text(strip=True) for td in rows[0].find_all(["th", "td"])]
            if "Horse" not in header_cells or "Jockey" not in header_cells:
                continue

            # Determine column indices dynamically
            col = self._map_columns(header_cells)
            if col is None:
                continue

            entries = []
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < 8:
                    continue

                horse_name = cells[col["horse"]] if col["horse"] < len(cells) else ""
                if not horse_name or horse_name in ["Horse", ""]:
                    continue

                # Detect scratches
                row_text = row.get_text().lower()
                scratched = "scr" in row_text or "scratch" in row_text

                jockey = cells[col["jockey"]] if col["jockey"] < len(cells) else ""
                trainer = cells[col["trainer"]] if col["trainer"] < len(cells) else ""
                ml_odds = cells[col["ml"]] if col["ml"] < len(cells) else "9/2"
                weight = cells[col["wgt"]] if col["wgt"] < len(cells) else "122"
                pp = cells[col["pp"]] if col["pp"] < len(cells) else str(len(entries) + 1)
                age_sex = cells[col["age_sex"]] if col.get("age_sex") and col["age_sex"] < len(cells) else ""

                # Clean jockey name (remove extra spaces)
                jockey = re.sub(r'\s+', ' ', jockey).strip()
                trainer = re.sub(r'\s+', ' ', trainer).strip()
                ml_odds = ml_odds.strip() if ml_odds else "9/2"

                entries.append({
                    "post_position": self._safe_int(pp) or (len(entries) + 1),
                    "horse_name": horse_name.strip(),
                    "jockey": jockey,
                    "trainer": trainer,
                    "morning_line": ml_odds if re.match(r'\d+/\d+', ml_odds) else "9/2",
                    "weight": self._safe_int(weight) or 122,
                    "age_sex": age_sex,
                    "scratched": scratched,
                })

            if not entries:
                continue

            # Get race info from header if available
            race_num = i + 1
            race_info = race_headers[i] if i < len(race_headers) else {}

            races.append({
                "race_number": race_info.get("race_number", race_num),
                "race_name": race_info.get("race_name", f"Race {race_num}"),
                "distance": race_info.get("distance", "6F"),
                "surface": race_info.get("surface", "Dirt"),
                "purse": race_info.get("purse", 65000),
                "condition": race_info.get("condition", ""),
                "post_time": race_info.get("post_time", ""),
                "entries": entries,
            })

            logger.info(f"[Equibase] Race {race_num}: {len(entries)} horses")

        return races

    def _extract_race_headers(self, soup) -> list:
        """
        Extract race name, distance, purse etc. from section headers.
        Equibase has race info in divs/headers above each table.
        """
        headers = []
        race_divs = soup.find_all(["div", "h2", "h3", "h4"],
                                   string=re.compile(r"Race\s+\d+", re.I))

        for div in race_divs:
            text = div.get_text(" ", strip=True)
            info = {"race_name": text[:60]}

            # Race number
            rn = re.search(r"Race\s+(\d+)", text, re.I)
            if rn:
                info["race_number"] = int(rn.group(1))

            # Distance
            dist = re.search(r"(\d[\s\d/]*(?:Furlong|Mile|f|m)\w*)", text, re.I)
            if dist:
                info["distance"] = dist.group(1).strip()

            # Purse
            purse = re.search(r"\$(\d{1,3}(?:,\d{3})*)", text)
            if purse:
                info["purse"] = float(purse.group(1).replace(",", ""))

            # Surface
            if "turf" in text.lower():
                info["surface"] = "Turf"
            elif "dirt" in text.lower():
                info["surface"] = "Dirt"
            elif "all.weather" in text.lower() or "aw" in text.lower():
                info["surface"] = "All-Weather"

            headers.append(info)

        # Also search parent containers for race info
        if not headers:
            # Fallback: find race info from surrounding text of each table
            for table in soup.find_all("table"):
                prev = table.find_previous(["h2", "h3", "h4", "div", "p"])
                if prev:
                    text = prev.get_text(" ", strip=True)
                    info = {"race_name": text[:60] if text else "Race"}
                    rn = re.search(r"Race\s+(\d+)", text, re.I)
                    if rn:
                        info["race_number"] = int(rn.group(1))
                    headers.append(info)

        return headers

    def _map_columns(self, headers: list) -> Optional[dict]:
        """Map column names to indices dynamically."""
        h = [c.lower().strip() for c in headers]

        def find(names):
            for name in names:
                for i, col in enumerate(h):
                    if name in col:
                        return i
            return None

        pp_idx      = find(["pp", "p#"])
        horse_idx   = find(["horse"])
        jockey_idx  = find(["jockey"])
        trainer_idx = find(["trainer"])
        wgt_idx     = find(["wgt", "weight"])
        ml_idx      = find(["m/l", "ml", "morning"])
        age_idx     = find(["a/s", "age"])

        if horse_idx is None or jockey_idx is None or trainer_idx is None:
            return None

        return {
            "pp":      pp_idx or 1,
            "horse":   horse_idx,
            "jockey":  jockey_idx,
            "trainer": trainer_idx,
            "wgt":     wgt_idx or jockey_idx - 1,
            "ml":      ml_idx or trainer_idx + 1,
            "age_sex": age_idx,
        }

    def _safe_int(self, s) -> Optional[int]:
        try:
            return int(re.sub(r"[^\d]", "", str(s)))
        except Exception:
            return None


# ── STANDALONE TEST ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    async def test():
        scraper = EquibaseScraper()
        result = await scraper.scrape_race_card()

        print(f"\nEquibase Scraper Test")
        print(f"{'='*60}")
        print(f"Date:    {result['date']}")
        print(f"Success: {result['success']}")
        print(f"Races:   {len(result['races'])}")
        if result.get("error"):
            print(f"Error:   {result['error']}")
        print()

        for race in result["races"]:
            print(f"Race {race['race_number']}: {race['race_name']}")
            print(f"  {race['distance']} {race['surface']} | ${race['purse']:,.0f}")
            print(f"  {'PP':<4} {'Horse':<30} {'Jockey':<20} {'Trainer':<20} {'ML'}")
            print(f"  {'-'*90}")
            for h in race["entries"]:
                scratch = " [SCR]" if h["scratched"] else ""
                print(f"  {h['post_position']:<4} {h['horse_name']:<30} {h['jockey']:<20} {h['trainer']:<20} {h['morning_line']}{scratch}")
            print()

    asyncio.run(test())
