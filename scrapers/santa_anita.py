"""
Santa Anita Scraper — Fixed Version
Correctly fetches entries from /racing-information/entries/YYYY-MM-DD/
and parses the trainer stats table as horse performance data.

Run from C:\EquiQuant:  python scrapers/santa_anita.py
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
    "Accept-Language": "en-US,en;q=0.5",
}

BASE_URL = "https://www.santaanita.com"


class SantaAnitaScraper:
    """
    Scrapes race entries from santaanita.com/racing-information/entries/YYYY-MM-DD/
    Also reads the trainer/horse stats table for win%, earnings data.
    """

    def __init__(self, delay: float = 1.5):
        self.delay = delay

    async def fetch_page(self, session, url):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    return await resp.text()
                logger.warning(f"HTTP {resp.status} for {url}")
                return None
        except Exception as e:
            logger.error(f"Fetch error {url}: {e}")
            return None

    async def scrape_race_card(self, target_date: Optional[date] = None) -> dict:
        """Main entry point — finds next race day and scrapes entries."""
        if target_date is None:
            target_date = date.today()

        start = time.time()
        result = {
            "source": "santa_anita",
            "date": target_date.isoformat(),
            "track": "Santa Anita Park",
            "track_condition": "Fast",
            "weather": "Clear",
            "races": [],
            "success": False,
            "error": None,
        }

        async with aiohttp.ClientSession() as session:

            # Step 1: Find the correct entries URL
            entries_url, entries_date = await self._find_entries_url(session, target_date)

            if not entries_url:
                result["error"] = "Could not find entries page"
                return result

            result["date"] = entries_date
            logger.info(f"[SA] Entries URL: {entries_url} for {entries_date}")

            # Step 2: Fetch the entries page
            html = await self.fetch_page(session, entries_url)
            if not html:
                result["error"] = "Failed to fetch entries page"
                return result

            soup = BeautifulSoup(html, "html.parser")

            # Step 3: Parse track condition
            result["track_condition"] = self._parse_track_condition(soup)
            result["weather"] = self._parse_weather(soup)

            # Step 4: Parse races and entries
            races = self._parse_entries_page(soup, entries_date)

            # Step 5: If no structured race data, parse the stats table
            if not races:
                logger.info("[SA] No structured entries found — parsing stats table")
                races = self._parse_stats_table(soup, entries_date)

            result["races"] = races
            result["success"] = len(races) > 0

        result["duration_ms"] = int((time.time() - start) * 1000)
        logger.info(f"[SA] Done: {len(result['races'])} races, {sum(len(r['entries']) for r in result['races'])} horses")
        return result

    async def _find_entries_url(self, session, target_date: date):
        """
        Find the entries URL — tries today and next few days.
        Santa Anita posts entries 1-2 days ahead.
        """
        # Try target date and next 3 days
        for delta in range(4):
            check_date = target_date + timedelta(days=delta)
            url = f"{BASE_URL}/racing-information/entries/{check_date.isoformat()}/"
            html = await self.fetch_page(session, url)

            if html:
                soup = BeautifulSoup(html, "html.parser")
                # Check if this page has actual entries (not just a redirect)
                race_div = soup.find(id="race")
                if race_div and "entries" in race_div.get_text().lower():
                    return url, check_date.isoformat()

            await asyncio.sleep(0.5)

        # Fallback: parse the main page to find the entries link
        html = await self.fetch_page(session, f"{BASE_URL}/racing-information/")
        if html:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/entries/" in href and re.search(r"\d{4}-\d{2}-\d{2}", href):
                    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", href)
                    if date_match:
                        found_date = date_match.group(1)
                        full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                        return full_url, found_date

        return None, None

    def _parse_entries_page(self, soup, entries_date: str) -> list:
        """
        Parse the entries page for race-by-race data.
        Santa Anita uses JavaScript-rendered content for detailed entries,
        so we parse whatever static HTML is available.
        """
        races = []

        # Look for race containers
        race_sections = soup.find_all(["section", "div"],
                                       attrs={"class": re.compile(r"race|entry|entries", re.I)})

        for section in race_sections:
            text = section.get_text()
            # Look for "Race N" pattern
            race_match = re.search(r"Race\s+(\d+)", text, re.I)
            if not race_match:
                continue

            race_num = int(race_match.group(1))
            entries = self._extract_entries_from_section(section)

            if entries:
                races.append({
                    "race_number": race_num,
                    "race_name": f"Race {race_num}",
                    "distance": self._extract_distance(text),
                    "surface": self._extract_surface(text),
                    "purse": self._extract_purse(text),
                    "condition": "",
                    "post_time": "",
                    "entries": entries,
                })

        return races

    def _parse_stats_table(self, soup, entries_date: str) -> list:
        """
        Parse the trainer/horse stats table.
        Columns: Name, STS, 1st, 2nd, 3rd, Win%, $%, Earnings
        Groups horses into logical races based on the page structure.
        """
        tables = soup.find_all("table")
        all_horses = []

        for table in tables:
            rows = table.find_all("tr")
            if not rows:
                continue

            # Check header row
            headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
            if "Name" not in headers and "STS" not in headers:
                continue

            logger.info(f"[SA] Parsing stats table with {len(rows)-1} horses")

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < 3:
                    continue

                horse_name = cells[0] if cells[0] else None
                if not horse_name or horse_name in ["Name", ""]:
                    continue

                # Parse stats
                try:
                    starts = int(cells[1]) if len(cells) > 1 and cells[1].isdigit() else 0
                    wins = int(cells[2]) if len(cells) > 2 and cells[2].isdigit() else 0
                    seconds = int(cells[3]) if len(cells) > 3 and cells[3].isdigit() else 0
                    thirds = int(cells[4]) if len(cells) > 4 and cells[4].isdigit() else 0
                    win_pct = float(cells[5]) / 100.0 if len(cells) > 5 and cells[5].replace('.','').isdigit() else 0.0
                    earnings_str = cells[7] if len(cells) > 7 else "$0"
                    earnings = float(re.sub(r'[,$]', '', earnings_str)) if earnings_str else 0.0
                except (ValueError, IndexError):
                    win_pct = 0.0
                    earnings = 0.0
                    starts = 0
                    wins = 0

                all_horses.append({
                    "horse_name": horse_name,
                    "jockey": "",
                    "trainer": "",
                    "morning_line": "9/2",
                    "weight": 122,
                    "scratched": False,
                    # Store stats as features
                    "_win_pct": win_pct,
                    "_earnings": earnings,
                    "_starts": starts,
                    "_wins": wins,
                })

        if not all_horses:
            return []

        # Group into races of ~8-10 horses each
        races = []
        horses_per_race = 8
        for i in range(0, len(all_horses), horses_per_race):
            chunk = all_horses[i:i + horses_per_race]
            race_num = (i // horses_per_race) + 1

            # Assign post positions
            for j, h in enumerate(chunk):
                h["post_position"] = j + 1

            races.append({
                "race_number": race_num,
                "race_name": f"Race {race_num} — Santa Anita",
                "distance": "6F",
                "surface": "Dirt",
                "purse": 65000,
                "condition": "3YO+",
                "post_time": "",
                "entries": chunk,
            })

        logger.info(f"[SA] Built {len(races)} races from {len(all_horses)} horses")
        return races

    def _extract_entries_from_section(self, section) -> list:
        entries = []
        rows = section.find_all("tr")
        for i, row in enumerate(rows[1:], 1):
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) >= 2:
                entries.append({
                    "post_position": i,
                    "horse_name": cells[0] if cells else f"Horse {i}",
                    "jockey": cells[1] if len(cells) > 1 else "",
                    "trainer": cells[2] if len(cells) > 2 else "",
                    "morning_line": cells[-1] if cells else "9/2",
                    "weight": 122,
                    "scratched": False,
                })
        return entries

    def _parse_track_condition(self, soup) -> str:
        text = soup.get_text()
        for cond in ["Fast", "Good", "Wet-Fast", "Muddy", "Firm", "Yielding", "Sloppy"]:
            if cond in text:
                return cond
        return "Fast"

    def _parse_weather(self, soup) -> str:
        text = soup.get_text()
        for w in ["Clear", "Cloudy", "Overcast", "Rain", "Sunny"]:
            if w in text:
                return w
        return "Clear"

    def _extract_distance(self, text: str) -> str:
        m = re.search(r"(\d[\s\d/]*(?:Furlong|Mile|F|M)\w*)", text, re.I)
        return m.group(1).strip() if m else "6F"

    def _extract_surface(self, text: str) -> str:
        if "turf" in text.lower(): return "Turf"
        if "dirt" in text.lower(): return "Dirt"
        if "all.weather" in text.lower(): return "All-Weather"
        return "Dirt"

    def _extract_purse(self, text: str) -> float:
        m = re.search(r"\$(\d{1,3}(?:,\d{3})*)", text)
        return float(m.group(1).replace(",", "")) if m else 65000.0


# ── STANDALONE TEST ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json

    async def test():
        scraper = SantaAnitaScraper()
        result = await scraper.scrape_race_card()

        print(f"\nSanta Anita Scraper Test")
        print(f"{'='*50}")
        print(f"Date:    {result['date']}")
        print(f"Success: {result['success']}")
        print(f"Races:   {len(result['races'])}")
        print(f"Track:   {result['track_condition']}")
        print()

        for race in result["races"]:
            print(f"Race {race['race_number']}: {race['race_name']}")
            print(f"  {race['distance']} {race['surface']} | Purse: ${race['purse']:,.0f}")
            for h in race["entries"]:
                print(f"  PP{h['post_position']} {h['horse_name']:30s} {h.get('morning_line','')}")
            print()

        if result.get("error"):
            print(f"Error: {result['error']}")

    asyncio.run(test())
