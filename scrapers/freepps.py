"""
FreePPs Scraper Agent
Downloads past performance data from thefreepps.com
Handles PDF download, text extraction, and structured parsing.
"""

import asyncio
import aiohttp
import aiofiles
import logging
import re
import time
import os
import pdfplumber
from datetime import datetime, date
from typing import Optional
from pathlib import Path
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://thefreepps.com"
DOWNLOAD_DIR = Path("./data/pps")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://thefreepps.com/",
}


class FreePPsScraper:
    """
    Downloads and parses Past Performances from thefreepps.com.

    Strategy:
    1. Scrape the site to find today's SA past performances (usually a PDF link)
    2. Download the PDF
    3. Extract text using pdfplumber
    4. Parse each horse's PP block into structured features
    """

    def __init__(self, delay: float = 2.0):
        self.delay = delay

    async def fetch_pps(self, target_date: Optional[date] = None, track: str = "Santa Anita") -> dict:
        if target_date is None:
            target_date = date.today()

        start = time.time()
        result = {
            "source": "freepps",
            "date": target_date.isoformat(),
            "track": track,
            "horses": [],
            "success": False,
            "error": None,
        }

        async with aiohttp.ClientSession(headers=HEADERS) as session:
            # Step 1: Find the PP file link
            pdf_url = await self._find_pp_link(session, target_date, track)
            if not pdf_url:
                result["error"] = f"No PP file found for {track} on {target_date}"
                logger.warning(result["error"])
                return result

            # Step 2: Download the PDF
            pdf_path = await self._download_pdf(session, pdf_url, target_date, track)
            if not pdf_path:
                result["error"] = "Failed to download PP PDF"
                return result

            # Step 3: Parse PDF into structured horse records
            horses = self._parse_pp_pdf(pdf_path)
            result["horses"] = horses
            result["success"] = len(horses) > 0
            result["records"] = len(horses)

        result["duration_ms"] = int((time.time() - start) * 1000)
        logger.info(f"[FreePPs] Parsed {len(result['horses'])} horse PPs in {result['duration_ms']}ms")
        return result

    async def _find_pp_link(self, session: aiohttp.ClientSession, target_date: date, track: str) -> Optional[str]:
        """Scrape thefreepps.com to find the PP download link for today."""
        try:
            async with session.get(BASE_URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.error(f"[FreePPs] Landing page returned {resp.status}")
                    return None
                html = await resp.text()

            soup = BeautifulSoup(html, "html.parser")

            # Look for links containing the track name and today's date
            date_patterns = [
                target_date.strftime("%m-%d-%Y"),
                target_date.strftime("%m%d%Y"),
                target_date.strftime("%Y%m%d"),
                target_date.strftime("%m/%d/%Y"),
            ]

            track_keywords = {
                "Santa Anita": ["santa-anita", "santaanita", "SA"],
                "Del Mar": ["del-mar", "delmar", "DM"],
            }
            keywords = track_keywords.get(track, [track.lower().replace(" ", "-")])

            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                text = a.get_text(strip=True).lower()

                # Check if link matches track and date
                track_match = any(kw.lower() in href or kw.lower() in text for kw in keywords)
                date_match = any(dp in href or dp in text for dp in date_patterns)

                if track_match and (".pdf" in href or ".zip" in href or "download" in href):
                    full_url = href if href.startswith("http") else f"{BASE_URL}/{href.lstrip('/')}"
                    logger.info(f"[FreePPs] Found PP link: {full_url}")
                    return full_url

            # Fallback: find any PDF link for the track
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" in href.lower() and any(kw.lower() in href.lower() for kw in keywords):
                    full_url = href if href.startswith("http") else f"{BASE_URL}/{href.lstrip('/')}"
                    return full_url

            logger.warning("[FreePPs] Could not find PP link automatically")
            return None

        except Exception as e:
            logger.error(f"[FreePPs] Error finding PP link: {e}")
            return None

    async def _download_pdf(self, session: aiohttp.ClientSession, url: str, target_date: date, track: str) -> Optional[Path]:
        """Download the PP PDF file."""
        filename = f"{track.replace(' ', '_')}_{target_date.isoformat()}_PPs.pdf"
        filepath = DOWNLOAD_DIR / filename

        if filepath.exists():
            logger.info(f"[FreePPs] Using cached PDF: {filepath}")
            return filepath

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status != 200:
                    logger.error(f"[FreePPs] Download failed: HTTP {resp.status}")
                    return None

                async with aiofiles.open(filepath, "wb") as f:
                    await f.write(await resp.read())

            logger.info(f"[FreePPs] Downloaded PDF: {filepath} ({filepath.stat().st_size:,} bytes)")
            return filepath

        except Exception as e:
            logger.error(f"[FreePPs] Download error: {e}")
            return None

    def _parse_pp_pdf(self, pdf_path: Path) -> list:
        """
        Extract and parse Past Performance data from a PDF.
        Returns list of horse dicts with structured PP features.
        """
        horses = []

        try:
            with pdfplumber.open(pdf_path) as pdf:
                full_text = ""
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"

            # Split into per-horse blocks (PP files typically have a header per horse)
            horse_blocks = self._split_into_horse_blocks(full_text)

            for block in horse_blocks:
                horse_data = self._parse_horse_block(block)
                if horse_data and horse_data.get("horse_name"):
                    horses.append(horse_data)

        except Exception as e:
            logger.error(f"[FreePPs] PDF parse error: {e}")

        return horses

    def _split_into_horse_blocks(self, text: str) -> list:
        """
        Split PDF text into per-horse blocks.
        PPs typically start each horse with the race/horse header line.
        """
        # Common PP formats start with PP number or horse name in CAPS
        # Adjust the pattern to match the actual FreePPs PDF format
        blocks = []
        current = []

        for line in text.split("\n"):
            # Detect start of new horse block (CAPS horse name followed by stats)
            if re.match(r"^\s*\d+\s+[A-Z][A-Z\s\']{3,}", line):
                if current:
                    blocks.append("\n".join(current))
                current = [line]
            else:
                current.append(line)

        if current:
            blocks.append("\n".join(current))

        return [b for b in blocks if len(b.strip()) > 50]

    def _parse_horse_block(self, block: str) -> dict:
        """
        Parse a single horse's PP block into structured features.
        PP data is columnar — positions are somewhat consistent across lines.
        """
        lines = [l for l in block.split("\n") if l.strip()]
        if not lines:
            return {}

        horse = {
            "horse_name": "",
            "post_position": None,
            "jockey": "",
            "trainer": "",
            "weight": None,
            "past_races": [],
            # Computed features (set after parsing past races)
            "beyer_last": None,
            "beyer_2back": None,
            "beyer_3back": None,
            "beyer_avg_3": None,
            "pace_e1": None,
            "pace_e2": None,
            "pace_lp": None,
            "days_since_last": None,
            "surface_switch": False,
            "distance_switch": False,
            "class_rating": None,
        }

        # First line typically: "PP  HORSE NAME  Jockey / Trainer  Wt  ML"
        header = lines[0]
        pp_match = re.match(r"^\s*(\d+)\s+([A-Z][A-Za-z\s\']+)", header)
        if pp_match:
            horse["post_position"] = int(pp_match.group(1))
            horse["horse_name"] = pp_match.group(2).strip()

        # Parse jockey/trainer from second or third line
        for line in lines[1:4]:
            if "/" in line:
                parts = line.split("/")
                horse["jockey"] = parts[0].strip()
                horse["trainer"] = parts[1].strip() if len(parts) > 1 else ""
                break

        # Parse past race lines (usually lines 4+ with dates and figures)
        past_races = []
        for line in lines[3:]:
            race = self._parse_past_race_line(line)
            if race:
                past_races.append(race)

        horse["past_races"] = past_races[:10]  # keep last 10

        # Compute derived features from past races
        if past_races:
            beyers = [r["beyer"] for r in past_races if r.get("beyer")]
            if beyers:
                horse["beyer_last"] = beyers[0]
                horse["beyer_2back"] = beyers[1] if len(beyers) > 1 else None
                horse["beyer_3back"] = beyers[2] if len(beyers) > 2 else None
                horse["beyer_avg_3"] = sum(beyers[:3]) / min(3, len(beyers))

            e1_vals = [r["e1"] for r in past_races if r.get("e1")]
            if e1_vals:
                horse["pace_e1"] = e1_vals[0]

            e2_vals = [r["e2"] for r in past_races if r.get("e2")]
            if e2_vals:
                horse["pace_e2"] = e2_vals[0]

            lp_vals = [r["lp"] for r in past_races if r.get("lp")]
            if lp_vals:
                horse["pace_lp"] = lp_vals[0]

            if past_races[0].get("date"):
                from datetime import date as d_cls
                try:
                    last_date = datetime.strptime(past_races[0]["date"], "%m/%d/%y").date()
                    horse["days_since_last"] = (date.today() - last_date).days
                except Exception:
                    pass

            # Surface switch detection
            if len(past_races) >= 2:
                horse["surface_switch"] = past_races[0].get("surface") != past_races[1].get("surface")

        return horse

    def _parse_past_race_line(self, line: str) -> Optional[dict]:
        """
        Parse a single past race line from the PP.
        Format varies by PP source but typically includes:
        date, track, distance, surface, class, finish, Beyer, E1, E2, LP
        """
        # Skip lines that don't look like past race data
        if not re.search(r"\d{2}/\d{2}/\d{2}", line):
            return None

        race = {
            "date": None,
            "track": None,
            "distance": None,
            "surface": None,
            "finish": None,
            "beyer": None,
            "e1": None,
            "e2": None,
            "lp": None,
        }

        # Date pattern: MM/DD/YY
        date_match = re.search(r"(\d{2}/\d{2}/\d{2})", line)
        if date_match:
            race["date"] = date_match.group(1)

        # Track abbreviation (2-3 caps)
        track_match = re.search(r"\b([A-Z]{2,4})\b", line)
        if track_match:
            race["track"] = track_match.group(1)

        # Distance: e.g. "6f" "1m" "1 1/16m"
        dist_match = re.search(r"(\d[\s\d/]*(?:f|m|F|M))", line)
        if dist_match:
            race["distance"] = dist_match.group(1).strip()

        # Surface: D=Dirt, T=Turf, AW=All-Weather
        if re.search(r"\bD\b", line):
            race["surface"] = "Dirt"
        elif re.search(r"\bT\b", line):
            race["surface"] = "Turf"
        elif re.search(r"\bAW\b", line):
            race["surface"] = "All-Weather"

        # Finish position
        fin_match = re.search(r"\b([1-9]\d?)\s*(?:st|nd|rd|th)\b", line, re.I)
        if fin_match:
            race["finish"] = int(fin_match.group(1))

        # Beyer speed figure (usually a 2-3 digit number 60-120)
        numbers = re.findall(r"\b(\d{2,3})\b", line)
        beyer_candidates = [int(n) for n in numbers if 55 <= int(n) <= 130]
        if beyer_candidates:
            race["beyer"] = beyer_candidates[0]

        # Pace figures (E1, E2, LP) — appear as sequential numbers
        if len(beyer_candidates) >= 3:
            race["e1"] = beyer_candidates[1] if len(beyer_candidates) > 1 else None
            race["e2"] = beyer_candidates[2] if len(beyer_candidates) > 2 else None
            race["lp"] = beyer_candidates[3] if len(beyer_candidates) > 3 else None

        return race if race["date"] else None
