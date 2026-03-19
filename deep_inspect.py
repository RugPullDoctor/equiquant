"""
EquiQuant — Deep Page Inspector
Fetches the March 20 entries page and shows ALL available data.
Run from C:\EquiQuant:  python deep_inspect.py
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import json

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

URLS = [
    "https://www.santaanita.com/racing-information/entries/2026-03-20/",
    "https://www.equibase.com/static/entry/SA032026USA-EQB.html",
    "https://www.drf.com/entries/entryDetails/id/SA/country/USA/date/03202026",
]

async def inspect():
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for url in URLS:
            print(f"\n{'='*70}")
            print(f"URL: {url}")
            print('='*70)

            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    print(f"Status: {resp.status}")
                    if resp.status != 200:
                        print("Skipping.")
                        continue

                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")

                    print(f"Title: {soup.find('title').get_text() if soup.find('title') else 'N/A'}")
                    print(f"Page size: {len(html):,} chars")

                    # ── ALL TABLES ──────────────────────────────────────────
                    tables = soup.find_all("table")
                    print(f"\nTables: {len(tables)}")
                    for i, t in enumerate(tables):
                        rows = t.find_all("tr")
                        print(f"\n  Table {i+1} ({len(rows)} rows):")
                        for j, row in enumerate(rows[:6]):
                            cells = [c.get_text(strip=True)[:25] for c in row.find_all(["td","th"])]
                            if cells:
                                print(f"    Row {j+1}: {cells}")

                    # ── LOOK FOR JOCKEY/TRAINER TEXT ────────────────────────
                    print(f"\n--- Jockey/Trainer mentions ---")
                    text = soup.get_text()
                    lines = [l.strip() for l in text.split('\n') if l.strip()]
                    for line in lines:
                        if any(kw in line.lower() for kw in ['jockey', 'trainer', 'rider', 'ml ', 'morning line', 'odds']):
                            print(f"  {line[:80]}")

                    # ── LOOK FOR ODDS PATTERNS ──────────────────────────────
                    print(f"\n--- Odds patterns (X/Y format) ---")
                    odds_found = re.findall(r'\b\d+/\d+\b', text)
                    if odds_found:
                        print(f"  Found: {list(set(odds_found))[:20]}")
                    else:
                        print("  None found")

                    # ── JSON/API DATA EMBEDDED IN PAGE ──────────────────────
                    print(f"\n--- Embedded JSON data ---")
                    scripts = soup.find_all("script")
                    for script in scripts:
                        content = script.string or ""
                        if any(kw in content.lower() for kw in ['jockey', 'trainer', 'odds', 'horse', 'entries']):
                            # Find JSON objects
                            json_matches = re.findall(r'\{[^{}]{20,500}\}', content)
                            for match in json_matches[:3]:
                                print(f"  {match[:200]}")

                    # ── ALL TEXT CONTENT (first 3000 chars) ─────────────────
                    print(f"\n--- Page text (first 3000 chars) ---")
                    clean_text = re.sub(r'\s+', ' ', text)
                    print(clean_text[:3000])

            except Exception as e:
                print(f"Error: {e}")
            
            await asyncio.sleep(1)

    print(f"\n{'='*70}")
    print("Deep inspection complete!")

asyncio.run(inspect())
