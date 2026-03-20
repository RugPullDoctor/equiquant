"""
EquiQuant — Fresh Santa Anita Inspector
Checks what the site looks like RIGHT NOW for today's races.
Run from C:\EquiQuant:  python inspect_today.py
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import date

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

TODAY = date.today().isoformat()

URLS = [
    f"https://www.santaanita.com/racing-information/entries/{TODAY}/",
    "https://www.santaanita.com/racing-information/entries/",
    "https://www.santaanita.com/racing-information/",
    f"https://www.equibase.com/static/entry/SA0{date.today().month}{date.today().year}USA-EQB.html",
]

async def inspect():
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for url in URLS:
            print(f"\n{'='*60}")
            print(f"URL: {url}")
            print(f"{'='*60}")
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    print(f"Status: {resp.status}")
                    if resp.status != 200:
                        continue
                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")
                    print(f"Page size: {len(html):,} chars")

                    # Show all tables
                    tables = soup.find_all("table")
                    print(f"\nTables found: {len(tables)}")
                    for i, t in enumerate(tables[:5]):
                        rows = t.find_all("tr")
                        print(f"\n  Table {i+1} ({len(rows)} rows):")
                        for j, row in enumerate(rows[:4]):
                            cells = [c.get_text(strip=True)[:20] for c in row.find_all(["td","th"])]
                            if cells: print(f"    Row {j+1}: {cells}")

                    # Show race-related links
                    print(f"\nRace links:")
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if any(kw in href.lower() for kw in ["race","entr","result","card"]):
                            print(f"  {href}")

                    # Show page text snippet
                    text = soup.get_text()
                    print(f"\nPage text (first 1000 chars):")
                    import re
                    print(re.sub(r'\s+', ' ', text)[:1000])

            except Exception as e:
                print(f"Error: {e}")
            await asyncio.sleep(1)

asyncio.run(inspect())
