"""
EquiQuant — Santa Anita Page Inspector
Fetches the real Santa Anita page and shows us exactly what HTML we're working with.
Run from C:\EquiQuant:  python inspect_sa.py
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

URLS_TO_TRY = [
    "https://www.santaanita.com/racing-information/",
    "https://www.santaanita.com/racing-information/entries/",
    "https://www.santaanita.com/racing-information/past-performances/",
]

async def inspect():
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for url in URLS_TO_TRY:
            print(f"\n{'='*60}")
            print(f"Fetching: {url}")
            print('='*60)

            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    print(f"Status: {resp.status}")
                    if resp.status != 200:
                        print("Skipping — not 200 OK")
                        continue

                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")

                    # Show page title
                    title = soup.find("title")
                    print(f"Page title: {title.get_text() if title else 'None'}")

                    # Show all links that mention race/entry/card
                    print("\n--- Race-related links ---")
                    count = 0
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        text = a.get_text(strip=True)
                        if any(kw in href.lower() or kw in text.lower()
                               for kw in ["race", "entry", "entries", "card", "program", "pp"]):
                            print(f"  [{text[:40]:40s}] {href}")
                            count += 1
                            if count >= 20:
                                print("  ... (truncated)")
                                break

                    if count == 0:
                        print("  No race links found")

                    # Show all tables
                    tables = soup.find_all("table")
                    print(f"\n--- Tables found: {len(tables)} ---")
                    for i, table in enumerate(tables[:5]):
                        rows = table.find_all("tr")
                        print(f"\n  Table {i+1}: {len(rows)} rows")
                        for j, row in enumerate(rows[:4]):
                            cells = [c.get_text(strip=True)[:20] for c in row.find_all(["td","th"])]
                            print(f"    Row {j+1}: {cells}")

                    # Show key CSS classes/IDs that might be race data
                    print("\n--- Key elements with race-related IDs/classes ---")
                    for el in soup.find_all(True):
                        el_id = el.get("id", "")
                        el_cls = " ".join(el.get("class", []))
                        combined = (el_id + " " + el_cls).lower()
                        if any(kw in combined for kw in ["race", "entry", "horse", "runner", "program"]):
                            text = el.get_text(strip=True)[:60]
                            print(f"  <{el.name} id='{el_id}' class='{el_cls[:40]}'> {text}")

            except Exception as e:
                print(f"Error: {e}")

    print("\n" + "="*60)
    print("Inspection complete!")
    print("Share this output so we can fix the scraper selectors.")

asyncio.run(inspect())
