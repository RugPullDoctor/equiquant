"""
EquiQuant — Startup Data Loader
Automatically runs when Railway starts up.
Loads race data and runs model if database is empty.
"""

import asyncio
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def startup_load():
    from database import SessionLocal, Race, init_db
    from datetime import date, timedelta
    import numpy as np

    init_db()
    db = SessionLocal()

    # Check if we already have data for today or tomorrow
    has_data = False
    for delta in range(3):
        d = (date.today() + timedelta(days=delta)).isoformat()
        if db.query(Race).filter(Race.race_date == d).count() > 0:
            has_data = True
            logger.info(f"Data already exists for {d} — skipping reload")
            break

    if has_data:
        db.close()
        return

    logger.info("No race data found — loading Equibase data...")
    db.close()

    # Load manual data (guaranteed to work without live scraping)
    from reload_equibase import main as reload_main
    await reload_main()

    # Run full model pipeline
    logger.info("Running model pipeline...")
    from full_model import run_full_pipeline
    run_full_pipeline()

    logger.info("Startup data load complete!")


if __name__ == "__main__":
    asyncio.run(startup_load())
