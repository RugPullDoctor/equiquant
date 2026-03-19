"""
EquiQuant AI — FastAPI Backend
Racing data scraper, feature engineering, and model API
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import asyncio
import logging
from datetime import datetime

from routers import races, scraper, model, kelly
from scheduler import start_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("EquiQuant AI starting up...")
    await start_scheduler()
    yield
    logger.info("EquiQuant AI shutting down.")


app = FastAPI(
    title="EquiQuant AI",
    description="Benter-style horse racing analytics API — Santa Anita Park",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(races.router, prefix="/api/races", tags=["Races"])
app.include_router(scraper.router, prefix="/api/scraper", tags=["Scraper"])
app.include_router(model.router, prefix="/api/model", tags=["Model"])
app.include_router(kelly.router, prefix="/api/kelly", tags=["Kelly"])

# Serve the frontend dashboard
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")


@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat(), "version": "1.0.0"}
