"""
EquiQuant AI — FastAPI Backend
Railway-compatible with auto startup data loading
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import logging
import os
from datetime import datetime
from pathlib import Path

from routers import races, scraper, model, kelly, admin
from scheduler import start_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("EquiQuant AI starting up...")
    await start_scheduler()

    try:
        from startup import startup_load
        await startup_load()
    except Exception as e:
        logger.warning(f"Startup data load skipped: {e}")

    yield
    logger.info("EquiQuant AI shutting down.")


app = FastAPI(
    title="EquiQuant AI",
    description="Benter-style horse racing analytics — Santa Anita Park",
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

app.include_router(races.router,   prefix="/api/races",   tags=["Races"])
app.include_router(scraper.router, prefix="/api/scraper", tags=["Scraper"])
app.include_router(model.router,   prefix="/api/model",   tags=["Model"])
app.include_router(kelly.router,   prefix="/api/kelly",   tags=["Kelly"])
app.include_router(admin.router,   prefix="/api/admin",   tags=["Admin"])


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0",
        "environment": os.getenv("RAILWAY_ENVIRONMENT", "local"),
    }


# Serve frontend
frontend_path = Path(__file__).parent / "frontend"
if frontend_path.exists():
    app.mount("/", StaticFiles(directory=str(frontend_path), html=True), name="frontend")
else:
    logger.warning("Frontend folder not found — API-only mode")
