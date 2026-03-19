"""
Admin Router — trigger full data reload from browser
"""
from fastapi import APIRouter, BackgroundTasks
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/reload")
async def trigger_reload(background_tasks: BackgroundTasks):
    """Trigger full Equibase reload + model run from browser."""
    background_tasks.add_task(_run_reload)
    return {"status": "started", "message": "Full reload running — refresh dashboard in 15 seconds"}


@router.post("/runmodel")
async def trigger_model(background_tasks: BackgroundTasks):
    """Re-run model on existing data."""
    background_tasks.add_task(_run_model)
    return {"status": "started", "message": "Model running — refresh in 10 seconds"}


async def _run_reload():
    try:
        logger.info("Admin: Starting full Equibase reload...")
        import asyncio
        from reload_equibase import main as reload_main
        await reload_main()
        logger.info("Admin: Reload complete")
        _run_model_sync()
    except Exception as e:
        logger.error(f"Admin reload error: {e}")


def _run_model_sync():
    try:
        logger.info("Admin: Running full model pipeline...")
        from full_model import run_full_pipeline
        run_full_pipeline()
        logger.info("Admin: Model complete")
    except Exception as e:
        logger.error(f"Admin model error: {e}")


async def _run_model():
    _run_model_sync()
