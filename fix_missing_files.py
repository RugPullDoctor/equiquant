"""
EquiQuant AI — Fix Missing Files
Run from C:\EquiQuant to create database.py and model.py
"""

import os

FILES = {}

FILES["database.py"] = '''
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

DATABASE_URL = "sqlite:///./equiquant.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Race(Base):
    __tablename__ = "races"
    id              = Column(Integer, primary_key=True, index=True)
    race_date       = Column(String, index=True)
    track           = Column(String, default="Santa Anita")
    race_number     = Column(Integer)
    race_name       = Column(String)
    distance        = Column(String)
    surface         = Column(String)
    purse           = Column(Float)
    condition       = Column(String)
    post_time       = Column(String)
    track_condition = Column(String)
    weather         = Column(String)
    created_at      = Column(DateTime, default=datetime.utcnow)
    updated_at      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Horse(Base):
    __tablename__ = "horses"
    id                  = Column(Integer, primary_key=True, index=True)
    race_id             = Column(Integer, index=True)
    race_date           = Column(String, index=True)
    post_position       = Column(Integer)
    horse_name          = Column(String, index=True)
    jockey              = Column(String)
    trainer             = Column(String)
    morning_line_odds   = Column(String)
    live_odds           = Column(String)
    weight              = Column(Integer)
    age                 = Column(Integer)
    scratched           = Column(Boolean, default=False)
    beyer_last          = Column(Float)
    beyer_2back         = Column(Float)
    beyer_3back         = Column(Float)
    beyer_avg_3         = Column(Float)
    pace_e1             = Column(Float)
    pace_e2             = Column(Float)
    pace_lp             = Column(Float)
    days_since_last     = Column(Integer)
    surface_switch      = Column(Boolean, default=False)
    distance_switch     = Column(Boolean, default=False)
    class_rating        = Column(Float)
    jockey_win_pct_90d  = Column(Float)
    trainer_win_pct_90d = Column(Float)
    post_position_bias  = Column(Float)
    field_size          = Column(Integer)
    model_win_prob      = Column(Float)
    model_place_prob    = Column(Float)
    model_show_prob     = Column(Float)
    edge                = Column(Float)
    kelly_fraction      = Column(Float)
    kelly_bet_amount    = Column(Float)
    raw_features        = Column(JSON)
    created_at          = Column(DateTime, default=datetime.utcnow)
    updated_at          = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RaceResult(Base):
    __tablename__ = "race_results"
    id           = Column(Integer, primary_key=True)
    race_id      = Column(Integer, index=True)
    race_date    = Column(String)
    winner       = Column(String)
    place        = Column(String)
    show         = Column(String)
    win_payout   = Column(Float)
    place_payout = Column(Float)
    show_payout  = Column(Float)
    created_at   = Column(DateTime, default=datetime.utcnow)


class Bet(Base):
    __tablename__ = "bets"
    id          = Column(Integer, primary_key=True)
    race_date   = Column(String)
    race_id     = Column(Integer)
    horse_id    = Column(Integer)
    horse_name  = Column(String)
    bet_type    = Column(String)
    amount      = Column(Float)
    odds        = Column(String)
    model_prob  = Column(Float)
    edge        = Column(Float)
    result      = Column(String)
    payout      = Column(Float, default=0.0)
    profit_loss = Column(Float, default=0.0)
    created_at  = Column(DateTime, default=datetime.utcnow)


class ScraperLog(Base):
    __tablename__ = "scraper_logs"
    id          = Column(Integer, primary_key=True)
    source      = Column(String)
    status      = Column(String)
    records     = Column(Integer, default=0)
    message     = Column(Text)
    duration_ms = Column(Integer)
    created_at  = Column(DateTime, default=datetime.utcnow)


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
'''

FILES["model.py"] = '''
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

INITIAL_WEIGHTS = {
    "beyer_last":               0.048,
    "beyer_avg_3":              0.031,
    "beyer_trend":              0.022,
    "pace_e1":                  0.018,
    "pace_lp":                  0.025,
    "bris_prime_power":         0.039,
    "speed_rank_pct":           0.082,
    "jockey_win_pct_90d":       1.240,
    "jockey_sa_win_pct":        0.890,
    "trainer_win_pct_90d":      1.180,
    "jockey_trainer_combo_win": 1.540,
    "post_position_bias":       2.100,
    "inside_pp_flag":           0.145,
    "track_condition_fast":     0.082,
    "field_size_factor":        0.210,
    "days_since_last":         -0.004,
    "is_fresh":                 0.088,
    "is_layoff_60plus":        -0.121,
    "surface_switch_flag":     -0.142,
    "win_pct_career":           0.610,
    "itm_pct_career":           0.420,
    "last_race_winner":         0.195,
    "pace_scenario_lone_speed": 0.312,
}


class BenterModel:
    def __init__(self):
        self.weights = INITIAL_WEIGHTS.copy()
        self.intercept = 0.0

    def score_horse(self, features: dict) -> float:
        score = self.intercept
        for feat, beta in self.weights.items():
            val = features.get(feat, 0.0) or 0.0
            score += beta * float(val)
        return score

    def predict_race(self, feature_vectors: list) -> list:
        if not feature_vectors:
            return []
        scores = [self.score_horse(fv) for fv in feature_vectors]
        max_score = max(scores)
        exp_scores = [np.exp(s - max_score) for s in scores]
        total = sum(exp_scores)
        return [e / total for e in exp_scores]

    def compute_edge(self, model_prob: float, track_odds: str) -> float:
        return model_prob - self._odds_to_prob(track_odds)

    def kelly_fraction(self, model_prob: float, track_odds: str) -> float:
        decimal_odds = self._odds_to_decimal(track_odds)
        b = decimal_odds - 1.0
        p = model_prob
        q = 1.0 - p
        f = (b * p - q) / b if b > 0 else 0.0
        return max(0.0, f)

    def bet_size(self, model_prob, track_odds, bankroll, kelly_fraction=0.25, max_bet=25000.0):
        edge = self.compute_edge(model_prob, track_odds)
        if edge <= 0:
            return 0.0
        f = self.kelly_fraction(model_prob, track_odds)
        return min(f * kelly_fraction * bankroll, max_bet)

    def analyze_race(self, horses, feature_vectors, bankroll, kelly_frac=0.25):
        probs = self.predict_race(feature_vectors)
        results = []
        for horse, fv, prob in zip(horses, feature_vectors, probs):
            odds = horse.get("live_odds") or horse.get("morning_line_odds") or "9/1"
            edge = self.compute_edge(prob, odds)
            bet = self.bet_size(prob, odds, bankroll, kelly_frac)
            results.append({
                **horse,
                "model_win_prob": round(prob, 4),
                "track_implied_prob": round(self._odds_to_prob(odds), 4),
                "edge": round(edge, 4),
                "edge_pct": round(edge * 100, 2),
                "kelly_bet": round(bet, 2),
                "bet_recommended": edge > 0.04,
            })
        return results

    def _odds_to_prob(self, odds):
        try:
            if "/" in str(odds):
                n, d = odds.split("/")
                return float(d) / (float(n) + float(d))
            return 1.0 / float(odds)
        except Exception:
            return 0.10

    def _odds_to_decimal(self, odds):
        try:
            if "/" in str(odds):
                n, d = odds.split("/")
                return (float(n) + float(d)) / float(d)
            return float(odds)
        except Exception:
            return 10.0
'''

FILES["scheduler.py"] = '''
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from database import SessionLocal, init_db

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()


async def start_scheduler():
    init_db()
    logger.info("Database initialized")
    scheduler.add_job(run_morning_scrape, CronTrigger(hour=7, minute=30, timezone="America/Los_Angeles"),
                      id="morning_scrape", name="Morning Race Card Scrape")
    scheduler.start()
    logger.info("Scheduler started")


async def run_morning_scrape():
    logger.info("[Scheduler] Starting morning scrape")
    from scrapers.santa_anita import SantaAnitaScraper
    db = SessionLocal()
    try:
        sa = SantaAnitaScraper()
        result = await sa.scrape_race_card()
        logger.info(f"[Scheduler] {len(result.get(\'races\', []))} races scraped")
    except Exception as e:
        logger.error(f"[Scheduler] Error: {e}")
    finally:
        db.close()
'''

# ── WRITE FILES ───────────────────────────────────────────────────────────────
for filepath, content in FILES.items():
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content.lstrip("\n"))
    print(f"  Created: {filepath}")

print("\n All files created!")
print("  Now run: uvicorn main:app --reload --port 8000")
