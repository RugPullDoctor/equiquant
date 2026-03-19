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
