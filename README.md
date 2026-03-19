# EquiQuant AI — Horse Racing Analytics Platform

> Benter-style quantitative model for Santa Anita Park  
> Python FastAPI backend · SQLite/Postgres · React frontend

---

## Project Structure

```
equiquant/
├── backend/
│   ├── main.py                  # FastAPI app entrypoint
│   ├── database.py              # SQLAlchemy models (Race, Horse, Bet, etc.)
│   ├── model.py                 # Benter regression model + Kelly calculator
│   ├── feature_engineering.py  # 108-variable feature computation
│   ├── scheduler.py             # APScheduler: daily scrape jobs
│   ├── requirements.txt
│   ├── .env                     # Your config (copy from .env.example)
│   ├── scrapers/
│   │   ├── santa_anita.py       # Scrapes santaanita.com/racing-information
│   │   └── freepps.py           # Downloads + parses PPs from thefreepps.com
│   └── routers/
│       ├── races.py             # GET /api/races/today, etc.
│       ├── scraper.py           # POST /api/scraper/run
│       ├── model.py             # GET /api/model/inference
│       └── kelly.py             # GET /api/kelly/bets/today
└── frontend/
    └── racing_ai_dashboard.html  # The dashboard (copy from Claude output)
```

---

## Quick Start (Cursor)

### 1. Open in Cursor
```
File → Open Folder → select the `equiquant/` folder
```

### 2. Create virtual environment
```bash
cd backend
python -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment
```bash
cp .env.example .env
# Edit .env with your settings
```

### 5. Run the server
```bash
uvicorn main:app --reload --port 8000
```

### 6. Open the dashboard
```
http://localhost:8000
```

### 7. Trigger first scrape manually
```bash
curl -X POST http://localhost:8000/api/scraper/run
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/races/today` | Today's full race card with model outputs |
| GET | `/api/races/{date}` | Race card for specific date (YYYY-MM-DD) |
| GET | `/api/races/{date}/{num}/analysis` | Per-race edge analysis |
| POST | `/api/scraper/run` | Trigger full scrape pipeline |
| POST | `/api/scraper/santa-anita` | Scrape race card only |
| POST | `/api/scraper/freepps` | Download + parse PPs |
| GET | `/api/scraper/status` | Recent scraper run logs |
| GET | `/api/model/variables` | Variable importance weights |
| GET | `/api/model/inference/{date}/{race}` | Run model on specific race |
| POST | `/api/model/train` | Retrain on historical data |
| GET | `/api/kelly/bets/today` | Today's recommended bets |
| GET | `/api/kelly/bets/today?kelly_fraction=0.25&min_edge=0.05` | Filtered bets |

---

## The Model — How It Works

### Step 1: Data Collection
- **Santa Anita scraper** → race card, entries, jockey/trainer, morning line odds
- **FreePPs scraper** → past performances: Beyer figures, pace ratings, race history

### Step 2: Feature Engineering (108 variables)

| Group | Count | Examples |
|-------|-------|---------|
| Performance | 34 | Beyer figures, pace E1/E2/LP, class rating, speed trend |
| Jockey/Trainer | 28 | Win%, ITM%, J×T combo, layoff stats, SA-specific stats |
| Track/Situational | 25 | Post position bias, track condition, surface, field size |
| Form/Fitness | 21 | Days since last, weight, surface switch, equipment |

### Step 3: Benter Regression

```
score(i) = β₀ + β₁·beyer + β₂·jockey_pct + β₃·post_bias + ... + βₙ·xₙ

P_model(i wins) = softmax(score) = exp(score_i) / Σ exp(score_j)
```

### Step 4: Edge Calculation

```
Edge(i) = P_model(i) − P_track(i)

where P_track = 1 / decimal_odds = denominator / (numerator + denominator)
```

### Step 5: Kelly Criterion

```
f* = (b·p − q) / b        Full Kelly
bet = f* × 0.25 × bankroll  25% Fractional Kelly (safer)
```

---

## Scraper Notes

### Santa Anita (santaanita.com)
- The scraper uses BeautifulSoup to parse the race entries page
- **You may need to adjust CSS selectors** in `scrapers/santa_anita.py` 
  if the site HTML changes. Use Cursor's browser preview or DevTools to 
  inspect the actual HTML and update `_parse_race_page()` accordingly.
- Polite 1.5s delay between requests (configurable)

### FreePPs (thefreepps.com)
- Downloads the daily PP PDF for Santa Anita
- Parses with `pdfplumber` — PDF layout can vary by provider
- The `_parse_horse_block()` regex patterns may need tuning for the 
  actual PDF format. Open a sample PDF and check column positions.
- Cached locally in `./data/pps/` to avoid re-downloading

### Common Issues
- **403 errors**: Site may block scrapers — try adjusting User-Agent or 
  adding session cookies. Some PP data requires free registration.
- **PDF parsing accuracy**: PP PDFs are notoriously inconsistent. 
  Print a sample and compare to parsed output. Adjust `_parse_past_race_line()`.
- **Missing horses**: Some horses may not have FreePPs data — the model 
  gracefully handles missing features with 0/null defaults.

---

## Scheduler (Automated Pipeline)

The scheduler runs automatically when the server starts:

| Time (PT) | Job |
|-----------|-----|
| 7:30 AM | Morning scrape — race card + FreePPs |
| Every 5 min (10am–5pm) | Live odds refresh |
| 6:00 PM | Load race results |
| 9:00 PM | Nightly model retraining |

---

## Connecting the Dashboard

Copy your `racing_ai_dashboard.html` to `frontend/`. Then update the 
dashboard's JS to fetch live data from the API:

```javascript
// Replace static race data with:
const resp = await fetch('/api/races/today');
const { races } = await resp.json();

// Replace static kelly data with:
const bets = await fetch('/api/kelly/bets/today?kelly_fraction=0.25');
```

---

## Roadmap

- [ ] Live tote board odds integration (Equibase API or DRF)
- [ ] Place/Show probability (Harville formula extension)
- [ ] Exacta/Trifecta combination betting
- [ ] Historical backtesting engine
- [ ] PostgreSQL migration for production scale
- [ ] Deployed cloud version (Railway / Render)

---

## Legal Notice

This platform is for **research and simulation purposes**. Verify all 
applicable laws before using model outputs for actual wagering in your jurisdiction.
