# Creator Economy Benchmarker

An end-to-end data pipeline that pulls live YouTube channel data,
auto-classifies creators into niches using NLP, stores everything
in a SQL database, and surfaces insights in an interactive dashboard.

## Live demo
👉 [creator-benchmarker(https://creator-benchmark.onrender.com/)

> First load may take 30 seconds — Render free tier spins down after inactivity.

---

## What it does

| Stage | Script | What happens |
|---|---|---|
| 0 | run_pipeline.py | Orchestrates all pipeline stages — skips if database already exists |
| 1 | search_channels.py | Resolves 80 YouTube handles → channel IDs (1 API unit each) |
| 2 | fetch_channels.py | Fetches subscriber count, views, video count per channel |
| 2.5 | fetch_videos.py | Fetches last 10 videos per channel — views, likes, comments |
| 3 | classify_niches.py | NLP classifies each channel into a niche from description text |
| 4 | load_database.py | Loads all data into SQLite with a clean 3-table schema |
| 5 | analytics.py | 8 SQL analytical queries powering the dashboard |
| 6 | dashboard/app.py | 4-page Plotly Dash interactive dashboard |

---

## Dashboard pages

### Page 1 — Niche Overview
Side-by-side benchmarks across 8 niches.
- Average subscribers, engagement rate, upload frequency
- Normalised benchmark heatmap

### Page 2 — Channel Deep-Dive
Compare any channel against its niche peers.
- Scatter plot: subscribers vs engagement rate
- Filterable, sortable channel rankings table

### Page 3 — Top Videos
Best performing videos per niche by view count.
- Niche filter dropdown
- Top 5 videos with engagement stats

### Page 4 — NLP Audit Trail
Shows how the NLP classifier auto-assigned niches from raw text.
- Accuracy gauge: 83%
- Match vs mismatch breakdown by niche
- Full classification table with confidence scores

---

## Key findings

- **Beauty** has the highest engagement rate (7.11%) despite mid-table subscribers
- **Gaming** leads on average subscribers (34.93M) but mid-table engagement
- **Finance** has the lowest engagement (2.45%) — counterintuitive given passionate audiences
- **Tech** uploads most frequently (8.2 videos/month) yet maintains 4% engagement
- NLP classifier achieves **86% accuracy** auto-labelling niches from description text

---

## Tech stack

| Layer | Technology |
|---|---|
| Data source | YouTube Data API v3 |
| Language | Python 3.11 |
| NLP | spaCy en_core_web_sm |
| Storage | SQLite via SQLAlchemy |
| Analytics | Pandas + raw SQL |
| Dashboard | Plotly Dash |
| Deployment | Render.com |

---

## Run it locally

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/creator-benchmarker.git
cd creator-benchmarker
```

### 2. Create Conda environment
```bash
conda env create -f environment.yml
conda activate creator-benchmarker
```

### 3. Add your YouTube API key
```bash
cp .env.example .env
# Edit .env and paste your YouTube Data API v3 key
# Get a free key at https://console.cloud.google.com
```

### 4. Run the full pipeline
```bash
python src/run_pipeline.py
```

This single command orchestrates all 5 pipeline stages automatically.
It skips execution if the database already exists — saving API quota on reruns.

To force a full refresh delete the database and rerun:
```bash
rm data/db/creator_benchmarker.db
python src/run_pipeline.py
```

### 5. Launch the dashboard
```bash
python dashboard/app.py
# Open http://127.0.0.1:8050
```

---
## Deployment

Deployed on [Render.com](https://render.com) using a persistent disk.

The start command runs `run_pipeline.py` which:
1. Checks if the database already exists on the persistent disk
2. If not — runs all 5 pipeline stages to build it from scratch
3. If yes — skips the pipeline entirely and starts gunicorn immediately

This means the first deploy takes ~3 minutes to build the database.
Every subsequent restart starts in seconds.

### Environment variables required on Render
| Variable | Description |
|---|---|
| `YOUTUBE_API_KEY` | Your YouTube Data API v3 key from Google Cloud Console |

---
## Project structure
```
creator-benchmarker/
├── data/
│   ├── raw/              ← API responses (gitignored)
│   └── db/               ← SQLite database (gitignored)
├── src/
│   ├── config.py
│   ├── run_pipeline.py
│   ├── search_channels.py
│   ├── fetch_channels.py
│   ├── fetch_videos.py
│   ├── classify_niches.py
│   ├── load_database.py
│   └── analytics.py
├── dashboard/
│   └── app.py
├── environment.yml
├── requirements.txt
├── Procfile
└── render.yaml
```

---

## API quota usage
| Script | Units used |
|---|---|
| search_channels.py | ~80 units |
| fetch_channels.py | ~80 units |
| fetch_videos.py | ~96 units |
| **Total** | **~256 / 10,000 daily** |

> `run_pipeline.py` checks if the database already exists before running.
> Subsequent deploys and restarts consume zero API quota.