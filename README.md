# Creator Economy Benchmarker

An end-to-end data pipeline that pulls live YouTube channel data,
auto-classifies creators into niches using NLP, stores everything
in a SQL database, and surfaces insights in an interactive Plotly
Dash dashboard.

## Business problem
No free tool exists to benchmark YouTube creator niches side by side
with real data. This project solves that for agencies, brand managers,
and aspiring creators.

## Tech stack
| Layer | Technology |
|---|---|
| Data source | YouTube Data API v3 |
| Language | Python 3.11 |
| NLP | spaCy |
| Storage | SQLite via SQLAlchemy |
| Analytics | Pandas + SQL |
| Dashboard | Plotly Dash |
| Hosting | Render |

## Architecture
```
search_channels.py   →   fetch_channels.py   →   classify_niches.py
handles → IDs            IDs → statistics        descriptions → niches
      ↓                        ↓                        ↓
channel_ids.json         channels_raw.json         SQLite database
                                                         ↓
                                                   analytics.py
                                                         ↓
                                                   Plotly Dashboard
```

## Setup

### 1. Clone the repo
    git clone https://github.com/yourusername/creator-benchmarker.git
    cd creator-benchmarker

### 2. Create and activate Conda environment
    conda env create -f environment.yml
    conda activate creator-benchmarker

### 3. Add your YouTube API key
    cp .env.example .env
    # Edit .env and add your YouTube Data API v3 key

### 4. Run the pipeline
    python src/search_channels.py
    python src/fetch_channels.py
    python src/classify_niches.py
    python src/load_database.py
    python src/analytics.py

### 5. Launch the dashboard
    python dashboard/app.py

## Project structure
    creator-benchmarker/
    ├── data/
    │   ├── raw/          ← API responses (gitignored, regenerate with scripts)
    │   └── db/           ← SQLite database (gitignored, regenerate with scripts)
    ├── src/
    │   ├── search_channels.py
    │   ├── fetch_channels.py
    │   ├── classify_niches.py
    │   └── load_database.py
    ├── dashboard/
    │   └── app.py
    ├── environment.yml
    ├── requirements.txt
    └── README.md

## Dashboard pages
- **Niche Overview** — avg subscribers, engagement rate, upload frequency
- **Channel Deep-Dive** — compare any channel vs its niche peers
- **Growth Trajectory** — which niches are accelerating vs plateauing

## Live demo
Coming soon
