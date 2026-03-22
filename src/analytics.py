import logging

from pathlib import Path
from config import DB_PATH

import pandas as pd
from sqlalchemy import create_engine, text

# ── Setup ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)




def get_engine():
    """Return a SQLAlchemy engine connected to the SQLite database."""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. "
            "Please run load_database.py first."
        )
    return create_engine(f"sqlite:///{DB_PATH}", echo=False)


def run_query(engine, sql: str) -> pd.DataFrame:
    """
    Helper used by every query function.
    Executes a SQL string and returns a DataFrame.
    Centralising this means we only have one place to fix
    if the SQLAlchemy API changes again.
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return pd.DataFrame(result.fetchall(), columns=result.keys())


# ── Query functions ───────────────────────────────────────────────────────────

def get_niche_benchmarks(engine) -> pd.DataFrame:
    """
    Query 1 — Niche overview benchmarks.
    Powers the Niche Overview page heatmap and bar charts.
    """
    return run_query(engine, """
        SELECT
            niche,
            total_channels,
            ROUND(avg_subscribers / 1000000, 2)  AS avg_subs_millions,
            ROUND(avg_views / 1000000, 2)         AS avg_views_millions,
            ROUND(avg_engagement_rate, 2)         AS avg_engagement_rate,
            ROUND(avg_videos_per_month, 1)        AS avg_videos_per_month,
            top_channel
        FROM niche_benchmarks
        ORDER BY avg_subscribers DESC
    """)


def get_channel_comparison(engine) -> pd.DataFrame:
    """
    Query 2 — All channels with key metrics for scatter plot.
    Powers the Channel Deep-Dive scatter plot.
    """
    return run_query(engine, """
        SELECT
            c.title,
            c.niche,
            c.subscriber_count,
            c.view_count,
            c.video_count,
            c.country,
            c.nlp_confidence,
            c.niche_match,
            ROUND(AVG(v.engagement_rate), 2)  AS avg_engagement_rate,
            COUNT(v.video_id)                 AS videos_analysed
        FROM channels c
        LEFT JOIN videos v ON c.channel_id = v.channel_id
        GROUP BY c.channel_id
        ORDER BY c.subscriber_count DESC
    """)


def get_top_videos_per_niche(engine, limit: int = 5) -> pd.DataFrame:
    """
    Query 3 — Top performing videos per niche by view count.
    Powers the Top Videos page.
    """
    # Fetch all videos with niche explicitly aliased
    df = run_query(engine, """
        SELECT
            v.niche           AS niche,
            v.title           AS title,
            c.title           AS channel,
            v.view_count      AS view_count,
            v.like_count      AS like_count,
            v.comment_count   AS comment_count,
            ROUND(v.engagement_rate, 2) AS engagement_rate,
            v.published_at    AS published_at
        FROM videos v
        JOIN channels c ON v.channel_id = c.channel_id
        WHERE v.view_count > 0
        ORDER BY v.view_count DESC
    """)

    if df.empty:
        return df

    print(f"Debug — columns after SQL: {list(df.columns)}")
    print(f"Debug — row count: {len(df)}")

    # Add rank within each niche without disturbing other columns
    df = df.copy()
    df["rank"] = df.groupby("niche")["view_count"].rank(
        method="first", ascending=False
    ).astype(int)

    # Filter to top N and clean up
    result = (
        df[df["rank"] <= limit]
        .drop(columns=["rank"])
        .sort_values(["niche", "view_count"], ascending=[True, False])
        .reset_index(drop=True)
    )

    return result


def get_engagement_vs_subscribers(engine) -> pd.DataFrame:
    """
    Query 4 — Engagement rate vs subscriber count per channel.
    Powers the scatter plot showing big channels vs engaged audiences.
    """
    return run_query(engine, """
        SELECT
            c.title,
            c.niche,
            c.subscriber_count,
            ROUND(AVG(v.engagement_rate), 3) AS avg_engagement_rate,
            COUNT(v.video_id)                AS videos_analysed
        FROM channels c
        JOIN videos v ON c.channel_id = v.channel_id
        GROUP BY c.channel_id
        HAVING videos_analysed >= 3
        ORDER BY c.subscriber_count DESC
    """)


def get_upload_cadence_by_niche(engine) -> pd.DataFrame:
    """
    Query 5 — Upload frequency and performance per channel.
    Powers the Growth Trajectory page.
    """
    return run_query(engine, """
        SELECT
            c.niche,
            c.title,
            COUNT(v.video_id)                AS recent_videos,
            ROUND(AVG(v.view_count), 0)      AS avg_views_per_video,
            ROUND(AVG(v.engagement_rate), 2) AS avg_engagement_rate,
            MIN(v.published_at)              AS earliest_video,
            MAX(v.published_at)              AS latest_video
        FROM channels c
        JOIN videos v ON c.channel_id = v.channel_id
        GROUP BY c.channel_id
        ORDER BY c.niche, avg_views_per_video DESC
    """)


def get_niche_engagement_distribution(engine) -> pd.DataFrame:
    """
    Query 6 — Per-video engagement rates across all niches.
    Powers the box plot showing engagement spread within each niche.
    """
    return run_query(engine, """
        SELECT
            niche,
            ROUND(engagement_rate, 3) AS engagement_rate,
            view_count,
            title
        FROM videos
        WHERE engagement_rate > 0
          AND view_count > 1000
        ORDER BY niche, engagement_rate DESC
    """)


def get_nlp_audit_trail(engine) -> pd.DataFrame:
    """
    Query 7 — NLP classification audit trail.
    Powers the NLP Audit page showing classification confidence.
    """
    return run_query(engine, """
        SELECT
            title,
            api_niche,
            nlp_niche,
            ROUND(nlp_confidence * 100, 1) AS confidence_pct,
            niche_match,
            SUBSTR(description, 1, 120)    AS description_preview
        FROM channels
        ORDER BY niche_match ASC, nlp_confidence DESC
    """)


def get_country_distribution(engine) -> pd.DataFrame:
    """
    Query 8 — Channel count and avg subscribers by country.
    Powers the geographic breakdown chart.
    """
    return run_query(engine, """
        SELECT
            COALESCE(country, 'Unknown')               AS country,
            COUNT(*)                                   AS channel_count,
            ROUND(AVG(subscriber_count) / 1000000, 2) AS avg_subs_millions
        FROM channels
        GROUP BY country
        HAVING channel_count >= 2
        ORDER BY channel_count DESC
    """)


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    engine = get_engine()
    log.info("Running analytical queries...\n")

    queries = {
        "1. Niche benchmarks":          get_niche_benchmarks,
        "2. Channel comparison":        get_channel_comparison,
        "3. Top videos per niche":      get_top_videos_per_niche,
        "4. Engagement vs subscribers": get_engagement_vs_subscribers,
        "5. Upload cadence by niche":   get_upload_cadence_by_niche,
        "6. Engagement distribution":   get_niche_engagement_distribution,
        "7. NLP audit trail":           get_nlp_audit_trail,
        "8. Country distribution":      get_country_distribution,
    }

    for name, func in queries.items():
        try:
            df = func(engine)
            log.info(f"Query {name}")
            log.info(f"  Rows returned : {len(df)}")
            log.info(f"  Columns       : {list(df.columns)}\n")
        except Exception as e:
            log.error(f"Query {name} failed: {e}\n")

    log.info("All queries complete. Ready to build the dashboard.")