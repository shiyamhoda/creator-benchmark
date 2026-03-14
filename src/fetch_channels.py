import os
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── Setup ────────────────────────────────────────────────────────────────────

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

BASE_DIR      = Path(__file__).parent.parent
RAW_DATA_PATH = BASE_DIR / "data" / "raw"
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

# ── Functions ────────────────────────────────────────────────────────────────

def build_youtube_client():
    """Create and return a YouTube API client."""
    return build("youtube", "v3", developerKey=API_KEY)


def load_channel_ids() -> dict:
    """
    Load niche → channel_id mapping from the file
    produced by search_channels.py.
    """
    ids_path = RAW_DATA_PATH / "channel_ids.json"

    if not ids_path.exists():
        raise FileNotFoundError(
            f"channel_ids.json not found at {ids_path}. "
            "Please run search_channels.py first."
        )

    with open(ids_path, encoding="utf-8") as f:
        data = json.load(f)

    total = sum(len(ids) for ids in data.values())
    log.info(f"Loaded {total} channel IDs across {len(data)} niches")
    return data


def fetch_channel_data(youtube, channel_id: str, niche: str) -> dict | None:
    """
    Fetch channel metadata and statistics from YouTube API.
    Returns a clean dict or None if the request fails.
    """
    try:
        response = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=channel_id
        ).execute()

        items = response.get("items", [])
        if not items:
            log.warning(f"No data returned for channel ID: {channel_id}")
            return None

        item       = items[0]
        snippet    = item.get("snippet",    {})
        statistics = item.get("statistics", {})

        return {
            "channel_id":       channel_id,
            "niche":            niche,
            "title":            snippet.get("title",        ""),
            "description":      snippet.get("description",  ""),
            "country":          snippet.get("country",      "Unknown"),
            "published_at":     snippet.get("publishedAt",  ""),
            "subscriber_count": int(statistics.get("subscriberCount", 0)),
            "video_count":      int(statistics.get("videoCount",      0)),
            "view_count":       int(statistics.get("viewCount",       0)),
        }

    except HttpError as e:
        log.error(f"HTTP error fetching channel {channel_id}: {e}")
        return None


def fetch_all_channels(channels_by_niche: dict) -> list[dict]:
    """
    Fetch full statistics for every channel ID in the mapping.
    Returns a list of channel dicts ready for storage.
    """
    youtube      = build_youtube_client()
    all_channels = []
    total        = sum(len(ids) for ids in channels_by_niche.values())
    count        = 0

    for niche, channel_ids in channels_by_niche.items():
        log.info(f"Fetching niche: {niche} ({len(channel_ids)} channels)")

        for channel_id in channel_ids:
            count += 1
            log.info(f"  [{count}/{total}] Fetching: {channel_id}")

            data = fetch_channel_data(youtube, channel_id, niche)
            if data:
                all_channels.append(data)
                log.info(f"  ✅ {data['title']} | {data['subscriber_count']:,} subscribers")

            time.sleep(0.5)

    return all_channels


def save_raw_data(channels: list[dict]) -> None:
    """Save fetched channel data to JSON."""
    output_path = RAW_DATA_PATH / "channels_raw.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(channels, f, indent=2, ensure_ascii=False)
    log.info(f"Saved {len(channels)} channels to {output_path}")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting YouTube channel data fetch...")
    channels_by_niche = load_channel_ids()
    all_channels      = fetch_all_channels(channels_by_niche)
    save_raw_data(all_channels)
    log.info(f"Done! Fetched {len(all_channels)} channels total.")