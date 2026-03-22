import os
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import RAW_DATA_PATH
# ── Setup ────────────────────────────────────────────────────────────────────

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)


# How many recent videos to fetch per channel
VIDEOS_PER_CHANNEL = 10


def build_youtube_client():
    """Create and return a YouTube API client."""
    return build("youtube", "v3", developerKey=API_KEY)


def load_channels() -> list[dict]:
    """
    Load channel data from channels_raw.json.
    We only need channel_id, title and niche from each record.
    """
    channels_path = RAW_DATA_PATH / "channels_raw.json"

    if not channels_path.exists():
        raise FileNotFoundError(
            f"channels_raw.json not found at {channels_path}. "
            "Please run fetch_channels.py first."
        )

    with open(channels_path, encoding="utf-8") as f:
        channels = json.load(f)

    log.info(f"Loaded {len(channels)} channels from channels_raw.json")
    return channels


def get_video_ids(youtube, channel_id: str) -> list[str]:
    """
    Get the most recent video IDs for a channel using its
    uploads playlist. Every channel has an auto-generated
    uploads playlist with ID = 'UU' + channel_id[2:]
    This costs 1 API unit per channel.
    """
    # Convert channel ID to uploads playlist ID
    # Channel ID starts with UC, uploads playlist starts with UU
    uploads_playlist_id = "UU" + channel_id[2:]

    try:
        response = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=VIDEOS_PER_CHANNEL
        ).execute()

        video_ids = [
            item["contentDetails"]["videoId"]
            for item in response.get("items", [])
        ]
        return video_ids

    except HttpError as e:
        log.warning(f"Could not fetch playlist for {channel_id}: {e}")
        return []


def get_video_stats(youtube, video_ids: list[str], channel_id: str, niche: str) -> list[dict]:
    """
    Fetch detailed stats for a list of video IDs.
    videos.list accepts up to 50 IDs per call — very efficient.
    Costs 1 API unit per batch of 50.
    """
    if not video_ids:
        return []

    try:
        response = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(video_ids)
        ).execute()

        videos = []
        for item in response.get("items", []):
            snippet    = item.get("snippet",          {})
            statistics = item.get("statistics",       {})
            content    = item.get("contentDetails",   {})

            videos.append({
                "video_id":        item["id"],
                "channel_id":      channel_id,
                "niche":           niche,
                "title":           snippet.get("title",          ""),
                "description":     snippet.get("description",    "")[:500],
                "published_at":    snippet.get("publishedAt",    ""),
                "tags":            snippet.get("tags",           []),
                "duration":        content.get("duration",       ""),
                "view_count":      int(statistics.get("viewCount",    0)),
                "like_count":      int(statistics.get("likeCount",    0)),
                "comment_count":   int(statistics.get("commentCount", 0)),
            })

        return videos

    except HttpError as e:
        log.error(f"Error fetching video stats: {e}")
        return []
    
def fetch_all_videos(channels: list[dict]) -> list[dict]:
    """
    Fetch recent video stats for every channel.
    Returns a flat list of video dicts ready for storage.
    """
    youtube    = build_youtube_client()
    all_videos = []
    total      = len(channels)

    for i, channel in enumerate(channels, 1):
        channel_id = channel["channel_id"]
        title      = channel["title"]
        niche      = channel["niche"]

        log.info(f"[{i}/{total}] {title} ({niche})")

        # Stage 1 — get video IDs from uploads playlist
        video_ids = get_video_ids(youtube, channel_id)
        if not video_ids:
            log.warning(f"  No videos found for {title}")
            continue

        log.info(f"  Found {len(video_ids)} video IDs")

        # Stage 2 — get full stats for those video IDs
        videos = get_video_stats(youtube, video_ids, channel_id, niche)

        # Stage 3 — calculate engagement rate for each video
        for video in videos:
            views = video["view_count"]
            if views > 0:
                video["engagement_rate"] = round(
                    (video["like_count"] + video["comment_count"]) / views * 100, 4
                )
            else:
                video["engagement_rate"] = 0.0

        all_videos.extend(videos)
        log.info(f"  ✅ {len(videos)} videos fetched")

        # Pause to respect API rate limits
        time.sleep(0.3)

    return all_videos


def save_videos(videos: list[dict]) -> None:
    """Save all video data to JSON."""
    output_path = RAW_DATA_PATH / "videos_raw.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(videos, f, indent=2, ensure_ascii=False)

    log.info(f"Saved {len(videos)} videos to {output_path}")


def estimate_quota(channels: list[dict]) -> None:
    """Print estimated API units used."""
    n              = len(channels)
    playlist_units = n * 1
    video_units    = (n * VIDEOS_PER_CHANNEL // 50) + 1
    total          = playlist_units + video_units
    log.info(f"Estimated quota used: ~{total} units")
    log.info(f"Daily quota remaining: ~{10000 - 256 - total:,} units")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting video data fetch...")
    log.info(f"Fetching last {VIDEOS_PER_CHANNEL} videos per channel")
    channels   = load_channels()
    all_videos = fetch_all_videos(channels)
    save_videos(all_videos)
    estimate_quota(channels)
    log.info(f"Done! Fetched {len(all_videos)} videos total.")