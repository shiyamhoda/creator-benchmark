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

# ── Channel handles by niche ─────────────────────────────────────────────────
# Format: YouTube handle WITHOUT the @ symbol
# To find a handle: go to any YouTube channel → the @handle is in the URL
# Cost: 1 API unit per channel (vs 100 units for search)

CHANNELS_BY_NICHE = {
    "Finance": [
        "AndreiJikh",
        "MinorityMindset",
        "GrahamStephan",
        "MeetKevin",
        "marktilbury",
        "NateOBrien",
        "RyanScribner",
        "ThePlainBagel",
        "JosephCarlsonShow",
        "HumphreyTalks",
    ],
    "Fitness": [
        "JeffNippard",
        "athleanx",
        "JeremyEthier",
        "HybridCalisthenics",
        "AustinDunham",
        "calisthenicmovement",
        "ChrisHeria",
        "MagnusMidtbo",
        "RenaissancePeriodization",
        "AlanThrall",
    ],
    "Beauty": [
        "NikkieTutorials",
        "safiyany",
        "jackieaina",
        "StephanieLange",
        "hindash",
        "LisaEldridge",
        "WayneGoss",
        "hyram",
        "samantharavndahl",
        "TatiWestbrook",
    ],
    "Gaming": [
        "markiplier",
        "jacksepticeye",
        "Ninja",
        "pokimane",
        "Valkyrae",
        "DudePerfect",
        "PewDiePie",
        "dreamwastaken",
        "LudwigAhgren",
        "DisguisedToast",
    ],
    "Tech": [
        "mkbhd",
        "LinusTechTips",
        "Fireship",
        "NetworkChuck",
        "UnboxTherapy",
        "Mrwhosetheboss",
        "Dave2D",
        "Computerphile",
        "TechLinked",
        "HardwareUnboxed",
    ],
    "Food": [
        "bingingwithbabish",
        "JoshuaWeissman",
        "EthanChlebowski",
        "internetshaquille",
        "ProHomeCooks",
        "GugaFoods",
        "TastingHistory",
        "foodwishes",
        "MythicalKitchen",
        "BrianLagerstrom",
    ],
    "Travel": [
        "karaandnate",
        "LostLeBlancs",
        "MarkWiens",
        "AbroadInJapan",
        "SailingLaVagabonde",
        "FearlessandFar",
        "WoltersWorld",
        "SamuelAndAudrey",
        "HopscotchTheGlobe",
        "chrisabroad",
    ],
    "Education": [
        "TEDEd",
        "kurzgesagt",
        "veritasium",
        "crashcourse",
        "3blue1brown",
        "SmarterEveryDay",
        "MarkRober",
        "Vsauce",
        "minutephysics",
        "scottmanley",
    ],
}

# ── Functions ────────────────────────────────────────────────────────────────

def build_youtube_client():
    """Create and return a YouTube API client."""
    return build("youtube", "v3", developerKey=API_KEY)


def get_channel_id_by_handle(youtube, handle: str, niche: str) -> dict | None:
    """
    Look up a channel ID using its YouTube handle.
    Uses channels.list with forHandle parameter — costs only 1 API unit.
    Returns a dict with channel_id and metadata or None if not found.
    """
    try:
        response = youtube.channels().list(
            part="snippet",
            forHandle=handle
        ).execute()

        items = response.get("items", [])
        if not items:
            log.warning(f"  ❌ Handle not found: @{handle}")
            return None

        channel_id = items[0]["id"]
        title      = items[0]["snippet"]["title"]

        log.info(f"  ✅ @{handle:<30} → {title:<35} | {channel_id}")
        return {
            "handle":     handle,
            "title":      title,
            "channel_id": channel_id,
            "niche":      niche,
        }

    except HttpError as e:
        log.error(f"  ❌ HTTP error for @{handle}: {e}")
        return None


def resolve_all_handles(youtube) -> dict:
    """
    Loop through every niche and handle.
    Returns a clean dict of niche → list of channel_id strings.
    """
    results = {}
    total   = sum(len(handles) for handles in CHANNELS_BY_NICHE.values())
    count   = 0

    for niche, handles in CHANNELS_BY_NICHE.items():
        log.info(f"\nResolving niche: {niche} ({len(handles)} channels)")
        results[niche] = []

        for handle in handles:
            count += 1
            log.info(f"[{count}/{total}] Looking up: @{handle}")

            result = get_channel_id_by_handle(youtube, handle, niche)
            if result:
                results[niche].append(result["channel_id"])

            # Small pause to be respectful of the API
            time.sleep(0.3)

    return results


def save_channel_ids(results: dict) -> None:
    """
    Save niche → channel_id mapping to JSON.
    This file is read by fetch_channels.py in the next step.
    """
    output_path = RAW_DATA_PATH / "channel_ids.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    total = sum(len(ids) for ids in results.values())
    log.info(f"\nSaved {total} channel IDs to {output_path}")
    log.info("Breakdown by niche:")
    for niche, ids in results.items():
        status = "✅" if len(ids) >= 8 else "⚠️"
        log.info(f"  {status} {niche:<12} {len(ids)} channels")


def estimate_quota_used(results: dict) -> None:
    """Print an estimate of how many API units were consumed."""
    total_channels = sum(len(ids) for ids in results.values())
    units_used     = total_channels * 1
    log.info(f"\nEstimated API quota used: {units_used} units")
    log.info(f"Daily quota remaining:    ~{10000 - units_used:,} units")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting channel handle resolution...")
    log.info("Method: forHandle lookup — 1 API unit per channel")
    youtube = build_youtube_client()
    results = resolve_all_handles(youtube)
    save_channel_ids(results)
    estimate_quota_used(results)
    log.info("\nDone! Run fetch_channels.py next to pull full statistics.")