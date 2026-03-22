import json
import logging
from pathlib import Path
from config import RAW_DATA_PATH
import spacy

# ── Setup ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)



# Load spaCy English model
nlp = spacy.load("en_core_web_sm")

# ── Niche keyword dictionary ──────────────────────────────────────────────────
# Each niche has a list of keywords we expect to find in channel descriptions.
# The more keywords match, the higher the confidence score.
# To add a new niche: add a new key with a list of relevant keywords.

NICHE_KEYWORDS = {
    "Finance": [
        "finance", "money", "investing", "investment", "stock", "stocks",
        "wealth", "budget", "budgeting", "savings", "dividend", "portfolio",
        "crypto", "cryptocurrency", "trading", "financial", "income",
        "retire", "retirement", "economy", "economic", "market", "markets",
        "fund", "funds", "asset", "assets", "credit", "debt", "loan",
    ],
    "Fitness": [
        "fitness", "workout", "exercise", "gym", "training", "muscle",
        "strength", "cardio", "nutrition", "diet", "health", "bodybuilding",
        "weightlifting", "calisthenics", "crossfit", "running", "athlete",
        "athletic", "physique", "gains", "protein", "supplement", "yoga",
        "flexibility", "endurance", "performance", "coach", "coaching",
    ],
    "Beauty": [
        "beauty", "makeup", "skincare", "cosmetic", "cosmetics", "hair",
        "fashion", "style", "tutorial", "tutorials", "foundation", "lipstick",
        "eyeshadow", "contour", "glam", "glamour", "routine", "skin",
        "moisturizer", "serum", "cleanser", "toner", "brush", "palette",
        "highlighter", "blush", "concealer", "grooming",
    ],
    "Gaming": [
        "gaming", "game", "games", "gamer", "gameplay", "playthrough",
        "streamer", "streaming", "twitch", "esports", "console", "pc",
        "xbox", "playstation", "nintendo", "minecraft", "fortnite",
        "videogame", "videogames", "rpg", "fps", "mmorpg", "walkthrough",
        "speedrun", "mod", "mods", "multiplayer", "controller",
    ],
    "Tech": [
        "tech", "technology", "software", "hardware", "programming", "coding",
        "developer", "computer", "laptop", "smartphone", "phone", "review",
        "reviews", "unboxing", "gadget", "gadgets", "apple", "android",
        "linux", "python", "javascript", "cybersecurity", "ai", "artificial",
        "machine learning", "data", "cloud", "network", "server", "cpu",
    ],
    "Food": [
        "food", "cooking", "recipe", "recipes", "chef", "kitchen", "baking",
        "cook", "meal", "meals", "ingredient", "ingredients", "cuisine",
        "restaurant", "taste", "flavor", "flavour", "dish", "dishes",
        "breakfast", "lunch", "dinner", "snack", "dessert", "grilling",
        "bbq", "barbecue", "vegan", "vegetarian", "foodie",
    ],
    "Travel": [
        "travel", "travelling", "traveling", "traveler", "traveller",
        "adventure", "explore", "exploring", "explorer", "journey",
        "trip", "trips", "destination", "destinations", "wanderlust",
        "backpack", "backpacking", "vlog", "vlogs", "culture", "cultures",
        "country", "countries", "city", "cities", "world", "abroad",
        "flight", "hotel", "hostel", "nomad",
    ],
    "Education": [
        "education", "educational", "learn", "learning", "teach", "teaching",
        "teacher", "science", "history", "math", "mathematics", "physics",
        "chemistry", "biology", "explain", "explained", "knowledge",
        "academic", "university", "school", "lecture", "course", "courses",
        "documentary", "facts", "research", "experiment", "discovery",
        "philosophy", "psychology", "economics",
    ],
}

def classify_channel(description: str, assigned_niche: str) -> dict:
    """
    Classify a channel into a niche based on its description text.

    Returns a dict with:
    - nlp_niche:      the niche predicted by NLP
    - nlp_confidence: score from 0.0 to 1.0
    - api_niche:      the niche we assigned when fetching (from our seed list)
    - niche_match:    True if NLP agrees with our seed list assignment

    We keep both the API-assigned niche and NLP niche so we can
    compare them in the dashboard — this is the "audit trail" page.
    """
    if not description or not description.strip():
        return {
            "nlp_niche":      "Unknown",
            "nlp_confidence": 0.0,
            "api_niche":      assigned_niche,
            "niche_match":    False,
        }

    # Tokenize and normalize with spaCy
    doc    = nlp(description.lower())
    tokens = {token.lemma_ for token in doc if not token.is_stop and not token.is_punct}

    # Score each niche by counting keyword matches
    scores = {}
    for niche, keywords in NICHE_KEYWORDS.items():
        matched  = sum(1 for kw in keywords if kw in tokens or kw in description.lower())
        scores[niche] = matched / len(keywords)

    # Pick the niche with the highest score
    best_niche      = max(scores, key=scores.get)
    best_confidence = round(scores[best_niche], 4)

    # If no keywords matched at all fall back to assigned niche
    if best_confidence == 0.0:
        best_niche = assigned_niche

    return {
        "nlp_niche":      best_niche,
        "nlp_confidence": best_confidence,
        "api_niche":      assigned_niche,
        "niche_match":    best_niche == assigned_niche,
    }
def classify_all_channels(channels: list[dict]) -> list[dict]:
    """
    Run NLP classification on every channel description.
    Merges classification results into each channel record.
    """
    classified  = []
    match_count = 0

    for channel in channels:
        result = classify_channel(
            description     = channel.get("description", ""),
            assigned_niche  = channel.get("niche", "Unknown"),
        )

        # Merge classification into the channel record
        enriched = {**channel, **result}
        classified.append(enriched)

        status = "✅" if result["niche_match"] else "❌"
        log.info(
            f"{status} {channel['title']:<35} "
            f"API: {result['api_niche']:<12} "
            f"NLP: {result['nlp_niche']:<12} "
            f"Confidence: {result['nlp_confidence']:.2%}"
        )

        if result["niche_match"]:
            match_count += 1

    accuracy = match_count / len(classified) * 100
    log.info(f"\nNLP accuracy vs API labels: {accuracy:.1f}% ({match_count}/{len(classified)})")
    return classified

def save_classified(channels: list[dict]) -> None:
    """Save classified channel data to JSON."""
    output_path = RAW_DATA_PATH / "channels_classified.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(channels, f, indent=2, ensure_ascii=False)
    log.info(f"Saved {len(channels)} classified channels to {output_path}")


def print_summary(channels: list[dict]) -> None:
    """Print a niche-level accuracy summary."""
    from collections import defaultdict
    niche_stats = defaultdict(lambda: {"total": 0, "matched": 0})

    for ch in channels:
        niche = ch["api_niche"]
        niche_stats[niche]["total"]   += 1
        niche_stats[niche]["matched"] += int(ch["niche_match"])

    log.info("\nAccuracy by niche:")
    log.info(f"  {'Niche':<12} {'Matched':<10} {'Total':<8} {'Accuracy'}")
    log.info(f"  {'-'*45}")
    for niche, stats in sorted(niche_stats.items()):
        accuracy = stats["matched"] / stats["total"] * 100
        log.info(
            f"  {niche:<12} {stats['matched']:<10} "
            f"{stats['total']:<8} {accuracy:.0f}%"
        )


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting NLP niche classification...")

    input_path = RAW_DATA_PATH / "channels_raw.json"
    with open(input_path, encoding="utf-8") as f:
        channels = json.load(f)

    log.info(f"Loaded {len(channels)} channels for classification")

    classified = classify_all_channels(channels)
    save_classified(classified)
    print_summary(classified)

    log.info("Done! Run load_database.py next.")