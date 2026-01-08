import os
import json
import time
from datetime import datetime, timezone

from client import WistiaClient, load_config

# =====================================================
# Paths & Setup
# =====================================================
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "../data")

MEDIA_DIR = os.path.join(DATA_DIR, "media")
ENGAGEMENT_DIR = os.path.join(DATA_DIR, "media_engagement")
VISITORS_DIR = os.path.join(DATA_DIR, "visitors")
STATE_FILE = os.path.join(DATA_DIR, "last_ingestion.json")

for d in [MEDIA_DIR, ENGAGEMENT_DIR, VISITORS_DIR]:
    os.makedirs(d, exist_ok=True)

# =====================================================
# State Management
# =====================================================
def load_state():
    """
    Load ingestion state from JSON file if exists.
    Returns default structure if file is missing.
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)

    return {
        "media": {},
        "visitors": {
            "last_created_at": None,
            "fetched_keys": []
        }
    }


def save_state(state):
    """
    Save ingestion state to JSON file.
    """
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# =====================================================
# Media Ingestion (Incremental)
# =====================================================
def ingest_media_stats(client, config, state):
    """
    Ingest media stats from Wistia.
    Saves a new JSON file only if stats have changed since the last ingestion.

    Args:
        client: WistiaClient instance
        config: loaded config dict
        state: current ingestion state dict (updated in place)
    """
    print("\n⬇️  MEDIA INGESTION (INCREMENTAL)")
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    for media_id in config["medias"]:
        last_snapshot = state["media"].get(media_id)
        data = client.get(f"/stats/medias/{media_id}.json")

        # Only save if first run or stats changed
        if last_snapshot != data:
            filename = f"media_{media_id}_{run_timestamp}.json"
            path = os.path.join(MEDIA_DIR, filename)

            with open(path, "w") as f:
                json.dump(data, f, indent=2)

            state["media"][media_id] = data
            print(f"✅ Saved media snapshot: {filename}")
        else:
            print(f"🟢 Media {media_id} unchanged, skipping snapshot")

# =====================================================
# Media Engagement Ingestion
# =====================================================
def ingest_media_engagement(client, config, state):
    """
    Ingest media engagement stats from Wistia.
    Saves engagement JSON per media_id with timestamp.
    Optionally skips saving if engagement data is unchanged.

    Args:
        client: WistiaClient instance
        config: loaded config dict
        state: current ingestion state dict (updated in place)
    """
    print("\n⬇️  MEDIA ENGAGEMENT INGESTION")

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Initialize state section if missing
    state.setdefault("media_engagement", {})

    for media_id in config["medias"]:
        endpoint = f"/stats/medias/{media_id}/engagement"
        engagement_data = client.get(endpoint)

        last_snapshot = state["media_engagement"].get(media_id)

        # Save only if first run or engagement changed
        if last_snapshot != engagement_data:
            filename = f"media_engagement_{media_id}_{run_timestamp}.json"
            path = os.path.join(ENGAGEMENT_DIR, filename)

            with open(path, "w") as f:
                json.dump(engagement_data, f, indent=2)

            state["media_engagement"][media_id] = engagement_data
            print(f"✅ Saved engagement stats: {filename}")
        else:
            print(f"🟢 Engagement for media {media_id} unchanged, skipping")


# =====================================================
# Visitors List (Incremental)
# =====================================================
def ingest_visitors_list(client, config, state):
    """
    Ingest visitors from Wistia incrementally.
    Updates state with fetched visitor keys and latest created_at.

    Args:
        client: WistiaClient instance
        config: loaded config dict
        state: current ingestion state dict (updated in place)

    Returns:
        List of new visitor dicts
    """
    print("\n⬇️  VISITOR LIST INGESTION (INCREMENTAL)")

    last_created_at = state["visitors"]["last_created_at"]
    new_visitors = []

    page = 1
    per_page = config["api"]["per_page"]  # 100

    while True:
        visitors = client.get(
            "/stats/visitors",
            params={"page": page, "per_page": per_page}
        )

        if not visitors:
            break

        for v in visitors:
            created_at = v["created_at"]
            visitor_key = v["visitor_key"]

            # Skip visitors already fetched
            if visitor_key in state["visitors"]["fetched_keys"]:
                continue

            new_visitors.append(v)
            state["visitors"]["fetched_keys"].append(visitor_key)

        if len(visitors) < per_page:
            break  # Last page

        print(f"🔎 Page {page} processed")
        page += 1
        time.sleep(0.2)

    if new_visitors:
        batch_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"visitors_{batch_ts}_batch_{len(new_visitors)}.json"
        path = os.path.join(VISITORS_DIR, filename)

        with open(path, "w") as f:
            json.dump(new_visitors, f, indent=2)

        newest_created = max(v["created_at"] for v in new_visitors)
        state["visitors"]["last_created_at"] = newest_created

        print(f"✅✅✅ Saved {len(new_visitors)} new visitors: {filename}")
    else:
        print("🔴 No new visitors found")

    return new_visitors

# =====================================================
# MAIN ENTRY POINT
# =====================================================
def main():
    config = load_config()
    client = WistiaClient(config)
    state = load_state()

    ingest_media_stats(client, config, state)
    ingest_media_engagement(client, config, state)
    ingest_visitors_list(client, config, state)

    save_state(state)
    print("\n🌟 INGESTION PIPELINE COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    main()
