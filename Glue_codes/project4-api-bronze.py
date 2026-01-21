# =======================================================
# Project 4: Glue Python Shell Job (project4-api-bronze)
# =======================================================

import sys
import json
import time
import logging
import boto3
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from awsglue.utils import getResolvedOptions

# =====================================================
# Logging Configuration
# =====================================================
logger = logging.getLogger("project4-api-bronze")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)

# =====================================================
# Read Glue Arguments
# =====================================================
args = getResolvedOptions(
    sys.argv,
    ["BRONZE_BUCKET", "SECRET_NAME", "REGION"]
)

BRONZE_BUCKET = args["BRONZE_BUCKET"]
SECRET_NAME = args["SECRET_NAME"]
REGION = args["REGION"]

# =====================================================
# AWS Clients
# =====================================================
s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager", region_name=REGION)

# =====================================================
# Constants
# =====================================================
BASE_URL = "https://api.wistia.com/v1"
MEDIAS = ["gskhw4w4lm", "v08dlrgr7v"]
PER_PAGE = 100

STATE_PREFIX = "state/"
STATE_KEY = f"{STATE_PREFIX}last_ingestion.json"
LATEST_VISITOR_KEY = f"{STATE_PREFIX}latest_visitor.json"

MEDIA_PREFIX = "raw-data/media/"
ENGAGEMENT_PREFIX = "raw-data/media_engagement/"
VISITORS_PREFIX = "raw-data/visitors/"

LOCAL_TZ = ZoneInfo("America/Los_Angeles")

# =====================================================
# Secrets Manager
# =====================================================
def get_wistia_token():
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response["SecretString"])
    return secret["wistia_api_token"]

WISTIA_API_TOKEN = get_wistia_token()

HEADERS = {
    "Authorization": f"Bearer {WISTIA_API_TOKEN}"
}

# =====================================================
# State Management
# =====================================================
def load_state():
    try:
        obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        logger.info("▶️ No previous state found. Initializing new state.")
        return {
            "media": {},
            "media_engagement": {},
            "visitors": {
                "last_created_at": None
            }
        }

def save_state(state):
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=STATE_KEY,
        Body=json.dumps(state, indent=2)
    )
    logger.info("✅ last_ingestion.json updated")

def load_latest_visitor():
    try:
        obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=LATEST_VISITOR_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        logger.info("▶️ No latest_visitor.json found (first run)")
        return None

def save_latest_visitor(visitor_key):
    payload = {
        "latest_visitor_key": visitor_key,
        "snapshot_ts": datetime.now(timezone.utc).isoformat()
    }

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=LATEST_VISITOR_KEY,
        Body=json.dumps(payload, indent=2)
    )

    logger.info(f"✅ latest_visitor.json updated → {visitor_key}")

# =====================================================
# HTTP Helper
# =====================================================
def wistia_get(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    response = requests.get(
        url,
        headers=HEADERS,
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response.json()

# =====================================================
# Media Stats Ingestion
# =====================================================
def ingest_media_stats(state):
    logger.info("▶️ Ingesting MEDIA stats")
    run_ts = datetime.now(LOCAL_TZ).strftime("%Y%m%d_%H%M%S")

    for media_id in MEDIAS:
        data = wistia_get(f"/stats/medias/{media_id}.json")
        last_snapshot = state["media"].get(media_id)

        if last_snapshot != data:
            key = f"{MEDIA_PREFIX}media_{media_id}_{run_ts}.json"
            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=key,
                Body=json.dumps(data, indent=2)
            )
            state["media"][media_id] = data
            logger.info(f"✅ Media snapshot saved: {key}")
        else:
            logger.info(f"🟢 Media {media_id} unchanged — skipped")

# =====================================================
# Media Engagement Ingestion
# =====================================================
def ingest_media_engagement(state):
    logger.info("▶️ Ingesting MEDIA engagement")
    run_ts = datetime.now(LOCAL_TZ).strftime("%Y%m%d_%H%M%S")

    for media_id in MEDIAS:
        data = wistia_get(f"/stats/medias/{media_id}/engagement")
        last_snapshot = state["media_engagement"].get(media_id)

        if last_snapshot != data:
            key = f"{ENGAGEMENT_PREFIX}media_engagement_{media_id}_{run_ts}.json"
            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=key,
                Body=json.dumps(data, indent=2)
            )
            state["media_engagement"][media_id] = data
            logger.info(f"✅ Engagement snapshot saved: {key}")
        else:
            logger.info(f"🟢 Engagement {media_id} unchanged — skipped")

# =====================================================
# Visitors Ingestion (Incremental + Early Stop)
# =====================================================
def ingest_visitors(state):
    logger.info("▶️ Ingesting VISITORS (early-stop incremental)")

    checkpoint = load_latest_visitor()
    last_visitor_key = checkpoint["latest_visitor_key"] if checkpoint else None

    page = 1
    new_visitors = []
    stop_fetching = False

    while True:
        visitors = wistia_get(
            "/stats/visitors",
            params={"page": page, "per_page": PER_PAGE}
        )

        if not visitors:
            break

        for v in visitors:
            if last_visitor_key and v["visitor_key"] == last_visitor_key:
                logger.info(
                    f"🛑 Found previously ingested visitor_key={last_visitor_key}. Stopping pagination."
                )
                stop_fetching = True
                break

            new_visitors.append(v)

        if stop_fetching or len(visitors) < PER_PAGE:
            break

        logger.info(f"✔️ Visitors page {page} processed")
        page += 1
        time.sleep(0.2)

    if not new_visitors:
        logger.info("🟢 No new visitors found")
        return

    ts = datetime.now(LOCAL_TZ).strftime("%Y%m%d_%H%M%S")
    key = f"{VISITORS_PREFIX}visitors_{ts}_batch_{len(new_visitors)}.json"

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(new_visitors, indent=2)
    )

    logger.info(f"✅ {len(new_visitors)} new visitors saved → {key}")

    save_latest_visitor(new_visitors[0]["visitor_key"])

    newest_created = max(v["created_at"] for v in new_visitors)
    state["visitors"]["last_created_at"] = newest_created

# =====================================================
# Main
# =====================================================
def main():
    logger.info("▶️ ===== Project4 API → Bronze Job Started =====")

    state = load_state()

    ingest_media_stats(state)
    ingest_media_engagement(state)
    ingest_visitors(state)

    save_state(state)

    logger.info("⭐⭐⭐ ===== Project4 API → Bronze Job Completed Successfully =====")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.critical("❌ Job failed", exc_info=True)
        raise
