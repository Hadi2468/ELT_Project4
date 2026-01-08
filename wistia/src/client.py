import os
import time
import yaml
import requests
import logging

# -----------------------------
# Logging configuration
# -----------------------------
logger = logging.getLogger("wistia.client")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# -----------------------------
# Load config
# -----------------------------
def load_config(path="./wistia/config/wistia_config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# -----------------------------
# Wistia API Client
# -----------------------------
class WistiaClient:
    def __init__(self, config):
        self.base_url = config["api"]["base_url"]
        self.timeout = config["api"]["timeout"]   # 50

        self.api_token = os.getenv("WISTIA_API_TOKEN")
        if not self.api_token:
            logger.error("WISTIA_API_TOKEN environment variable is not set")
            raise RuntimeError("WISTIA_API_TOKEN is not set")

        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json"
        }

        logger.info("WistiaClient initialized")

    def get(self, endpoint, params=None, retries=3, backoff=2):
        """
        Generic GET with retry + exponential backoff.

        Args:
            endpoint: API endpoint (string)
            params: query parameters dict
            retries: number of retries
            backoff: exponential backoff base
        """
        url = f"{self.base_url}{endpoint}"

        for attempt in range(1, retries + 1):
            try:
                # logger.info(f"GET {endpoint} | Attempt {attempt}/{retries}")

                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=self.timeout
                )

                if response.status_code == 429:
                    sleep_time = backoff ** attempt
                    logger.warning(
                        f"Rate limited (429). Sleeping {sleep_time}s before retry"
                    )
                    time.sleep(sleep_time)
                    continue

                response.raise_for_status()
                # logger.info(f"✔️  Success {endpoint}\n")
                return response.json()

            except requests.exceptions.ConnectionError as e:
                if attempt == retries:
                    logger.error(
                        f"Connection failed after {retries} retries: {endpoint}",
                        exc_info=True
                    )
                    raise

                sleep_time = backoff ** attempt
                logger.warning(
                    f"Connection error: {e}. Retrying in {sleep_time}s"
                )
                time.sleep(sleep_time)

            except requests.exceptions.HTTPError as e:
                logger.error(
                    f"HTTP error {response.status_code} for {endpoint}: {response.text}",
                    exc_info=True
                )
                raise

        logger.critical(f"GET failed after {retries} retries: {endpoint}")
        raise RuntimeError(f"Failed GET after {retries} retries: {endpoint}")
