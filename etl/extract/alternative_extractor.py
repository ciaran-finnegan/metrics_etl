import requests
from utils.exceptions import ExtractionError
from utils.logging_config import logger

class AlternativeExtractor:
    def __init__(self, api_key: str = None):
        self.api_key = api_key

    def fetch(self) -> dict:
        url = "https://api.alternative.me/fng/?limit=1"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Alternative.me API request failed: {e}")
            raise ExtractionError(f"Fear & Greed extraction failed: {e}")