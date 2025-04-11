import requests
from utils.exceptions import ExtractionError
from utils.logging_config import logger

class FredExtractor:
    def __init__(self, api_key: str, series_id: str = "MANMM101XXM189S"):
        self.api_key = api_key
        self.series_id = series_id

    def fetch(self) -> dict:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={self.series_id}&api_key={self.api_key}&file_type=json"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"FRED API request failed: {e}")
            raise ExtractionError(f"FRED extraction failed: {e}")