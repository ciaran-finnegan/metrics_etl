import requests
from utils.exceptions import ExtractionError
from utils.logging_config import logger
from typing import Dict, Any

# Placeholder if needed
class ExtractionError(Exception):
    pass
class Logger:
    def info(self, msg): print(f"INFO: {msg}")
    def error(self, msg): print(f"ERROR: {msg}")
    def debug(self, msg): print(f"DEBUG: {msg}") # Added debug
logger = Logger()

class FredExtractor:
    def __init__(self, params: Dict[str, Any]):
        """Initializes from a params dictionary."""
        self.api_key = params.get("api_key")
        self.series_id = params.get("series_id")
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"

        if not self.api_key or not self.series_id:
            raise ValueError("FredExtractor requires 'api_key' and 'series_id' in params")
        logger.info(f"Initialized FredExtractor for series: {self.series_id}")

    def fetch(self) -> dict:
        """Fetches data for the configured series_id."""
        api_params = {
            "series_id": self.series_id,
            "api_key": self.api_key,
            "file_type": "json"
        }
        logger.debug(f"Fetching FRED data: {self.base_url} with params {api_params}")
        try:
            response = requests.get(self.base_url, params=api_params)
            response.raise_for_status()
            return response.json() # Return raw JSON data
        except requests.exceptions.RequestException as e:
            logger.error(f"FRED API request failed for series {self.series_id}: {e}")
            raise ExtractionError(f"FRED extraction failed: {e}")
        except Exception as e:
            logger.error(f"Error processing FRED data for series {self.series_id}: {e}")
            raise ExtractionError(f"Error processing FRED data: {e}")