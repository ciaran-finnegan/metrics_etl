import requests
from utils.exceptions import ExtractionError
from utils.logging_config import logger
from typing import Dict, Any
from datetime import datetime, timedelta

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
        # Get last 6 months of data to ensure we have the latest observation
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)  # Increased from 90 to 180 days
        
        api_params = {
            "series_id": self.series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date.strftime("%Y-%m-%d"),
            "observation_end": end_date.strftime("%Y-%m-%d")
        }
        logger.debug(f"Fetching FRED data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        try:
            response = requests.get(self.base_url, params=api_params)
            response.raise_for_status()
            data = response.json()
            
            # Log the raw response
            if "observations" in data:
                observations = data["observations"]
                logger.debug(f"FRED returned {len(observations)} observations")
                if observations:
                    dates = [obs["date"] for obs in observations]
                    logger.debug(f"Date range in response: {min(dates)} to {max(dates)}")
                else:
                    logger.warning("FRED returned no observations")
            else:
                logger.warning(f"Unexpected FRED response structure: {data}")
            
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"FRED API request failed for series {self.series_id}: {e}")
            raise ExtractionError(f"FRED extraction failed: {e}")
        except Exception as e:
            logger.error(f"Error processing FRED data for series {self.series_id}: {e}")
            raise ExtractionError(f"Error processing FRED data: {e}")