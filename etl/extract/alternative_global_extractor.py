import requests
from utils.exceptions import ExtractionError
from utils.logging_config import logger

# Remove placeholder classes
# class ExtractionError(Exception): ...
# class Logger: ...

class AlternativeGlobalExtractor:
    def __init__(self, params=None):
        # No params needed for this endpoint
        self.url = "https://api.alternative.me/v2/global/"
        logger.info(f"Initialized AlternativeGlobalExtractor for URL: {self.url}")

    def fetch(self):
        """Fetches global market data from Alternative.me API"""
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            data = response.json()
            
            # Basic validation of expected data structure
            if not data or 'data' not in data:
                raise ExtractionError(f"Unexpected data structure from {self.url}")
                
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Alternative.me global API request failed: {e}")
            raise ExtractionError(f"Alternative.me global API request failed: {e}")
        except Exception as e:
            logger.error(f"Error processing Alternative.me global data: {e}")
            raise ExtractionError(f"Error processing Alternative.me global data: {e}") 