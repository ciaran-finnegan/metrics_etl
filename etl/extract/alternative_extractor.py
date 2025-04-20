import requests
from utils.exceptions import ExtractionError
from utils.logging_config import logger
from typing import Dict, Any

# Placeholder if needed
class Logger:
    def info(self, msg): print(f"INFO: {msg}")
    def error(self, msg): print(f"ERROR: {msg}")
    def debug(self, msg): print(f"DEBUG: {msg}")
logger = Logger()

class AlternativeExtractor:
    def __init__(self, params=None):
        # Fear and Greed Index from alternative.me doesn't need params
        self.url = "https://api.alternative.me/fng/?limit=1"
        logger.info(f"Initialized AlternativeExtractor (FNG) for URL: {self.url}")
    
    def fetch(self):
        """Fetches fear and greed index from Alternative.me API"""
        logger.debug(f"Fetching data from {self.url}")
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            data = response.json()
            
            # Basic validation 
            if not data or 'data' not in data or not isinstance(data['data'], list) or not data['data']:
                logger.error(f"Unexpected data structure from {self.url}: {data}")
                raise ValueError(f"Unexpected data structure from {self.url}")
                
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Alternative.me FNG API request failed: {e}")
            raise ExtractionError(f"Alternative.me FNG API request failed: {e}")
        except Exception as e:
            logger.error(f"Error processing Alternative.me FNG data: {e}")
            raise ExtractionError(f"Error processing Alternative.me FNG data: {e}")