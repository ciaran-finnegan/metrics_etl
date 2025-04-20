import requests
import time
from typing import Dict, Any
from collections import deque
from utils.exceptions import ExtractionError
from utils.logging_config import logger

class BitcoinPriceExtractor:
    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.base_url = "https://api.coingecko.com/api/v3"
        self.last_request_time = 0
        self.min_request_interval = 6.0  # Minimum time between requests (10 per minute)
        self.request_timestamps = deque(maxlen=10)  # Track last 10 requests for minute window
        self.minute_window_size = 60  # 1 minute in seconds
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        if api_key:
            self.headers["x-cg-pro-api-key"] = api_key

    def _clean_old_requests(self, current_time: float) -> None:
        """Remove requests older than 1 minute"""
        while self.request_timestamps and current_time - self.request_timestamps[0] > self.minute_window_size:
            self.request_timestamps.popleft()

    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limits"""
        current_time = time.time()
        
        # Check if we need to wait due to minimum interval
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            current_time = time.time()
        
        # Check minute window limit
        self._clean_old_requests(current_time)
        if len(self.request_timestamps) >= 10:  # 10 requests per minute
            oldest_request = self.request_timestamps[0]
            wait_time = self.minute_window_size - (current_time - oldest_request)
            if wait_time > 0:
                logger.warning(f"Minute rate limit reached. Waiting {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                current_time = time.time()
        
        self.last_request_time = current_time
        self.request_timestamps.append(current_time)

    def fetch(self) -> Dict[str, Any]:
        """Fetch Bitcoin price and related metrics"""
        try:
            self._wait_for_rate_limit()
            
            # Fetch Bitcoin data
            response = requests.get(
                f"{self.base_url}/coins/bitcoin",
                headers=self.headers
            )
            
            if response.status_code == 429:
                logger.warning("Rate limit hit, waiting before retry...")
                time.sleep(60)  # Wait a full minute
                return self.fetch()  # Retry the request
            
            response.raise_for_status()
            data = response.json()
            
            # Extract relevant metrics
            market_data = data.get('market_data', {})
            return {
                'price': market_data.get('current_price', {}).get('usd'),
                'price_change_24h': market_data.get('price_change_percentage_24h'),
                'price_change_7d': market_data.get('price_change_percentage_7d'),
                'price_change_30d': market_data.get('price_change_percentage_30d'),
                'ath': market_data.get('ath', {}).get('usd'),
                'atl': market_data.get('atl', {}).get('usd'),
                'market_cap': market_data.get('market_cap', {}).get('usd'),
                'volume_24h': market_data.get('total_volume', {}).get('usd'),
                'circulating_supply': data.get('market_data', {}).get('circulating_supply'),
                'total_supply': data.get('market_data', {}).get('total_supply'),
                'last_updated': data.get('last_updated')
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"CoinGecko API request failed: {e}")
            raise ExtractionError(f"Bitcoin price extraction failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in Bitcoin price extraction: {e}")
            raise ExtractionError(f"Bitcoin price extraction failed: {e}") 