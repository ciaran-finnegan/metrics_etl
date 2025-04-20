import requests
import time
from typing import Dict, Any
from collections import deque
from utils.exceptions import ExtractionError
from utils.logging_config import logger, log_plugin_info
from etl.extract.base_extractor import BaseExtractor
import datetime

class CoinGeckoExtractor(BaseExtractor):
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.api_key = params.get("api_key")
        self.coin_id = params.get("coin_id", "bitcoin")
        self.base_url = "https://api.coingecko.com/api/v3"
        
        # Rate limiting parameters
        self.last_request_time = 0
        self.min_request_interval = 1.5  # Ensure we're safely below the limits
        self.request_timestamps = deque(maxlen=30)  # For the 30 requests/minute limit
        self.minute_window_size = 60  # 1 minute window
        
        # Monthly quota tracking
        self.monthly_requests = 0
        self.max_monthly_requests = 10000 if self.api_key else 300  # Pro vs Free tier
        self.monthly_reset_time = time.time() + (30 * 24 * 60 * 60)
        
        # Log initialization
        plan_type = "Pro" if self.api_key else "Free"
        log_plugin_info('extract', 'CoinGeckoExtractor', 
                       f"Initialized for coin '{self.coin_id}' using {plan_type} plan "
                       f"(limit: {self.max_monthly_requests} requests/month)")

    def _clean_old_requests(self, current_time: float) -> None:
        """Remove requests older than 1 minute from our tracking queue"""
        old_count = len(self.request_timestamps)
        while self.request_timestamps and current_time - self.request_timestamps[0] > self.minute_window_size:
            self.request_timestamps.popleft()
        
        if old_count > len(self.request_timestamps) and len(self.request_timestamps) % 5 == 0:
            log_plugin_info('extract', 'CoinGeckoExtractor', 
                           f"Cleaned request tracking queue. Currently at {len(self.request_timestamps)}/30 requests in the last minute")

    def _check_monthly_limit(self) -> None:
        """Check and reset monthly request count if needed"""
        current_time = time.time()
        
        # Calculate remaining requests and time until reset
        remaining_requests = self.max_monthly_requests - self.monthly_requests
        time_until_reset = self.monthly_reset_time - current_time
        days_until_reset = time_until_reset / (24 * 60 * 60)
        
        # Periodic logging of quota status
        if self.monthly_requests > 0 and self.monthly_requests % 50 == 0:
            log_plugin_info('extract', 'CoinGeckoExtractor', 
                           f"Monthly usage: {self.monthly_requests}/{self.max_monthly_requests} "
                           f"({remaining_requests} remaining, reset in {days_until_reset:.1f} days)")
        
        # Handle reset or waiting if limit reached
        if current_time >= self.monthly_reset_time:
            logger.info("Monthly request counter reset")
            self.monthly_requests = 0
            self.monthly_reset_time = current_time + (30 * 24 * 60 * 60)
        elif self.monthly_requests >= self.max_monthly_requests:
            wait_time = self.monthly_reset_time - current_time
            wait_hours = wait_time / 3600
            log_plugin_info('extract', 'CoinGeckoExtractor', 
                           f"⚠️ Monthly rate limit ({self.max_monthly_requests}) reached. "
                           f"Waiting {wait_hours:.2f} hours...")
            
            # We could implement a more sophisticated backoff here instead of sleeping the whole time
            # For now, we'll just wait until reset with logging
            time.sleep(wait_time)
            
            self.monthly_requests = 0
            self.monthly_reset_time = time.time() + (30 * 24 * 60 * 60)
            log_plugin_info('extract', 'CoinGeckoExtractor', "Monthly rate limit reset. Continuing with requests.")

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make a request to the CoinGecko API with comprehensive rate limiting"""
        current_time = time.time()

        # Check monthly limits
        self._check_monthly_limit()

        # Ensure we don't exceed per-request rate limit (for normal operations)
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            log_plugin_info('extract', 'CoinGeckoExtractor', f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            current_time = time.time()

        # Ensure we don't exceed minute-based rate limit
        self._clean_old_requests(current_time)
        if len(self.request_timestamps) >= 30:
            oldest_request = self.request_timestamps[0]
            wait_time = self.minute_window_size - (current_time - oldest_request)
            if wait_time > 0:
                log_plugin_info('extract', 'CoinGeckoExtractor', 
                               f"Minute rate limit reached (30 req/min). Waiting {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                current_time = time.time()
                # Clean queue again after waiting
                self._clean_old_requests(current_time)

        try:
            # Set up request headers and parameters
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            if self.api_key:
                headers["x-cg-pro-api-key"] = self.api_key
            
            full_url = f"{self.base_url}/{endpoint}"
            log_plugin_info('extract', 'CoinGeckoExtractor', 
                           f"Making request to '{endpoint}' for {self.coin_id}")
            
            # Make the request
            response = requests.get(full_url, params=params, headers=headers)

            # Update request tracking
            request_time_after_call = time.time()
            self.last_request_time = request_time_after_call
            self.request_timestamps.append(request_time_after_call)
            self.monthly_requests += 1
            
            # Handle rate limiting responses
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                log_plugin_info('extract', 'CoinGeckoExtractor', 
                               f"⚠️ Rate limit hit (429). Waiting {retry_after} seconds before retry...")
                time.sleep(retry_after)
                return self._make_request(endpoint, params)  # Retry
            
            # Handle other error responses
            elif response.status_code != 200:
                log_plugin_info('extract', 'CoinGeckoExtractor', 
                               f"Error response: {response.status_code} - {response.text}")
                response.raise_for_status()
            
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"CoinGecko API request failed: {e}")
            raise ExtractionError(f"CoinGecko API request failed: {e}")

    def extract(self) -> Dict[str, Any]:
        """
        Fetch cryptocurrency data for the configured coin_id
        from CoinGecko's simple/price endpoint.
        """
        try:
            log_plugin_info('extract', 'CoinGeckoExtractor', f"Fetching data for {self.coin_id}")
            
            data = self._make_request("simple/price", {
                "ids": self.coin_id,
                "vs_currencies": "usd",
                "include_market_cap": "true",
                "include_24hr_vol": "true",
                "include_24hr_change": "true",
                "include_last_updated_at": "true"
            })

            if self.coin_id not in data:
                logger.error(f"Data for coin_id '{self.coin_id}' not found in CoinGecko response: {data}")
                raise ExtractionError(f"Data for coin_id '{self.coin_id}' not found in CoinGecko response.")

            coin_data = data[self.coin_id]
            
            # Log successful extraction with a sample of the data
            price = coin_data.get('usd', 'N/A')
            market_cap = coin_data.get('usd_market_cap', 'N/A')
            price_change = coin_data.get('usd_24h_change', 'N/A')
            
            log_plugin_info('extract', 'CoinGeckoExtractor', 
                           f"Successfully fetched {self.coin_id} data: "
                           f"Price=${price}, "
                           f"24h Change={price_change:.2f}%, " if isinstance(price_change, (int, float)) 
                                                               else f"24h Change={price_change}, "
                           f"Market Cap=${market_cap:,.0f}" if isinstance(market_cap, (int, float))
                                                           else f"Market Cap={market_cap}")

            return coin_data

        except Exception as e:
            logger.error(f"CoinGecko extraction failed for {self.coin_id}: {e}")
            raise ExtractionError(f"CoinGecko extraction failed for {self.coin_id}: {e}")
            
    def fetch(self) -> Dict[str, Any]:
        """Alias for extract method to maintain compatibility with ETL pipeline"""
        return self.extract() 