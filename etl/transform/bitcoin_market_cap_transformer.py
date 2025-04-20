from datetime import datetime
# from utils.exceptions import TransformationError
from utils.logging_config import logger

# Placeholder if needed
class TransformationError(Exception):
    pass

class BitcoinMarketCapTransformer:
    def transform(self, raw_data: dict) -> dict:
        """Transforms the raw data bundle from CoinGeckoExtractor to extract market cap."""
        try:
            market_cap = raw_data.get("usd_market_cap")
            if market_cap is None:
                raise TransformationError("'usd_market_cap' key not found in raw CoinGecko data")

            timestamp_unix = raw_data.get("last_updated_at")
            if timestamp_unix:
                dt_object = datetime.fromtimestamp(timestamp_unix)
            else:
                dt_object = datetime.now()
            
            date_str = dt_object.strftime("%Y-%m-%d")
            iso_timestamp = dt_object.isoformat()

            return {
                "date": date_str,
                "value": market_cap,
                "units": "USD",
                "metadata": {
                    "source": "coingecko",
                    "last_updated_at_source": iso_timestamp
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin market cap transformation failed: {e} - Data: {raw_data}")
            raise TransformationError(f"Bitcoin market cap transformation failed: {e}") 