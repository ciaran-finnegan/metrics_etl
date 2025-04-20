from datetime import datetime
# from utils.exceptions import TransformationError
from utils.logging_config import logger

# Placeholder if needed
class TransformationError(Exception):
    pass

class Bitcoin30dChangeTransformer: # Renamed class to match file
    def transform(self, raw_data: dict) -> dict:
        """Transforms the raw data bundle from CoinGeckoExtractor to extract 30d price change %."""
        try:
            price_change_30d = raw_data.get("usd_30d_change") # Key from CoinGecko API
            if price_change_30d is None:
                raise TransformationError("'usd_30d_change' key not found in raw CoinGecko data")

            timestamp_unix = raw_data.get("last_updated_at")
            if timestamp_unix:
                dt_object = datetime.fromtimestamp(timestamp_unix)
            else:
                dt_object = datetime.now()
            
            date_str = dt_object.strftime("%Y-%m-%d")
            iso_timestamp = dt_object.isoformat()

            return {
                "date": date_str,
                "value": price_change_30d,
                "units": "percentage",
                "metadata": {
                    "source": "coingecko",
                    "last_updated_at_source": iso_timestamp
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin 30d change transformation failed: {e} - Data: {raw_data}")
            raise TransformationError(f"Bitcoin 30d change transformation failed: {e}") 