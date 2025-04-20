from datetime import datetime
# from utils.exceptions import TransformationError
from utils.logging_config import logger

# Placeholder if needed
class TransformationError(Exception):
    pass

class Bitcoin24hVolumeTransformer: # Renamed class to match file
    def transform(self, raw_data: dict) -> dict:
        """Transforms the raw data bundle from CoinGeckoExtractor to extract 24h volume."""
        try:
            volume_24h = raw_data.get("usd_24h_vol")
            if volume_24h is None:
                raise TransformationError("'usd_24h_vol' key not found in raw CoinGecko data")

            timestamp_unix = raw_data.get("last_updated_at")
            if timestamp_unix:
                dt_object = datetime.fromtimestamp(timestamp_unix)
            else:
                dt_object = datetime.now()
            
            date_str = dt_object.strftime("%Y-%m-%d")
            iso_timestamp = dt_object.isoformat()

            return {
                "date": date_str,
                "value": volume_24h,
                "units": "USD",
                "metadata": {
                    "source": "coingecko",
                    "last_updated_at_source": iso_timestamp
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin 24h volume transformation failed: {e} - Data: {raw_data}")
            raise TransformationError(f"Bitcoin 24h volume transformation failed: {e}") 