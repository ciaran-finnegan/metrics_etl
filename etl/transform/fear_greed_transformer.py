from datetime import datetime
from utils.exceptions import TransformationError
from utils.logging_config import logger

# Placeholder if needed
class TransformationError(Exception): pass
class Logger: ...
logger = Logger()

class FearGreedTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            if not raw_data or not isinstance(raw_data.get("data"), list) or not raw_data["data"]:
                raise TransformationError("Invalid or empty data structure from AlternativeExtractor (FNG)")
            
            latest = raw_data["data"][0]
            value = int(latest.get("value"))
            classification = latest.get("value_classification")
            timestamp_unix = latest.get("timestamp")

            if value is None or classification is None or timestamp_unix is None:
                 raise TransformationError(f"Missing required fields (value, classification, timestamp) in FNG data: {latest}")

            dt_object = datetime.fromtimestamp(int(timestamp_unix))
            date_str = dt_object.strftime("%Y-%m-%d")
            iso_timestamp = dt_object.isoformat()

            return {
                "date": date_str,
                "value": value,
                "units": "index",
                "classification": classification,
                "metadata": {
                     "source": "alternative.me",
                     "last_updated_at_source": iso_timestamp,
                     # Add classification here if not a primary column
                     # "classification": classification 
                 }
            }
        except (KeyError, IndexError, ValueError, TypeError) as e:
            logger.error(f"Fear & Greed transformation failed due to data structure or type issue: {e} - Data: {raw_data}")
            raise TransformationError(f"F&G data processing error: {e}")
        except Exception as e:
            logger.error(f"Fear & Greed transformation failed unexpectedly: {e} - Data: {raw_data}")
            raise TransformationError(f"F&G processing error: {e}")