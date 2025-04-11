from datetime import datetime
from utils.exceptions import TransformationError
from utils.logging_config import logger

class FearGreedTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            latest = raw_data["data"][0]
            return {
                "value": int(latest["value"]),
                "classification": latest["value_classification"],
                "updated_at": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Fear & Greed transformation failed: {e}")
            raise TransformationError(f"F&G processing error: {e}")