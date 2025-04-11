from datetime import datetime
from utils.exceptions import TransformationError
from utils.logging_config import logger

class M2Transformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            latest = raw_data["observations"][-1]
            return {
                "date": latest["date"],
                "value": float(latest["value"]),
                "updated_at": datetime.now().isoformat(),
                "units": "USD"
            }
        except Exception as e:
            logger.error(f"M2 transformation failed: {e}")
            raise TransformationError(f"M2 processing error: {e}")