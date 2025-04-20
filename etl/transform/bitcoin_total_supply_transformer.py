from datetime import datetime
from utils.exceptions import TransformationError
from utils.logging_config import logger

class BitcoinTotalSupplyTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            date = datetime.fromisoformat(raw_data["last_updated"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
            return {
                "date": date,
                "signal_name": "bitcoin_total_supply",
                "value": raw_data["total_supply"],
                "units": "BTC",
                "metadata": {
                    "source": "coingecko",
                    "updated_at": raw_data["last_updated"]
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin total supply transformation failed: {e}")
            raise TransformationError(f"Bitcoin total supply processing error: {e}") 