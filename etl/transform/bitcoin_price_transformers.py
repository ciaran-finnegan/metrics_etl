from datetime import datetime
from utils.exceptions import TransformationError
from utils.logging_config import logger

class BitcoinPriceTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            date = datetime.fromisoformat(raw_data["last_updated"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
            return {
                "date": date,
                "signal_name": "bitcoin_price",
                "value": raw_data["price"],
                "units": "USD",
                "metadata": {
                    "source": "coingecko",
                    "updated_at": raw_data["last_updated"]
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin price transformation failed: {e}")
            raise TransformationError(f"Bitcoin price processing error: {e}")

class Bitcoin24hChangeTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            date = datetime.fromisoformat(raw_data["last_updated"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
            return {
                "date": date,
                "signal_name": "bitcoin_24h_change",
                "value": raw_data["price_change_24h"],
                "units": "percentage",
                "metadata": {
                    "source": "coingecko",
                    "updated_at": raw_data["last_updated"]
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin 24h change transformation failed: {e}")
            raise TransformationError(f"Bitcoin 24h change processing error: {e}")

class Bitcoin7dChangeTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            date = datetime.fromisoformat(raw_data["last_updated"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
            return {
                "date": date,
                "signal_name": "bitcoin_7d_change",
                "value": raw_data["price_change_7d"],
                "units": "percentage",
                "metadata": {
                    "source": "coingecko",
                    "updated_at": raw_data["last_updated"]
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin 7d change transformation failed: {e}")
            raise TransformationError(f"Bitcoin 7d change processing error: {e}")

class Bitcoin30dChangeTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            date = datetime.fromisoformat(raw_data["last_updated"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
            return {
                "date": date,
                "signal_name": "bitcoin_30d_change",
                "value": raw_data["price_change_30d"],
                "units": "percentage",
                "metadata": {
                    "source": "coingecko",
                    "updated_at": raw_data["last_updated"]
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin 30d change transformation failed: {e}")
            raise TransformationError(f"Bitcoin 30d change processing error: {e}")

class BitcoinMarketCapTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            date = datetime.fromisoformat(raw_data["last_updated"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
            return {
                "date": date,
                "signal_name": "bitcoin_market_cap",
                "value": raw_data["market_cap"],
                "units": "USD",
                "metadata": {
                    "source": "coingecko",
                    "updated_at": raw_data["last_updated"]
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin market cap transformation failed: {e}")
            raise TransformationError(f"Bitcoin market cap processing error: {e}")

class Bitcoin24hVolumeTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            date = datetime.fromisoformat(raw_data["last_updated"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
            return {
                "date": date,
                "signal_name": "bitcoin_24h_volume",
                "value": raw_data["volume_24h"],
                "units": "USD",
                "metadata": {
                    "source": "coingecko",
                    "updated_at": raw_data["last_updated"]
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin 24h volume transformation failed: {e}")
            raise TransformationError(f"Bitcoin 24h volume processing error: {e}")

class BitcoinCirculatingSupplyTransformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            date = datetime.fromisoformat(raw_data["last_updated"].replace("Z", "+00:00")).strftime("%Y-%m-%d")
            return {
                "date": date,
                "signal_name": "bitcoin_circulating_supply",
                "value": raw_data["circulating_supply"],
                "units": "BTC",
                "metadata": {
                    "source": "coingecko",
                    "updated_at": raw_data["last_updated"]
                }
            }
        except Exception as e:
            logger.error(f"Bitcoin circulating supply transformation failed: {e}")
            raise TransformationError(f"Bitcoin circulating supply processing error: {e}")

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