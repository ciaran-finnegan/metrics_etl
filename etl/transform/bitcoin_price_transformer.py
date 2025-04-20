from datetime import datetime
from utils.exceptions import TransformationError
from utils.logging_config import logger

# Placeholder if needed
class TransformationError(Exception):
    pass

class BitcoinPriceTransformer:
    def transform(self, raw_data: dict) -> dict:
        """Transform raw CoinGecko bitcoin price data"""
        try:
            # For dictionary response like {'bitcoin': {'usd': 42000}}
            if isinstance(raw_data, dict) and 'bitcoin' in raw_data:
                raw_data = raw_data['bitcoin']
                
            # Check if 'usd' key exists in the data
            if 'usd' not in raw_data:
                logger.error(f"Missing 'usd' key in raw_data: {raw_data}")
                raise TransformationError("'usd' key not found in raw CoinGecko data")
                
            # Get the price and format the response
            price = raw_data['usd']
            
            return {
                "date": datetime.now().date().isoformat(),
                "value": price,
                "units": "USD",
                "signal_name": "bitcoin_price"
            }
            
        except Exception as e:
            logger.error(f"Bitcoin price transformation failed: {e}")
            raise TransformationError(f"Bitcoin price transformation failed: {e}") 