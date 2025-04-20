from datetime import datetime
from utils.exceptions import TransformationError
from utils.logging_config import logger

class TotalMarketCapTransformer:
    def transform(self, raw_data: dict) -> dict:
        """Transform total market cap data from Alternative.me global endpoint"""
        try:
            # Alternative.me API returns data in this structure
            if 'data' not in raw_data:
                logger.error(f"Missing 'data' in raw_data: {raw_data}")
                raise TransformationError("Missing 'data' key in raw Alternative.me global data")
            
            data = raw_data['data']
            
            # Extract total market cap - check if it exists in the expected path
            if 'quotes' not in data or 'USD' not in data['quotes'] or 'total_market_cap' not in data['quotes']['USD']:
                logger.error(f"Total market cap not found in data structure: {data}")
                raise TransformationError("Total market cap ('quotes.USD.total_market_cap') not found in raw Alternative.me global data")
            
            total_market_cap = data['quotes']['USD']['total_market_cap']
            
            # Extract timestamp if available
            timestamp = data.get('last_updated')
            timestamp_str = datetime.fromtimestamp(timestamp).isoformat() if timestamp else datetime.now().isoformat()
            
            return {
                "date": datetime.now().date().isoformat(),
                "value": total_market_cap,
                "units": "USD",
                "metadata": {
                    "source": "alternative.me",
                    "last_updated_at_source": timestamp_str
                }
            }
        except TransformationError:
            # Re-raise known errors
            raise
        except Exception as e:
            logger.error(f"Total market cap transformation failed: {e}")
            raise TransformationError(f"Total market cap transformation failed: {e}") 