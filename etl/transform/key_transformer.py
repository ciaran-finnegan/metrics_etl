from datetime import datetime
from utils.exceptions import TransformationError
from utils.logging_config import logger

class KeyTransformer:
    """
    A simple transformer that extracts a specific key from the data.
    Useful for CoinGecko responses where different signals come from the same API response.
    """
    
    def __init__(self, params=None):
        """
        Initialize the KeyTransformer with the key to extract
        
        Args:
            params (dict): Dictionary with 'key' parameter specifying which key to extract
        """
        self.params = params or {}
        self.key = self.params.get('key')
        
        if not self.key:
            raise ValueError("KeyTransformer requires 'key' parameter")
    
    def transform(self, data):
        """
        Extract the specified key from the data
        
        Args:
            data (dict): The data dictionary to transform
            
        Returns:
            dict: A dictionary with the extracted value
        """
        try:
            if self.key not in data:
                raise TransformationError(f"Key '{self.key}' not found in data")
                
            value = data.get(self.key)
            
            return {
                "date": datetime.now().date().isoformat(),
                "value": value,
                "units": "USD" if "usd" in self.key.lower() else "",
            }
        except Exception as e:
            logger.error(f"KeyTransformer failed: {e}")
            raise TransformationError(f"KeyTransformer error: {e}") 