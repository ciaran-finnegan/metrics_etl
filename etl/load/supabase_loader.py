import logging
from datetime import datetime
from supabase import create_client
from utils.exceptions import LoadError
from utils.logging_config import logger

class SupabaseLoader:
    def __init__(self, url: str, key: str, table: str = "financial_signals"):
        """
        Args:
            url: Supabase project URL (e.g., "https://xyz.supabase.co")
            key: Supabase API key
            table: Target table name (defaults to financial_signals)
        """
        try:
            self.client = create_client(url, key)
            self.table = table
            logger.info(f"Supabase connected to table: {table}")
        except Exception as e:
            logger.error(f"Supabase connection failed: {e}")
            raise LoadError("Supabase init error")

    def load(self, data: dict) -> bool:
        """Insert data into Supabase table with JSON metadata"""
        try:
            if not data:
                raise LoadError("Empty data received")
                
            # Extract core fields
            signal_name = data.get("signal_name")
            value = data.get("value")
            
            if not all([signal_name, value]):
                raise LoadError(f"Missing required fields in data: {data}")
            
            # Build record with core fields and metadata
            record = {
                "date": data.get("date", datetime.now().date().isoformat()),
                "signal_name": signal_name,
                "value": value,
                "units": data.get("units", "USD"),  # Default to USD
                "metadata": {
                    # Include all additional fields in metadata
                    k: v for k, v in data.items() 
                    if k not in ["date", "signal_name", "value", "units"]
                }
            }
            
            logger.info(f"Attempting to insert record: {record}")
            
            try:
                # Insert into Supabase
                result = self.client.table(self.table).insert(record).execute()
                
                if not result.data:
                    raise LoadError(f"No data returned from Supabase insert. Response: {result}")
                    
                logger.info(f"Successfully inserted data for {record['signal_name']}: {result.data}")
                return True
                
            except Exception as e:
                logger.error(f"Supabase insert failed. Error type: {type(e).__name__}")
                logger.error(f"Error message: {str(e)}")
                logger.error(f"Record being inserted: {record}")
                raise LoadError(f"Supabase insert error: {type(e).__name__} - {str(e)}")
            
        except Exception as e:
            logger.error(f"Supabase load failed: {str(e)}")
            logger.error(f"Failed data: {data}")
            raise LoadError(f"Supabase load error: {str(e)}")