import logging
from datetime import datetime
from supabase import create_client
from utils.exceptions import LoadError
from utils.logging_config import logger
from typing import Dict, Any

class SupabaseLoader:
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the Supabase client using a configuration dictionary.
        Args:
            config (Dict[str, Any]): Dictionary containing:
                - url (str): Supabase project URL.
                - key (str): Supabase API key.
                - table (str, optional): Target table name (defaults to "financial_signals").
        """
        try:
            url = config.get("url")
            key = config.get("key")
            self.table = config.get("table", "financial_signals")

            if not url or not key:
                raise ValueError("Supabase URL and Key must be provided in config.")

            self.client = create_client(url, key)
            logger.info(f"Supabase connected to table: {self.table}")
        except ValueError as ve:
             logger.error(f"Supabase configuration error: {ve}")
             raise LoadError(f"Supabase configuration error: {ve}")
        except Exception as e:
            logger.error(f"Supabase connection failed: {e}")
            raise LoadError(f"Supabase init error: {e}")

    def load(self, data: dict) -> bool:
        """Insert data into Supabase table with JSON metadata"""
        try:
            if not data:
                raise LoadError("Empty data received by SupabaseLoader")

            signal_name = data.get("signal_name")
            value = float(data["value"])

            if not signal_name:
                raise LoadError(f"Missing required field 'signal_name' in data: {data}")

            record = {
                "date": data.get("date", datetime.now().date().isoformat()),
                "signal_name": signal_name,
                "value": value,
                "units": data.get("units", ""),
                "day_change": data.get("day_change", 0.0)
            }
            
            logger.debug(f"Date value before loading: {record['date']} (type: {type(record['date'])})")
            logger.debug(f"Full record before loading: {record}")
            
            # Use upsert instead of insert to handle duplicates gracefully
            result = self.client.table(self.table).upsert(record, on_conflict="date,signal_name").execute()

            # Check Supabase response
            if hasattr(result, 'error') and result.error:
                # Log the raw error for debugging
                logger.error(f"Supabase upsert error response: {result.error}")
                raise LoadError(f"Supabase upsert error: {result.error.message}")
            
            # Check if data was returned (indicates insert/update occurred)
            if not result.data:
                 logger.warning(f"No data returned from Supabase upsert for signal '{signal_name}', but no explicit error. Assuming record already existed or no change was needed. Response: {result}")
                 return True

            logger.info(f"Successfully upserted data in Supabase for {record['signal_name']}: {result.data}")
            return True

        except LoadError: # Re-raise known LoadErrors
             raise
        except Exception as e:
            # Catching generic exception - check if it's a Supabase APIError related to duplicate key
            error_str = str(e)
            error_type = type(e).__name__
            
            # Check if it's likely the duplicate key error from the database directly
            # The Supabase client might wrap this differently sometimes.
            if "23505" in error_str and "duplicate key value violates unique constraint" in error_str:
                 logger.warning(
                     f"Caught duplicate key violation (23505) during Supabase load for signal '{data.get('signal_name')}'. "
                     f"This suggests an issue potentially bypassing the insert logic or a race condition. Assuming record exists."
                 )
                 logger.debug(f"Original exception details: {error_type} - {error_str}")
                 logger.debug(f"Data attempted: {data}")
                 return True # Treat as success, as the record likely exists
            else:
                # Log and raise for other unexpected errors
                logger.error(f"Supabase load failed unexpectedly: {error_type} - {error_str}")
                logger.error(f"Failed data: {data}")
                raise LoadError(f"Supabase load error: {error_type} - {error_str}")