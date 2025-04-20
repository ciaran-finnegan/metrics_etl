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
            value = data.get("value") # Allow value to be None or 0

            if not signal_name:
                raise LoadError(f"Missing required field 'signal_name' in data: {data}")
            if value is None:
                 logger.warning(f"Value is None for signal '{signal_name}'. Inserting record with null value.")

            record = {
                "date": data.get("date", datetime.now().date().isoformat()),
                "signal_name": signal_name,
                "value": value,
                "units": data.get("units"), # Get units, could be None
                "metadata": {
                    k: v for k, v in data.get("metadata", {}).items() # Use metadata sub-dict if present
                }
            }
            
            # Add any top-level fields not in core list or 'metadata' key to metadata dict
            core_fields = ["date", "signal_name", "value", "units", "metadata"]
            for k, v in data.items():
                if k not in core_fields:
                    record["metadata"][k] = v
            
            # Ensure metadata is not empty before logging/inserting an empty object {}?
            # Or let Supabase handle potential empty JSON objects if needed.
            
            logger.info(f"Attempting to insert record into Supabase table '{self.table}': {record}")

            result = self.client.table(self.table).insert(record).execute()

            # Check Supabase response
            if hasattr(result, 'error') and result.error:
                logger.error(f"Supabase insert error response: {result.error}")
                raise LoadError(f"Supabase insert error: {result.error.message}")
            if not result.data:
                # Sometimes insert might succeed but return empty data, treat as warning? Or error?
                logger.warning(f"No data returned from Supabase insert, but no explicit error. Response: {result}")
                # raise etl.custom_exceptions.LoadError(f"No data returned from Supabase insert. Response: {result}")

            logger.info(f"Successfully inserted data into Supabase for {record['signal_name']}: {result.data}")
            return True

        except LoadError: # Re-raise known LoadErrors
             raise
        except Exception as e:
            logger.error(f"Supabase load failed unexpectedly: {type(e).__name__} - {str(e)}")
            logger.error(f"Failed data: {data}")
            raise LoadError(f"Supabase load error: {type(e).__name__} - {str(e)}")