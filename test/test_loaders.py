import os
from dotenv import load_dotenv
from etl.load.supabase_loader import SupabaseLoader
from etl.load.google_sheets_loader import GoogleSheetsLoader
from utils.logging_config import logger, setup_logging

def main():
    load_dotenv()
    setup_logging()
    
    logger.info("Starting loader tests...")

    # Test data
    test_data = {
        "date": "2024-01-01",
        "signal_name": "test_signal",
        "value": 100.0,
        "units": "USD"
    }
    logger.info(f"Test data: {test_data}")

    try:
        # Test Supabase
        logger.info("Testing Supabase loader...")
        supabase = SupabaseLoader(
            url=os.getenv("SUPABASE_URL"),
            key=os.getenv("SUPABASE_KEY")
        )
        supabase.load(test_data)
        logger.info("Supabase test completed successfully")

        # Test Google Sheets
        logger.info("Testing Google Sheets loader...")
        gsheets = GoogleSheetsLoader(
            creds_path="credentials.json"  # Using default sheet_name and worksheet
        )
        gsheets.load(test_data)
        logger.info("Google Sheets test completed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    main()