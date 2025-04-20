#!/usr/bin/env python
"""
Script to run the Direct Nitter ETL pipeline for Twitter data.
"""

import os
import sys
import time
import json
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv

# Add the root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.extract.direct_nitter_twitter_extractor import DirectNitterTwitterExtractor
from etl.transform.twitter_sentiment_transformer import TwitterSentimentTransformer
from etl.load.supabase_loader import SupabaseLoader
from etl.load.google_sheets_loader import GoogleSheetsLoader
from etl.load.json_file_loader import JSONFileLoader
from utils.config_loader import load_signal_from_yaml

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(log_dir, exist_ok=True)

log_filename = f"direct_nitter_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(log_dir, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_direct_nitter_etl(signal_name):
    """
    Run the Direct Nitter ETL pipeline for a specific signal.
    
    Args:
        signal_name (str): Name of the signal in the config file
    """
    load_dotenv()
    
    logger.info(f"Starting Direct Nitter ETL pipeline for signal: {signal_name}")
    
    try:
        # Load signal configuration
        signal_config = load_signal_from_yaml(signal_name)
        if not signal_config:
            logger.error(f"Signal '{signal_name}' not found in configuration")
            return False
        
        # Extract data
        logger.info("Starting extraction phase")
        extractor_config = signal_config.get("extractor", {})
        extractor_params = extractor_config.get("params", {})
        
        extractor = DirectNitterTwitterExtractor(**extractor_params)
        extraction_start_time = time.time()
        
        output_file = extractor_config.get("output")
        extracted_data = extractor.extract()
        if extracted_data:
            logger.info(f"Extraction complete. Extracted data for {len(extracted_data.get('tweets_by_handle', {}))} handles")
            extractor.save(output_file)
        else:
            logger.error("Extraction failed. No data extracted.")
            return False
        
        extraction_time = time.time() - extraction_start_time
        logger.info(f"Extraction completed in {extraction_time:.2f} seconds")
        
        # Transform data
        logger.info("Starting transformation phase")
        transformer_config = signal_config.get("transformer", {})
        transformer_params = transformer_config.get("params", {})
        
        # Add file paths to transformer params
        transformer_params["input_file"] = output_file
        transformer_params["output_file"] = transformer_config.get("output")
        
        transformer = TwitterSentimentTransformer(**transformer_params)
        transformation_start_time = time.time()
        
        transformed_data = transformer.transform(extracted_data)
        if transformed_data:
            logger.info(f"Transformation complete. Calculated sentiment: {transformed_data.get('sentiment_score', 'N/A')}")
            if transformer_config.get("output"):
                transformer.save(transformer_config.get("output"), transformed_data)
        else:
            logger.warning("Transformation returned no data. Proceeding with loaders anyway.")
            
        transformation_time = time.time() - transformation_start_time
        logger.info(f"Transformation completed in {transformation_time:.2f} seconds")
        
        # Load data
        logger.info("Starting loading phase")
        loaders = signal_config.get("loaders", [])
        
        for loader_config in loaders:
            loader_type = loader_config.get("type")
            loader_params = loader_config.get("params", {})
            
            try:
                if loader_type == "supabase":
                    loader = SupabaseLoader(**loader_params)
                    loader.load(transformed_data)
                    logger.info(f"Data loaded to Supabase table: {loader_params.get('table_name')}")
                    
                elif loader_type == "google_sheets":
                    loader = GoogleSheetsLoader(**loader_params)
                    loader.load(transformed_data)
                    logger.info(f"Data loaded to Google Sheets: {loader_params.get('spreadsheet_id')}")
                    
                elif loader_type == "json_file":
                    loader = JSONFileLoader(**loader_params)
                    loader.load(transformed_data)
                    logger.info(f"Data loaded to JSON file: {loader_params.get('output_file')}")
                    
                else:
                    logger.warning(f"Unknown loader type: {loader_type}")
                    
            except Exception as e:
                logger.error(f"Error loading data with {loader_type} loader: {str(e)}")
        
        logger.info("ETL pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error running ETL pipeline: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Direct Nitter ETL pipeline")
    parser.add_argument("--signal", required=True, help="Signal name from the configuration file")
    
    args = parser.parse_args()
    
    success = run_direct_nitter_etl(args.signal)
    sys.exit(0 if success else 1) 