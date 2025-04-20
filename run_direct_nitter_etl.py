#!/usr/bin/env python
"""
Direct Nitter Twitter ETL Pipeline Runner

This script runs the full ETL pipeline for Twitter data using the DirectNitterTwitterExtractor.
It extracts tweets from financial influencers using Nitter instances (no API required),
performs sentiment analysis, and loads the results to Supabase and Google Sheets.
"""

import os
import sys
import logging
import argparse
import yaml
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/direct_nitter_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

logger = logging.getLogger(__name__)

# Import ETL modules
from etl.extract.direct_nitter_twitter_extractor import DirectNitterTwitterExtractor
from etl.transform.twitter_sentiment_transformer import TwitterSentimentTransformer
from etl.load.supabase_loader import SupabaseLoader
from etl.load.google_sheets_loader import GoogleSheetsLoader

def load_config():
    """Load the signals configuration."""
    config_path = Path("config/signals.yaml")
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    return config

def load_class(module_path, class_name):
    """Dynamically load a class from a module."""
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

def main():
    """Run the Direct Nitter Twitter ETL pipeline."""
    parser = argparse.ArgumentParser(description="Run the Direct Nitter Twitter ETL pipeline")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--signal", default="financial_tweets_direct_nitter", 
                        help="Signal name from signals.yaml to process")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
    
    logger.info(f"Starting Direct Nitter Twitter ETL pipeline for signal: {args.signal}")
    
    # Load configuration
    config = load_config()
    if "signals" not in config or args.signal not in config["signals"]:
        logger.error(f"Signal '{args.signal}' not found in configuration")
        return 1
    
    signal_config = config["signals"][args.signal]
    
    # Create directories if they don't exist
    os.makedirs("data/extracted", exist_ok=True)
    os.makedirs("data/transformed", exist_ok=True)
    
    # Extract
    logger.info("Starting extraction phase")
    try:
        extractor_config = signal_config["extractor"]
        extractor_class = DirectNitterTwitterExtractor
        if isinstance(extractor_config, dict) and "module" in extractor_config and "class" in extractor_config:
            extractor_module = extractor_config["module"]
            extractor_class_name = extractor_config["class"]
            extractor_class = load_class(extractor_module, extractor_class_name)
        
        extractor = extractor_class(extractor_config.get("params", {}))
        extracted_data = extractor.extract()
        extractor.save(extracted_data)
        
        logger.info(f"Extraction completed: {len(extracted_data.get('tweets', []))} tweets from {len(extracted_data.get('profiles', []))} profiles")
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}", exc_info=True)
        return 1
    
    # Transform
    logger.info("Starting transformation phase")
    try:
        transformer_config = signal_config["transformer"]
        transformer_class = TwitterSentimentTransformer
        if isinstance(transformer_config, dict) and "module" in transformer_config and "class" in transformer_config:
            transformer_module = transformer_config["module"]
            transformer_class_name = transformer_config["class"]
            transformer_class = load_class(transformer_module, transformer_class_name)
        
        transformer = transformer_class(transformer_config.get("params", {}))
        transformed_data = transformer.transform()
        transformer.save(transformed_data)
        
        logger.info(f"Transformation completed: processed {len(transformed_data.get('tweets', []))} tweets")
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}", exc_info=True)
        return 1
    
    # Load
    logger.info("Starting loading phase")
    if "loaders" in signal_config:
        for i, loader_config in enumerate(signal_config["loaders"]):
            try:
                logger.info(f"Running loader {i+1}/{len(signal_config['loaders'])}")
                
                loader_class = None
                if "module" in loader_config and "class" in loader_config:
                    loader_module = loader_config["module"]
                    loader_class_name = loader_config["class"]
                    loader_class = load_class(loader_module, loader_class_name)
                elif "type" in loader_config:
                    if loader_config["type"] == "supabase_loader":
                        loader_class = SupabaseLoader
                    elif loader_config["type"] == "google_sheets_loader":
                        loader_class = GoogleSheetsLoader
                
                if loader_class:
                    loader = loader_class(loader_config.get("params", {}))
                    loader.load(transformed_data)
                    logger.info(f"Loader {i+1} completed successfully")
                else:
                    logger.error(f"Could not determine loader class for loader {i+1}")
            except Exception as e:
                logger.error(f"Loader {i+1} failed: {str(e)}", exc_info=True)
    
    logger.info("Direct Nitter Twitter ETL pipeline completed successfully")
    return 0

if __name__ == "__main__":
    exit(main()) 