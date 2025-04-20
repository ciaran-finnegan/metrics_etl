#!/usr/bin/env python
"""
Test script for DirectNitterTwitterExtractor and TwitterSentimentTransformer.

This script extracts tweets from specified financial Twitter influencers using the
DirectNitterTwitterExtractor and then performs sentiment analysis on the extracted
tweets using the TwitterSentimentTransformer.
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.extract.direct_nitter_twitter_extractor import DirectNitterTwitterExtractor
from etl.transform.twitter_sentiment_transformer import TwitterSentimentTransformer


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/direct_nitter_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

logger = logging.getLogger(__name__)


def main():
    """
    Run the test for DirectNitterTwitterExtractor and TwitterSentimentTransformer.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Create output directory if it doesn't exist
    output_dir = Path("data/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define financial influencer Twitter handles
    handles = [
        "@matt_willemsen",  # Start with a reliable handle for testing
        "@docXBT",
        "@raoulGMI",
        "@IrvingBuyTheDip",
        "@JulianKlymochko",
        "@FinanceWill"
    ]
    
    # Define file paths with timestamps
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    extracted_file = output_dir / f"financial_tweets_direct_nitter_{timestamp}.json"
    transformed_file = output_dir / f"financial_tweets_sentiment_{timestamp}.json"
    
    logger.info("Starting extraction with DirectNitterTwitterExtractor")
    
    # More reliable Nitter instances
    nitter_instances = [
        "https://nitter.net",
        "https://nitter.42l.fr",
        "https://nitter.pussthecat.org",
        "https://nitter.fdn.fr",
        "https://nitter.esmailelbob.xyz",
    ]
    
    # SSL verification settings - disable for instances with certificate issues
    verify_ssl = {
        "nitter.esmailelbob.xyz": False,
        "nitter.pussthecat.org": False
    }
    
    # Initialize extractor
    extractor = DirectNitterTwitterExtractor({
        "handles": handles,
        "tweets_per_user": 5,
        "min_request_interval": 1.5,
        "max_request_interval": 3.0,
        "max_retries": 3,
        "include_replies": False,
        "include_retweets": False,
        "timeout": 15,
        "output_file": str(extracted_file),
        "nitter_instances": nitter_instances,
        "verify_ssl": verify_ssl,
        "default_verify": True
    })
    
    # Extract tweets
    logger.info(f"Extracting tweets from {len(handles)} handles")
    extracted_data = extractor.extract()
    
    # Save extracted data
    extractor.save(extracted_data)
    
    # Check if any tweets were extracted
    if not extracted_data["tweets"]:
        logger.error("No tweets extracted, exiting")
        return 1
    
    logger.info(f"Extracted {len(extracted_data['tweets'])} tweets from {len(extracted_data['profiles'])} profiles")
    
    # Initialize transformer
    transformer = TwitterSentimentTransformer({
        "input_file": str(extracted_file),
        "output_file": str(transformed_file)
    })
    
    # Transform data
    logger.info("Starting sentiment analysis with TwitterSentimentTransformer")
    transformed_data = transformer.transform()
    
    # Save transformed data
    transformer.save(transformed_data)
    
    # Log number of tweets transformed
    if transformed_data and "tweets" in transformed_data:
        logger.info(f"Successfully transformed {len(transformed_data['tweets'])} tweets")
        
        # Print a sample tweet with sentiment
        if transformed_data["tweets"]:
            sample_tweet = transformed_data["tweets"][0]
            logger.info(f"Sample tweet sentiment: {sample_tweet['sentiment']} (confidence: {sample_tweet['sentiment_confidence']:.2f})")
            logger.info(f"Sample tweet text: {sample_tweet['text'][:100]}...")
    else:
        logger.error("No tweets were transformed")
        return 1
    
    logger.info("Test completed successfully")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the DirectNitterTwitterExtractor and TwitterSentimentTransformer")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
    
    exit(main()) 