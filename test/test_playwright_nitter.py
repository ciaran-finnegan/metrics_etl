#!/usr/bin/env python
"""
Test script for PlaywrightNitterExtractor.

This script extracts tweets from specified Twitter handles using the
PlaywrightNitterExtractor, which uses Playwright for browser automation and 
OpenAI's vision capabilities for analyzing screenshots of tweets.
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

from etl.extract.playwright_nitter_extractor import PlaywrightNitterExtractor


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/playwright_nitter_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

logger = logging.getLogger(__name__)


def main():
    """
    Run the test for PlaywrightNitterExtractor.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Create output directories if they don't exist
    output_dir = Path("data/output")
    screenshots_dir = Path("data/screenshots/twitter")
    output_dir.mkdir(parents=True, exist_ok=True)
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    
    # Define Twitter handles to extract from
    handles = [
        "@matt_willemsen",  # Primary test handle
        "@JulianKlymochko",
        "@FinanceWill"
    ]
    
    # Define file paths with timestamps
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    extracted_file = output_dir / f"twitter_playwright_{timestamp}.json"
    
    logger.info("Starting extraction with PlaywrightNitterExtractor")
    
    # Nitter instances to try - using only nitter.net as requested
    nitter_instances = [
        "https://nitter.net"
    ]
    
    # Initialize extractor with longer wait times and more retries
    extractor = PlaywrightNitterExtractor({
        "handles": handles,
        "tweets_per_user": 5,        # Increased to get more tweets
        "min_wait_time": 2.0,
        "max_wait_time": 4.0,
        "max_retries": 5,
        "include_replies": True,     # Include all types of tweets
        "include_retweets": True,
        "output_file": str(extracted_file),
        "nitter_instances": nitter_instances,
        "screenshots_dir": str(screenshots_dir),
        "headless": True,
        "browser_type": "chromium",
        "human_like_scrolling": True,
        "random_mouse_movements": True,
        "use_random_order": False,
        "continue_on_error": True
    })
    
    # Extract tweets
    logger.info(f"Extracting tweets from {len(handles)} handles")
    extracted_data = extractor.extract()
    
    # Save extracted data
    extractor.save(extracted_data)
    
    # Check if any tweets were extracted
    tweets = extracted_data.get("all_tweets", [])
    if not tweets:
        logger.error("No tweets extracted, exiting")
        return 1
    
    logger.info(f"Extracted {len(tweets)} tweets from {len(extracted_data.get('profiles', {}))} profiles")
    
    # Log some sample tweets
    if tweets:
        sample_tweet = tweets[0]
        logger.info(f"Sample tweet from @{sample_tweet.get('username')}:")
        logger.info(f"Text: {sample_tweet.get('text', '')[:100]}...")
        
        if "sentiment" in sample_tweet:
            logger.info(f"Sentiment: {sample_tweet.get('sentiment')}")
        
        if "stats" in sample_tweet:
            stats = sample_tweet.get("stats", {})
            logger.info(f"Stats: {stats.get('likes', 0)} likes, {stats.get('retweets', 0)} retweets")
    
    logger.info("Test completed successfully")
    logger.info(f"Results saved to {extracted_file}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the PlaywrightNitterExtractor")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
    
    exit(main()) 