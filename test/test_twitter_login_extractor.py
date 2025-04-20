#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for TwitterLoginExtractor.

This script tests the TwitterLoginExtractor class functionality.
It requires valid Twitter credentials to run properly.
"""

import os
import sys
import json
import time
import logging
import argparse
from dotenv import load_dotenv
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extract.twitter_login_extractor import TwitterLoginExtractor
from utils.logging_config import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def main():
    """
    Test the TwitterLoginExtractor class.
    
    This function initializes the TwitterLoginExtractor with credentials,
    extracts tweets from specified handles, and outputs the results.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test the TwitterLoginExtractor')
    parser.add_argument('--handles', nargs='+', help='Twitter handles to extract from')
    parser.add_argument('--tweets-per-user', type=int, default=5, help='Number of tweets to extract per user')
    parser.add_argument('--output-file', type=str, help='Output file to save extracted data')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--include-replies', action='store_true', help='Include replies in extracted tweets')
    parser.add_argument('--include-retweets', action='store_true', help='Include retweets in extracted tweets')
    parser.add_argument('--screenshots-dir', type=str, help='Directory to save screenshots to')
    parser.add_argument('--storage-state-path', type=str, help='Path to save/load browser state')
    
    args = parser.parse_args()
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Get credentials from environment variables
    username = os.getenv('TWITTER_EMAIL') or os.getenv('TWITTER_USERNAME')
    password = os.getenv('TWITTER_PASSWORD')
    verification_code = os.getenv('TWITTER_VERIFICATION_CODE')
    
    if not username or not password:
        logger.error("Twitter credentials are required. Set TWITTER_EMAIL (or TWITTER_USERNAME) and TWITTER_PASSWORD environment variables.")
        return
        
    # Set default handles if not provided
    handles = args.handles or ['@matt_willemsen', '@elonmusk', '@BillGates']
    
    # Set output file if not provided
    output_file = args.output_file or 'twitter_login_output.json'
    
    # Set screenshots directory if not provided but screenshots are enabled
    screenshots_dir = args.screenshots_dir or 'screenshots'
    take_screenshots = args.screenshots_dir is not None
    
    # Print test configuration
    logger.info(f"Testing TwitterLoginExtractor with the following configuration:")
    logger.info(f"- Handles: {handles}")
    logger.info(f"- Tweets per user: {args.tweets_per_user}")
    logger.info(f"- Include replies: {args.include_replies}")
    logger.info(f"- Include retweets: {args.include_retweets}")
    logger.info(f"- Headless mode: {args.headless}")
    logger.info(f"- Output file: {output_file}")
    logger.info(f"- Take screenshots: {take_screenshots}")
    if take_screenshots:
        logger.info(f"- Screenshots directory: {screenshots_dir}")
    if args.storage_state_path:
        logger.info(f"- Storage state path: {args.storage_state_path}")
    
    # Initialize the extractor
    extractor = TwitterLoginExtractor(
        username=username,
        password=password,
        verification_code=verification_code,
        handles=handles,
        tweets_per_user=args.tweets_per_user,
        min_request_interval=2.0,
        max_request_interval=5.0,
        max_retries=3,
        include_replies=args.include_replies,
        include_retweets=args.include_retweets,
        headless=args.headless,
        output_file=output_file,
        take_screenshots=take_screenshots,
        screenshots_dir=screenshots_dir if take_screenshots else None,
        storage_state_path=args.storage_state_path
    )
    
    # Start extraction timer
    start_time = time.time()
    logger.info("Starting tweet extraction...")
    
    # Run extraction
    result = extractor.extract()
    
    # End extraction timer
    end_time = time.time()
    duration = end_time - start_time
    
    # Print results
    tweet_count = len(result.get("tweets", []))
    profile_count = len(result.get("profiles", []))
    error_count = len(result.get("errors", []))
    
    logger.info(f"Extraction completed in {duration:.2f} seconds")
    logger.info(f"Extracted {tweet_count} tweets and {profile_count} profiles")
    
    if error_count > 0:
        logger.warning(f"Encountered {error_count} errors during extraction:")
        for error in result.get("errors", []):
            logger.warning(f"- {error}")
    
    # Save results to file
    if output_file:
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        logger.info(f"Results saved to {output_file}")
    
    # Print sample tweet if available
    if tweet_count > 0:
        sample_tweet = result["tweets"][0]
        logger.info("Sample tweet extracted:")
        logger.info(f"- User: @{sample_tweet.get('username', 'unknown')}")
        logger.info(f"- Text: {sample_tweet.get('text', 'No text')[:100]}...")
        logger.info(f"- Date: {sample_tweet.get('date', 'unknown')}")
        logger.info(f"- Likes: {sample_tweet.get('likes', 0)}")
        logger.info(f"- Retweets: {sample_tweet.get('retweets', 0)}")

if __name__ == "__main__":
    main() 