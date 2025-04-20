#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for TwitterLoginExtractor.

This script demonstrates how to use the TwitterLoginExtractor class to extract tweets
from specific Twitter handles by logging in with valid credentials.
"""

import os
import sys
import json
import logging
from datetime import datetime

# Add parent directory to path to import from etl module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.extract.twitter_login_extractor import TwitterLoginExtractor
from etl.utils.logging_config import setup_logging

# Set up logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Test the TwitterLoginExtractor by extracting tweets from specified handles.
    """
    # Twitter credentials - replace with your own
    username = os.environ.get("TWITTER_USERNAME", "")
    password = os.environ.get("TWITTER_PASSWORD", "")
    # Optional verification code for 2FA
    verification_code = os.environ.get("TWITTER_VERIFICATION_CODE", "")
    
    # Check if credentials are provided
    if not username or not password:
        logger.error("Twitter credentials not provided. Set TWITTER_USERNAME and TWITTER_PASSWORD environment variables.")
        return
    
    # Test handles
    handles = ["elonmusk", "OpenAI", "FinancialTimes"]
    
    # Create output directory if it doesn't exist
    output_dir = "data/twitter"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create screenshots directory if it doesn't exist
    screenshots_dir = "data/screenshots"
    os.makedirs(screenshots_dir, exist_ok=True)
    
    # Storage state path to save/load browser state
    storage_state_path = os.path.join(output_dir, "twitter_storage_state.json")
    
    # Initialize extractor
    extractor = TwitterLoginExtractor(
        username=username,
        password=password,
        verification_code=verification_code,
        handles=handles,
        tweets_per_user=5,  # Limit to 5 tweets per user for testing
        min_request_interval=2.0,
        max_request_interval=5.0,
        max_retries=3,
        include_replies=False,
        include_retweets=True,
        headless=False,  # Set to True for production use
        output_file=os.path.join(output_dir, "twitter_login_results.json"),
        take_screenshots=True,
        screenshots_dir=screenshots_dir,
        storage_state_path=storage_state_path
    )
    
    # Extract tweets
    logger.info(f"Starting extraction for handles: {handles}")
    result = extractor.extract()
    
    # Print extraction statistics
    logger.info(f"Extraction complete.")
    logger.info(f"Total tweets extracted: {len(result['tweets'])}")
    logger.info(f"Total profiles extracted: {len(result['profiles'])}")
    
    if result["errors"]:
        logger.warning(f"Errors encountered: {len(result['errors'])}")
        for error in result["errors"]:
            logger.warning(f"Error: {error}")
    
    # Save results to JSON file
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"twitter_login_results_{current_timestamp}.json")
    
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Results saved to {output_file}")

if __name__ == "__main__":
    main() 