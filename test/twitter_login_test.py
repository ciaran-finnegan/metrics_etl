#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Twitter login and tweet extraction test script.

Tests the ability to log into Twitter using the TwitterLoginExtractor and extract tweets from specified handles.
Shows the login process and tweet extraction for debugging.
"""

import os
import sys
import time
import logging
import json
import random
from pathlib import Path
from pprint import pprint
import traceback

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from playwright.sync_api import sync_playwright
from etl.extract.twitter_login_extractor import TwitterLoginExtractor
from utils.logging_config import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def wait_for_user(message="Press Enter to continue..."):
    """Wait for user input before continuing"""
    try:
        input(f"\n{message}\n")
    except KeyboardInterrupt:
        logger.info("User interrupted the process")
        sys.exit(1)

def main():
    """Main function to run the Twitter login and tweet extraction test."""
    logger.info("Starting Twitter login and tweet extraction test")
    
    # Get credentials either from file or environment variables
    try:
        # First try from environment variables
        username = os.environ.get('TWITTER_USERNAME')
        password = os.environ.get('TWITTER_PASSWORD')
        verification_code = os.environ.get('TWITTER_VERIFICATION_CODE', '')
        
        # If environment variables not set, try from file
        if not username or not password:
            credentials_file = 'config/twitter_login.json'
            if os.path.exists(credentials_file):
                with open(credentials_file, 'r') as f:
                    credentials = json.load(f)
                    username = credentials.get('username')
                    password = credentials.get('password')
                    verification_code = credentials.get('verification_code', '')
        
        # Validate credentials
        if not username or not password:
            logger.error("No Twitter credentials found. Please set TWITTER_USERNAME and TWITTER_PASSWORD environment variables or create config/twitter_login.json")
            return False
            
        # Create user data directory for persistent sessions
        user_data_dir = os.path.join(os.path.expanduser('~'), '.twitter_test_browser_data')
        os.makedirs(user_data_dir, exist_ok=True)
        
        # Configure the extractor with persistent browser data
        extractor_params = {
            'username': username,
            'password': password,
            'verification_code': verification_code,
            'handles': ['elonmusk', 'financialtimes', 'WSJ'],
            'max_tweets_per_handle': 5,
            'take_screenshots': True,
            'screenshots_dir': 'test_screenshots',
            'headless': False,
            'user_data_dir': user_data_dir
        }
        
        # Create screenshots directory
        os.makedirs(extractor_params['screenshots_dir'], exist_ok=True)
        
        # Create the extractor
        extractor = TwitterLoginExtractor(extractor_params)
        
        # Attempt to extract tweets
        try:
            logger.info("Starting Twitter extraction")
            result = extractor.extract()
            
            # Check results
            tweets_by_handle = result.get('tweets_by_handle', {})
            total_tweets = sum(len(tweets) for tweets in tweets_by_handle.values())
            
            logger.info(f"Extraction complete. Total tweets extracted: {total_tweets}")
            
            # Print summary for each handle
            for handle, tweets in tweets_by_handle.items():
                logger.info(f"  @{handle}: {len(tweets)} tweets")
            
            # Save results to file
            output_file = 'test_output/twitter_extraction_results.json'
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Results saved to {output_file}")
            
            # Test was successful
            logger.info("Test completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Test failed: {str(e)}")
            if hasattr(e, "__traceback__"):
                traceback.print_tb(e.__traceback__)
            return False
            
    except Exception as e:
        logger.error(f"Error loading credentials: {str(e)}")
        return False

if __name__ == "__main__":
    main() 