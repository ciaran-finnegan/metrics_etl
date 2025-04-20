#!/usr/bin/env python
"""
Test script for the RapidAPITwitterExtractor with usage tracking.
This script demonstrates how to use the extractor and verify that usage tracking works.
"""
import json
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Add the project root to sys.path to ensure imports work
sys.path.append(str(Path(__file__).parent))

# Import the extractor
from etl.extract.rapidapi_twitter_extractor import RapidAPITwitterExtractor

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Set up a test usage file to avoid interfering with actual usage tracking
    test_usage_file = "test_twitter241_api_usage.json"
    
    # Clean up previous test file if it exists
    if os.path.exists(test_usage_file):
        os.remove(test_usage_file)
        logger.info(f"Removed previous test usage file: {test_usage_file}")
    
    # Get API key from environment variable
    api_key = os.environ.get("RAPIDAPI_KEY")
    if not api_key:
        logger.error("RAPIDAPI_KEY not found in .env file or environment variables. Please add it to your .env file.")
        return
    else:
        logger.info("Successfully loaded RAPIDAPI_KEY from environment")
    
    # First, initialize an extractor just to check usage stats without making API calls
    logger.info("Initializing extractor for usage stats...")
    usage_extractor = RapidAPITwitterExtractor({
        "api_key": api_key,
        "operation": "usage_stats",
        "usage_file": test_usage_file,
        "enforce_limits": True
    })
    
    # Get initial usage (should be 0 since we're using a fresh test file)
    usage_data = usage_extractor.extract()
    logger.info(f"Initial usage: {json.dumps(usage_data['usage'], indent=2)}")
    
    # Now test with a minimal API call (using low count to conserve API calls)
    should_make_api_call = input("Make an API call to test? (y/n): ").strip().lower() == 'y'
    
    if should_make_api_call:
        logger.info("Testing with a minimal API call (retrieving 1 tweet)...")
        search_extractor = RapidAPITwitterExtractor({
            "api_key": api_key,
            "operation": "search",
            "query": "python",  # A simple search query
            "count": 1,  # Minimal count to conserve API calls
            "usage_file": test_usage_file,
            "enforce_limits": True
        })
        
        # Make the API call
        search_results = search_extractor.extract()
        logger.info(f"Search returned {len(search_results.get('tweets', []))} tweets")
        
        # If we got results, show the first tweet text
        if search_results.get('tweets'):
            first_tweet = search_results['tweets'][0]
            logger.info(f"Sample tweet: {first_tweet.get('text', '')[:100]}...")
        
        # Check updated usage
        logger.info("Checking updated usage stats...")
        updated_usage = RapidAPITwitterExtractor({
            "api_key": api_key,
            "operation": "usage_stats",
            "usage_file": test_usage_file
        }).extract()
        
        logger.info(f"Updated usage: {json.dumps(updated_usage['usage'], indent=2)}")
        
        # Verify calls_this_month increased by at least 1
        initial_calls = usage_data['usage']['calls_made']
        updated_calls = updated_usage['usage']['calls_made']
        if updated_calls > initial_calls:
            logger.info(f"✅ Usage tracking confirmed: Calls increased from {initial_calls} to {updated_calls}")
        else:
            logger.warning(f"⚠️ Usage tracking may not be working: Calls still at {updated_calls}")
    
    logger.info("Test complete!")
    
    # Clean up test file if desired
    should_cleanup = input("Remove test usage file? (y/n): ").strip().lower() == 'y'
    if should_cleanup and os.path.exists(test_usage_file):
        os.remove(test_usage_file)
        logger.info(f"Removed test usage file: {test_usage_file}")

if __name__ == "__main__":
    main() 