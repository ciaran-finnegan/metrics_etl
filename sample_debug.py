#!/usr/bin/env python3
"""
Debug script to test the TwitterExtractor directly 
"""

import yaml
import sys
from etl.extract.twitter_extractor import TwitterExtractor
from utils.logging_config import setup_logging

def debug_extractor():
    # Set up logging
    setup_logging()
    
    # Create a test configuration with known handles
    test_config = {
        "handles": ["naval", "sama"],
        "days_lookback": 3,
        "tweets_per_user": 5,
        "include_replies": False,
        "include_retweets": False
    }
    
    # Print configuration
    print("Test configuration:")
    print(yaml.dump(test_config))
    
    # Initialize extractor
    print("\nInitializing extractor...")
    extractor = TwitterExtractor(params=test_config)
    
    # Extract data
    print("\nExtracting tweets...")
    result = extractor.extract()
    
    # Print result statistics
    print(f"\nExtraction complete. Results:")
    print(f"- Total tweets: {result.get('total_tweets', 0)}")
    print(f"- Handles processed: {result.get('handles_processed', 0)}")
    
    # Try to simulate the caching issue
    handles = result.get("handles_processed")
    tweets = result.get("total_tweets")
    
    print("\nTesting cache key creation...")
    try:
        # Convert param items to a tuple for hashing
        items = sorted(test_config.items())
        cache_key = ('etl.extract.twitter_extractor', 'TwitterExtractor', tuple(items))
        print("Cache key creation successful.")
    except Exception as e:
        print(f"Cache key creation failed: {type(e).__name__}: {e}")
        
    # Try to debug the tweet content
    print("\nTweet content sample:")
    tweets_by_handle = result.get('tweets_by_handle', {})
    for handle, tweets in tweets_by_handle.items():
        print(f"\nHandle: {handle}, Tweet count: {len(tweets)}")
        if tweets:
            first_tweet = tweets[0]
            # Check for potentially unhashable types
            for key, value in first_tweet.items():
                if isinstance(value, (list, dict)):
                    print(f"  - Potential issue: '{key}' is a {type(value).__name__}")
    
    print("\nDebug complete!")
    
if __name__ == "__main__":
    debug_extractor() 