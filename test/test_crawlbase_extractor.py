#!/usr/bin/env python3
"""
Test script for the CrawlbaseTwitterExtractor
"""
import os
import json
import logging
import sys
from pathlib import Path

# Add parent directory to path to allow importing modules from the project
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from etl.extract.crawlbase_twitter_extractor import CrawlbaseTwitterExtractor
from etl.transform.sentiment_analysis import SentimentAnalyzer

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# List of Twitter handles to test
TEST_HANDLES = [
    "artemis", "matt_willemsen", "docXBT", "raoulGMI",
    "superstevefarms", "branbtc", "apompliano", "jamie1coutts",
    "breedlove22", "karpathy", "naval"
]

def main():
    # Load environment variables
    load_dotenv()
    
    # Use the JavaScript token as Twitter is JavaScript-heavy
    api_token = "GpsUqR7qNw5yasInk0N6AA"  # JavaScript token from Crawlbase
    
    # Test with matt_willemsen as the example handle
    test_handle = "matt_willemsen"
    
    logger.info("Starting Crawlbase Twitter extractor test")
    
    # Initialize the extractor for user tweets
    tweet_params = {
        "api_token": api_token,
        "operation": "user_tweets",
        "username": test_handle,
        "count": 3,  # Limited for testing
        "javascript_enabled": True,
        "min_request_interval": 2.0,
        "output_file": f"data/extracted/{test_handle}_tweets_test.json",
        "use_x_domain": False,  # Use twitter.com instead of x.com
        "use_dify_format": True  # Use the format from Dify.ai documentation
    }
    
    extractor = CrawlbaseTwitterExtractor(tweet_params)
    
    # Get current API usage
    usage = extractor.get_current_usage()
    logger.info(f"Crawlbase API usage: {usage['count']}/{usage['limit']} calls ({usage['percentage']:.1f}% of monthly limit)")
    
    # Extract tweets
    tweets = extractor.extract()
    
    if tweets and 'tweets' in tweets:
        logger.info(f"Successfully extracted {len(tweets['tweets'])} tweets from @{test_handle}")
        
        # Save to JSON file for inspection
        output_file = f"data/extracted/{test_handle}_tweets_manual_test.json"
        with open(output_file, 'w') as f:
            json.dump(tweets, f, indent=2)
        logger.info(f"Saved tweets to {output_file}")
        
        # If we have tweets, try analyzing sentiment
        if len(tweets['tweets']) > 0:
            logger.info("Testing sentiment analysis on tweets...")
            try:
                analyzer = SentimentAnalyzer()
                for i, tweet in enumerate(tweets['tweets'][:3]):  # Just analyze first 3 for test
                    text = tweet.get('text', '')
                    sentiment = analyzer.analyze_sentiment(text)
                    logger.info(f"Tweet {i+1}: {text[:50]}... Sentiment: {sentiment}")
            except Exception as e:
                logger.error(f"Error analyzing sentiment: {e}")
    else:
        logger.error(f"Failed to extract tweets from @{test_handle} or no tweets returned")

def test_search_extraction():
    """Test the Twitter search functionality"""
    # Load environment variables
    load_dotenv()
    
    # Get the Crawlbase API token
    api_token = os.getenv("CRAWLBASE_API_TOKEN")
    if not api_token:
        logger.error("CRAWLBASE_API_TOKEN not found in environment variables")
        return
    
    # Initialize the extractor for search
    search_params = {
        "api_token": api_token,
        "operation": "search",
        "query": "bitcoin price",
        "count": 3,  # Limited for testing
        "javascript_enabled": True,
        "min_request_interval": 2.0,
        "output_file": "data/extracted/bitcoin_search_test.json"
    }
    
    extractor = CrawlbaseTwitterExtractor(search_params)
    
    # Extract search results
    logger.info("Searching for 'bitcoin price'...")
    search_data = extractor.extract()
    
    # Print a summary of the search results
    if "tweets" in search_data:
        logger.info(f"Successfully found {len(search_data['tweets'])} tweets")
        for i, tweet in enumerate(search_data["tweets"][:2], 1):  # Show first 2 tweets
            logger.info(f"Tweet {i}:")
            logger.info(f"  Username: {tweet.get('username')}")
            logger.info(f"  ID: {tweet.get('id')}")
            logger.info(f"  Text: {tweet.get('text')[:100]}...")
    else:
        logger.warning("No search results were found")
        logger.info(f"Search data: {json.dumps(search_data, indent=2)}")
    
    return search_data

if __name__ == "__main__":
    main() 