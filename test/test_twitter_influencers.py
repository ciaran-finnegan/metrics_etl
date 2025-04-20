#!/usr/bin/env python3
"""
Test script for the TwitterInfluencersExtractor and TwitterSentimentTransformer
"""

import json
import logging
import sys
import os
from pathlib import Path

# Add parent directory to path to allow importing modules from the project
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from etl.extract.twitter_influencers_extractor import TwitterInfluencersExtractor
from etl.transform.twitter_sentiment_transformer import TwitterSentimentTransformer

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Load environment variables
    load_dotenv()
    
    # Use the JavaScript token from test_crawlbase_extractor.py
    api_token = "GpsUqR7qNw5yasInk0N6AA"  # JavaScript token from Crawlbase
    
    # Define test handles with matt_willemsen as the primary example
    handles = ["matt_willemsen", "docXBT", "raoulGMI"]
    
    # Define parameters for the extractor
    params = {
        "api_token": api_token,
        "handles": handles,
        "tweets_per_user": 5,
        "output_file": "data/extracted/test_influencers.json",
        "javascript_enabled": True,
        "use_x_domain": False,  # Use twitter.com instead of x.com
        "use_dify_format": True  # Use the format from Dify.ai documentation
    }
    
    # Create an instance of the influencer extractor
    logger.info("Initializing TwitterInfluencersExtractor...")
    extractor = TwitterInfluencersExtractor(params)
    
    # Extract tweets
    logger.info(f"Extracting tweets from {len(handles)} handles: {', '.join(handles)}")
    data = extractor.extract()
    
    # Check if we have extracted data
    if data:
        # Log information about the extracted data
        tweet_count = 0
        for handle, tweets in data.get('tweets_by_handle', {}).items():
            logger.info(f"@{handle}: {len(tweets)} tweets extracted")
            tweet_count += len(tweets)
        
        logger.info(f"Total tweets extracted: {tweet_count}")
        
        # Log information about profiles
        if 'profiles' in data:
            for handle, profile in data.get('profiles', {}).items():
                logger.info(f"Profile for @{handle}: {profile.get('name', 'Unknown')}, Followers: {profile.get('followers_count', 'Unknown')}")
    else:
        logger.error("No data extracted or extraction failed")
        return
    
    # Initialize the transformer
    logger.info("Initializing TwitterSentimentTransformer...")
    transformer = TwitterSentimentTransformer({
        "input_file": "data/extracted/test_influencers.json",
        "output_file": "data/transformed/test_sentiment.json"
    })
    
    # Transform the data
    logger.info("Transforming extracted tweets into sentiment metrics...")
    transformed_data = transformer.transform(data)
    
    # Log transformed data summary
    if transformed_data:
        logger.info(f"Successfully transformed data: sentiment score {transformed_data.get('value', 'N/A')}")
        
        # Print a sample of individual tweets with sentiment
        logger.info("Sample of transformed tweets with sentiment:")
        individual_tweets = transformed_data.get('metadata', {}).get('individual_tweets', [])
        
        # Show first 3 tweets (or all if less than 3)
        for i, tweet in enumerate(individual_tweets[:3]):
            logger.info(f"Tweet {i+1} by @{tweet.get('handle')}:")
            logger.info(f"  Content: {tweet.get('content')[:100]}..." if len(tweet.get('content', '')) > 100 else f"  Content: {tweet.get('content')}")
            logger.info(f"  Sentiment: compound={tweet.get('sentiment', {}).get('compound', 0):.3f}, "
                       f"positive={tweet.get('sentiment', {}).get('positive', 0):.3f}, "
                       f"negative={tweet.get('sentiment', {}).get('negative', 0):.3f}")
            logger.info(f"  Is crypto related: {tweet.get('is_crypto_related', False)}")
            logger.info(f"  Is macro related: {tweet.get('is_macro_related', False)}")
            logger.info("---")
    else:
        logger.error("Transformation failed or resulted in no data")

if __name__ == "__main__":
    main() 