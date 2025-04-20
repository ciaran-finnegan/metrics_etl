"""
Test script for DirectNitterExtractor implementation.
"""
import os
import sys
import logging
import json
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extract.direct_nitter_extractor import DirectNitterExtractor

def main():
    """
    Test the DirectNitterExtractor implementation.
    """
    logger.info("Testing DirectNitterExtractor")
    
    # Test extracting tweets for a specific user
    test_user = "matt_willemsen"
    tweet_count = 5
    
    logger.info(f"Testing tweet extraction for @{test_user} (count: {tweet_count})")
    
    # Initialize the extractor
    params = {
        "min_request_interval": 1.0,
        "max_request_interval": 2.0,
        "max_retries": 3,
        "include_replies": False,
        "include_retweets": False,
        "timeout": 15
    }
    
    extractor = DirectNitterExtractor(params)
    
    # Get tweets
    try:
        tweets = extractor.get_user_tweets(test_user, tweet_count)
        logger.info(f"Successfully extracted {len(tweets)} tweets")
        
        if tweets:
            logger.info(f"First tweet content: {tweets[0].get('text', '')[:100]}...")
            
            # Output to file for inspection
            output_path = "data/output/direct_nitter_test.json"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(tweets, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved extracted tweets to {output_path}")
        else:
            logger.warning("No tweets were extracted")
            
        # Get profile
        try:
            profile = extractor.get_user_profile(test_user)
            logger.info(f"Profile extraction successful: {bool(profile)}")
            if profile:
                logger.info(f"Username: {profile.get('username')}")
                logger.info(f"Display name: {profile.get('display_name')}")
                logger.info(f"Followers: {profile.get('followers_count')}")
                logger.info(f"Bio: {profile.get('bio', '')[:100]}...")
        except Exception as e:
            logger.error(f"Error extracting profile: {str(e)}")
            
    except Exception as e:
        logger.error(f"Error extracting tweets: {str(e)}")
        
    logger.info("Test completed")

if __name__ == "__main__":
    main() 