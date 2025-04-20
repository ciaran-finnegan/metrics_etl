#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

def setup_output_dirs():
    """Set up the output directories for data and screenshots"""
    output_dir = Path("test_output")
    output_dir.mkdir(exist_ok=True)
    
    screenshots_dir = output_dir / "twitter_screenshots"
    screenshots_dir.mkdir(exist_ok=True)
    
    return str(output_dir), str(screenshots_dir)

def main():
    """Test the crewAI Twitter extractor"""
    parser = argparse.ArgumentParser(description='Test crewAI Twitter extraction')
    parser.add_argument('--username', type=str, help='Twitter username or email')
    parser.add_argument('--password', type=str, help='Twitter password')
    parser.add_argument('--verification_code', type=str, help='Twitter verification code for 2FA')
    parser.add_argument('--handles', type=str, help='Comma-separated list of Twitter handles to extract from')
    parser.add_argument('--max_tweets', type=int, default=5, help='Maximum tweets to extract per handle')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--output_file', type=str, help='Output file path')
    
    args = parser.parse_args()
    
    # Use environment variables if not provided as arguments
    username = args.username or os.environ.get('TWITTER_USERNAME')
    password = args.password or os.environ.get('TWITTER_PASSWORD')
    verification_code = args.verification_code or os.environ.get('TWITTER_VERIFICATION_CODE')
    
    # Check if credentials exist
    if not username or not password:
        try:
            # Try to load from config file
            config_path = os.path.join("config", "twitter_login.json")
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    username = username or config.get('username')
                    password = password or config.get('password')
                    verification_code = verification_code or config.get('verification_code')
        except Exception as e:
            logging.error(f"Error loading config: {e}")
    
    if not username or not password:
        logging.error("Twitter credentials are required. Please provide them as arguments or environment variables.")
        return 1
    
    # Default handles if not provided
    if args.handles:
        handles = [h.strip() for h in args.handles.split(',')]
    else:
        handles = ['elonmusk', 'financialtimes', 'WSJ']
    
    # Set up output directories
    output_dir, screenshots_dir = setup_output_dirs()
    
    # Output file
    output_file = args.output_file or os.path.join(output_dir, "twitter_crew_results.json")
    
    # Import the extractor
    from etl.extract.crew_twitter_extractor import CrewTwitterExtractor
    
    logging.info("Starting crewAI Twitter extraction test")
    
    # Create extractor with parameters
    params = {
        'username': username,
        'password': password,
        'verification_code': verification_code,
        'handles': handles,
        'max_tweets_per_handle': args.max_tweets,
        'output_file': output_file,
        'screenshots_dir': screenshots_dir,
        'headless': args.headless,
        'user_data_dir': os.path.join(os.path.expanduser('~'), '.twitter_crew_data')
    }
    
    extractor = CrewTwitterExtractor(params)
    
    # Run extraction
    logging.info(f"Extracting tweets from handles: {', '.join(handles)}")
    result = extractor.extract()
    
    # Log results
    tweets_by_handle = result.get('tweets_by_handle', {})
    total_tweets = sum(len(tweets) for tweets in tweets_by_handle.values())
    
    logging.info(f"Extraction complete. Total tweets extracted: {total_tweets}")
    
    # Print summary for each handle
    for handle, tweets in tweets_by_handle.items():
        logging.info(f"  @{handle}: {len(tweets)} tweets")
    
    logging.info(f"Results saved to {output_file}")
    logging.info("Test completed successfully")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 