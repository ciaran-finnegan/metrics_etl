#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import logging
import argparse
import asyncio
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add the parent directory to the Python path
sys.path.append(str(Path(__file__).parent.parent))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

# Load environment variables from .env file
load_dotenv()

def setup_output_dirs():
    """Set up the output directories for data and screenshots"""
    output_dir = Path("test_output")
    output_dir.mkdir(exist_ok=True)
    
    screenshots_dir = output_dir / "twitter_screenshots"
    screenshots_dir.mkdir(exist_ok=True)
    
    return str(output_dir), str(screenshots_dir)

async def async_main():
    """Async version of the main function that runs the Twitter extraction"""
    parser = argparse.ArgumentParser(description='Test LLM-assisted Twitter extraction')
    parser.add_argument('--username', type=str, help='Twitter username or email')
    parser.add_argument('--password', type=str, help='Twitter password')
    parser.add_argument('--verification_code', type=str, help='Twitter verification code for 2FA')
    parser.add_argument('--handles', type=str, help='Comma-separated list of Twitter handles to extract from')
    parser.add_argument('--max_tweets', type=int, default=5, help='Maximum tweets to extract per handle')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    
    args = parser.parse_args()
    
    # Use environment variables if not provided as arguments
    username = args.username or os.getenv('TWITTER_USERNAME')
    password = args.password or os.getenv('TWITTER_PASSWORD')
    verification_code = args.verification_code or os.getenv('TWITTER_VERIFICATION_CODE')
    
    # Default handles if not provided
    if args.handles:
        handles = [h.strip() for h in args.handles.split(',')]
    else:
        handles = ['elonmusk', 'financialtimes', 'WSJ']
    
    # Set up output directories
    output_dir, screenshots_dir = setup_output_dirs()
    
    # Output file for extracted tweets
    output_file = os.path.join(output_dir, "twitter_extraction_results.json")
    
    # Import the extractor class
    from etl.extract.llm_assisted_twitter_extractor import LLMAssistedTwitterExtractor
    
    logging.info("Starting LLM-assisted Twitter extraction test")
    
    # Create extractor instance with parameters dictionary
    params = {
        'username': username,
        'password': password,
        'verification_code': verification_code,
        'output_file': output_file,
        'screenshots_dir': screenshots_dir,
        'max_tweets_per_handle': args.max_tweets,
        'headless': args.headless,
        'user_data_dir': os.path.join(os.path.expanduser('~'), '.twitter_browser_data'),
        'handles': handles  # Add handles directly to the params
    }
    
    extractor = LLMAssistedTwitterExtractor(params)
    
    logging.info(f"Extracting tweets from handles: {', '.join(handles)}")
    
    # Extract tweets - call without arguments since handles are in params
    result = await extractor.extract()
    
    # Get all tweets (flatten the result if it's organized by handle)
    if isinstance(result, dict) and 'tweets_by_handle' in result:
        tweets = []
        for handle_tweets in result['tweets_by_handle'].values():
            tweets.extend(handle_tweets)
    elif isinstance(result, dict) and 'tweets' in result:
        tweets = result['tweets']
    else:
        tweets = result if isinstance(result, list) else []
    
    # Output results
    logging.info(f"Extraction complete. Total tweets extracted: {len(tweets)}")
    
    # Save to output file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({"tweets": tweets}, f, indent=2)
    
    logging.info("Test completed successfully")

def main():
    """Main entry point that runs the async function"""
    asyncio.run(async_main())

if __name__ == "__main__":
    main() 