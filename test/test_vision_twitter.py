#!/usr/bin/env python3
"""
Test script for the VisionTwitterExtractor.

This script tests the Twitter extraction functionality using OpenAI's vision
capabilities to guide Playwright browser automation.
"""

import os
import sys
import json
import logging
import argparse
import asyncio
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from etl.extract.vision_twitter_extractor import VisionTwitterExtractor

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def setup_output_dirs():
    """Create output directories for data and screenshots."""
    # Create test_output directory
    output_dir = os.path.join(project_root, "test_output")
    screenshots_dir = os.path.join(output_dir, "twitter_screenshots")
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(screenshots_dir, exist_ok=True)
    
    return {
        "output_dir": output_dir,
        "screenshots_dir": screenshots_dir
    }

async def main():
    """Run the Twitter extraction test."""
    parser = argparse.ArgumentParser(description="Test Twitter extraction using OpenAI vision")
    
    # Twitter credentials
    parser.add_argument("--username", type=str, help="Twitter username")
    parser.add_argument("--email", type=str, help="Twitter email")
    parser.add_argument("--password", type=str, help="Twitter password")
    parser.add_argument("--verification_code", type=str, help="Verification code (if needed)")
    
    # Extraction parameters
    parser.add_argument("--handles", type=str, nargs="+", help="List of Twitter handles to extract")
    parser.add_argument("--max_tweets", type=int, default=10, help="Maximum tweets per handle")
    parser.add_argument("--headless", action="store_true", default=False, help="Run in headless mode")
    parser.add_argument("--no-headless", dest="headless", action="store_false", help="Run with visible browser")
    parser.add_argument("--output_file", type=str, help="Path to output file")
    
    args = parser.parse_args()
    
    # Check Twitter credentials
    email = args.email or os.environ.get("TWITTER_EMAIL", "")
    username = args.username or os.environ.get("TWITTER_USERNAME", "")
    password = args.password or os.environ.get("TWITTER_PASSWORD", "")
    verification_code = args.verification_code or ""
    
    # Check if we have credentials
    if not username or not password or not email:
        logger.error("Twitter credentials not provided. Please provide them as arguments or environment variables")
        return
    
    # Use default handles if not provided
    handles = args.handles or ["elonmusk", "financialtimes", "WSJ"]
    
    # Set up output directories
    dirs = setup_output_dirs()
    
    # Determine output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = args.output_file or os.path.join(dirs["output_dir"], f"twitter_extraction_{timestamp}.json")
    
    # Create extractor
    logger.info(f"[INFO] Starting Vision-assisted Twitter extraction test for handles: {handles}")
    
    extractor = VisionTwitterExtractor({
        "email": email,
        "username": username,
        "password": password,
        "verification_code": verification_code,
        "handles": handles,
        "max_tweets_per_handle": args.max_tweets,
        "output_file": output_file,
        "screenshots_dir": dirs["screenshots_dir"],
        "headless": args.headless,
        "user_data_dir": os.path.join(dirs["output_dir"], "twitter_browser_data")
    })
    
    # Run extraction
    results = await extractor.extract()
    
    # Log results
    total_tweets = sum(len(tweets) for tweets in results.get("tweets_by_handle", {}).values())
    logger.info(f"[INFO] Extraction complete. Total tweets extracted: {total_tweets}")
    
    # Log per handle
    for handle, tweets in results.get("tweets_by_handle", {}).items():
        logger.info(f"  - @{handle}: {len(tweets)} tweets")
    
    logger.info(f"[INFO] Results saved to: {output_file}")
    logger.info("[INFO] Test completed successfully")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main()) 