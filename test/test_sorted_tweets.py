#!/usr/bin/env python3
"""
Test script to verify that we properly extract and sort tweets by timestamp.
"""

import os
import sys
import logging
import json
import datetime
from pathlib import Path
from typing import Dict, Any, List

# Add parent directory to path to allow importing modules from the project
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from etl.extract.twitter_influencers_extractor import TwitterInfluencersExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def format_timestamp(timestamp_str):
    """Format a timestamp string to be more readable"""
    if not timestamp_str:
        return "Unknown date"
    
    try:
        dt = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        # Format as "12 Apr 2025 14:30"
        return dt.strftime("%d %b %Y %H:%M")
    except Exception as e:
        logger.warning(f"Error formatting timestamp '{timestamp_str}': {e}")
        return timestamp_str

def analyze_tweet_dates(tweets):
    """Analyze the distribution of tweet dates"""
    if not tweets:
        return "No tweets to analyze"
    
    # Convert timestamps to datetime objects
    dates = []
    for tweet in tweets:
        timestamp = tweet.get("timestamp")
        if timestamp:
            try:
                dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                dates.append(dt)
            except:
                pass
    
    if not dates:
        return "No valid dates found"
    
    # Sort dates newest to oldest
    dates.sort(reverse=True)
    
    # Calculate some statistics
    now = datetime.datetime.now(dates[0].tzinfo)
    newest_age = now - dates[0]
    oldest_age = now - dates[-1]
    
    # Group by year-month
    months = {}
    for dt in dates:
        key = dt.strftime("%Y-%m")
        months[key] = months.get(key, 0) + 1
    
    # Sort months
    sorted_months = sorted(months.items(), reverse=True)
    
    result = []
    result.append(f"Date range: {dates[-1].strftime('%d %b %Y')} to {dates[0].strftime('%d %b %Y')}")
    result.append(f"Newest tweet: {dates[0].strftime('%d %b %Y %H:%M')} ({newest_age.days} days old)")
    result.append(f"Oldest tweet: {dates[-1].strftime('%d %b %Y %H:%M')} ({oldest_age.days} days old)")
    result.append(f"Distribution by month:")
    
    for month, count in sorted_months:
        year, month_num = month.split("-")
        month_name = datetime.datetime(int(year), int(month_num), 1).strftime("%b %Y")
        result.append(f"  {month_name}: {count} tweets")
    
    return "\n".join(result)

def main():
    """Test the Twitter influencers extractor with proper sorting"""
    # Load environment variables
    load_dotenv()
    
    # Use the JavaScript token 
    api_token = "GpsUqR7qNw5yasInk0N6AA"
    
    # Define test handles
    handles = ["karpathy", "matt_willemsen", "naval"]
    
    # Define parameters for the extractor
    params = {
        "api_token": api_token,
        "handles": handles,
        "tweets_per_user": 15,  # Increase to get more tweets for better testing
        "output_file": "data/extracted/test_sorted.json",
        "javascript_enabled": True,
        "use_x_domain": False,
        "use_dify_format": True,
        "min_request_interval": 2.0
    }
    
    # Create an instance of the extractor
    logger.info("Initializing TwitterInfluencersExtractor...")
    extractor = TwitterInfluencersExtractor(params)
    
    # Extract tweets
    logger.info(f"Extracting tweets from {len(handles)} handles: {', '.join(handles)}")
    data = extractor.extract()
    
    # Check if we have extracted data
    if not data or not data.get('tweets_by_handle'):
        logger.error("No data extracted or extraction failed")
        return
    
    # Analyze tweet distribution for each handle
    for handle, tweets in data['tweets_by_handle'].items():
        if not tweets:
            logger.info(f"No tweets for @{handle}")
            continue
        
        logger.info(f"\n===== @{handle} =====")
        logger.info(f"Extracted {len(tweets)} tweets")
        
        # Check if tweets are sorted by timestamp
        timestamps = [
            tweet.get('timestamp') for tweet in tweets 
            if tweet.get('timestamp')
        ]
        
        if timestamps:
            is_sorted = all(timestamps[i] >= timestamps[i+1] for i in range(len(timestamps)-1))
            logger.info(f"Tweets are sorted by timestamp: {is_sorted}")
            
            # Show the most recent and oldest tweet dates
            newest = format_timestamp(timestamps[0])
            oldest = format_timestamp(timestamps[-1])
            logger.info(f"Most recent tweet: {newest}")
            logger.info(f"Oldest tweet: {oldest}")
            
            # Analyze tweet distribution
            analysis = analyze_tweet_dates(tweets)
            logger.info(f"Date distribution:\n{analysis}")
        
        # Print the first 3 tweets to verify content
        logger.info("\nSample tweets:")
        for i, tweet in enumerate(tweets[:3]):
            content = tweet.get('content', 'No content')
            if len(content) > 100:
                content = content[:97] + "..."
            
            timestamp = format_timestamp(tweet.get('timestamp', ''))
            logger.info(f"[{timestamp}] {content}")
        
        logger.info("\n")

if __name__ == "__main__":
    main() 