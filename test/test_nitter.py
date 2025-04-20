#!/usr/bin/env python3
"""
Test script for extracting tweets using Nitter for more reliable chronological ordering.
"""

import os
import sys
import logging
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
            except Exception as e:
                logger.warning(f"Error parsing timestamp '{timestamp}': {e}")
                continue
    
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
    """Test the TwitterInfluencersExtractor using Nitter for improved chronological ordering"""
    # Load environment variables
    load_dotenv()
    
    # Use the JavaScript token 
    api_token = "GpsUqR7qNw5yasInk0N6AA"
    
    # Define test handles - use matt_willemsen as primary example
    handles = ["matt_willemsen"]
    
    # First test: Using regular Twitter
    logger.info("===== TESTING WITH REGULAR TWITTER =====")
    
    twitter_params = {
        "api_token": api_token,
        "handles": handles,
        "tweets_per_user": 10,
        "output_file": "data/extracted/test_twitter.json",
        "javascript_enabled": True,
        "use_x_domain": False,
        "use_dify_format": True,
        "use_nitter": False,  # Don't use Nitter for this test
        "min_request_interval": 2.0
    }
    
    twitter_extractor = TwitterInfluencersExtractor(twitter_params)
    twitter_data = twitter_extractor.extract()
    
    if not twitter_data or not twitter_data.get('tweets_by_handle'):
        logger.error("Failed to extract tweets using regular Twitter")
    else:
        for handle, tweets in twitter_data['tweets_by_handle'].items():
            logger.info(f"\n----- Regular Twitter @{handle} -----")
            logger.info(f"Extracted {len(tweets)} tweets")
            
            # Check if tweets are sorted
            timestamps = [t.get('timestamp') for t in tweets if t.get('timestamp')]
            if timestamps:
                is_sorted = all(timestamps[i] >= timestamps[i+1] for i in range(len(timestamps)-1))
                logger.info(f"Tweets are sorted by timestamp: {is_sorted}")
                
                # Most recent and oldest tweet
                logger.info(f"Most recent tweet: {format_timestamp(timestamps[0])}")
                logger.info(f"Oldest tweet: {format_timestamp(timestamps[-1])}")
                
                # Analyze dates
                analysis = analyze_tweet_dates(tweets)
                logger.info(f"Date distribution:\n{analysis}")
            
            # Sample of tweets
            logger.info("\nSample tweets:")
            for i, tweet in enumerate(tweets[:3]):
                timestamp = format_timestamp(tweet.get('timestamp', ''))
                content = tweet.get('content', 'No content')
                if len(content) > 100:
                    content = content[:97] + "..."
                logger.info(f"[{timestamp}] {content}")
    
    # Second test: Using Nitter
    logger.info("\n\n===== TESTING WITH NITTER =====")
    
    nitter_params = {
        "api_token": api_token,
        "handles": handles,
        "tweets_per_user": 10,
        "output_file": "data/extracted/test_nitter.json",
        "javascript_enabled": True,
        "use_x_domain": False,
        "use_dify_format": True,
        "use_nitter": True,  # Use Nitter for this test
        "nitter_instance": "nitter.io",  # Use nitter.io instead of nitter.net
        "debug_mode": True,  # Enable debug mode to see more info
        "min_request_interval": 2.0
    }
    
    nitter_extractor = TwitterInfluencersExtractor(nitter_params)
    nitter_data = nitter_extractor.extract()
    
    if not nitter_data or not nitter_data.get('tweets_by_handle'):
        logger.error("Failed to extract tweets using Nitter")
    else:
        for handle, tweets in nitter_data['tweets_by_handle'].items():
            logger.info(f"\n----- Nitter @{handle} -----")
            logger.info(f"Extracted {len(tweets)} tweets")
            
            # Check if tweets are sorted
            timestamps = [t.get('timestamp') for t in tweets if t.get('timestamp')]
            if timestamps:
                is_sorted = all(timestamps[i] >= timestamps[i+1] for i in range(len(timestamps)-1))
                logger.info(f"Tweets are sorted by timestamp: {is_sorted}")
                
                # Most recent and oldest tweet
                logger.info(f"Most recent tweet: {format_timestamp(timestamps[0])}")
                logger.info(f"Oldest tweet: {format_timestamp(timestamps[-1])}")
                
                # Analyze dates
                analysis = analyze_tweet_dates(tweets)
                logger.info(f"Date distribution:\n{analysis}")
            
            # Sample of tweets
            logger.info("\nSample tweets:")
            for i, tweet in enumerate(tweets[:3]):
                timestamp = format_timestamp(tweet.get('timestamp', ''))
                content = tweet.get('content', 'No content')
                if len(content) > 100:
                    content = content[:97] + "..."
                logger.info(f"[{timestamp}] {content}")
    
    # Compare results
    if twitter_data and nitter_data:
        twitter_tweets = []
        nitter_tweets = []
        
        for handle in handles:
            twitter_tweets.extend(twitter_data['tweets_by_handle'].get(handle, []))
            nitter_tweets.extend(nitter_data['tweets_by_handle'].get(handle, []))
        
        logger.info("\n\n===== COMPARISON =====")
        logger.info(f"Regular Twitter: {len(twitter_tweets)} tweets")
        logger.info(f"Nitter: {len(nitter_tweets)} tweets")
        
        # Compare most recent tweets
        twitter_timestamps = [t.get('timestamp') for t in twitter_tweets if t.get('timestamp')]
        nitter_timestamps = [t.get('timestamp') for t in nitter_tweets if t.get('timestamp')]
        
        if twitter_timestamps and nitter_timestamps:
            twitter_newest = format_timestamp(max(twitter_timestamps))
            nitter_newest = format_timestamp(max(nitter_timestamps))
            
            logger.info(f"Regular Twitter most recent tweet: {twitter_newest}")
            logger.info(f"Nitter most recent tweet: {nitter_newest}")
            
            # Try to parse and compare dates
            try:
                twitter_dt = datetime.datetime.fromisoformat(max(twitter_timestamps).replace('Z', '+00:00'))
                nitter_dt = datetime.datetime.fromisoformat(max(nitter_timestamps).replace('Z', '+00:00'))
                
                if twitter_dt > nitter_dt:
                    logger.info("Regular Twitter has more recent tweets")
                elif nitter_dt > twitter_dt:
                    logger.info("Nitter has more recent tweets")
                else:
                    logger.info("Both have the same most recent tweet timestamp")
                
                # Calculate difference in days
                diff_days = abs((twitter_dt - nitter_dt).days)
                logger.info(f"Difference in most recent tweets: {diff_days} days")
                
            except Exception as e:
                logger.warning(f"Error comparing timestamps: {e}")

if __name__ == "__main__":
    main() 