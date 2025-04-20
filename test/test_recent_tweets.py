#!/usr/bin/env python3
"""
Test script for extracting the most recent tweets using Crawlbase API
"""

import os
import sys
import logging
import requests
import json
import datetime
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Add project root to Python path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def format_timestamp(timestamp_str):
    """Format a timestamp string to be more readable"""
    if not timestamp_str:
        return "Unknown date"
    
    try:
        dt = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        # Format as "1 Jan 2023 14:30"
        return dt.strftime("%d %b %Y %H:%M")
    except:
        return timestamp_str

def get_recent_tweets(username, token, count=10):
    """
    Get the most recent tweets from a user profile
    
    Args:
        username: Twitter handle without @ symbol
        token: Crawlbase API token
        count: Maximum number of tweets to extract
        
    Returns:
        List of tweet objects
    """
    logger.info(f"Attempting to get {count} recent tweets from @{username}...")
    
    # Use Twitter's actual URL structure
    url = f"https://twitter.com/{username}"
    
    # Encode the URL for Crawlbase
    encoded_url = requests.utils.quote(url, safe='')
    
    # Construct the Crawlbase API URL with several enhancements:
    # 1. JavaScript rendering enabled (required for Twitter's dynamic content)
    # 2. Use desktop user agent
    # 3. Set higher timeout (30 seconds) to allow the page to fully load
    # 4. Set cache to false to ensure we get the latest data
    api_url = f"https://api.crawlbase.com/?token={token}&url={encoded_url}&javascript=true&timeout=30000&cache=false"
    
    # Hide token in logs
    safe_url = api_url.replace(token, f"{token[:4]}...{token[-4:]}")
    logger.info(f"Making request to: {safe_url}")
    
    try:
        # Send the request
        logger.info("Sending request (this may take up to 30 seconds)...")
        response = requests.get(api_url)
        
        if response.status_code != 200:
            logger.error(f"Request failed with status code {response.status_code}")
            return []
        
        logger.info(f"Response received! Size: {len(response.text)} bytes")
        
        # Save the HTML for debugging
        output_dir = "debug_output"
        os.makedirs(output_dir, exist_ok=True)
        debug_file = os.path.join(output_dir, f"{username}_twitter_page.html")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(response.text)
        logger.info(f"Saved HTML to {debug_file}")
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all article elements (tweets)
        articles = soup.select('article[data-testid="tweet"]') or soup.select('article')
        logger.info(f"Found {len(articles)} potential tweets")
        
        # Extract tweet data
        tweets = []
        for i, article in enumerate(articles[:count]):
            # Initialize empty tweet
            tweet = {
                "user": username,
                "extract_time": datetime.datetime.now().isoformat()
            }
            
            # Extract tweet text
            text_elements = article.select('div[data-testid="tweetText"]')
            if text_elements:
                tweet["content"] = text_elements[0].get_text(separator=' ', strip=True)
            else:
                # Try alternative selectors
                lang_divs = article.select('div[lang]')
                tweet["content"] = ' '.join([div.get_text(strip=True) for div in lang_divs])
            
            # Extract tweet ID and timestamp
            time_elements = article.select('time')
            links = article.select('a[href*="/status/"]')
            
            if time_elements:
                timestamp = time_elements[0].get('datetime')
                tweet["timestamp"] = timestamp
                tweet["formatted_time"] = format_timestamp(timestamp)
            
            for link in links:
                href = link.get('href', '')
                if '/status/' in href:
                    tweet_id = href.split('/status/')[1].split('/')[0]
                    tweet["id"] = tweet_id
                    tweet["url"] = f"https://twitter.com/{username}/status/{tweet_id}"
                    break
            
            # Extract engagement metrics
            metrics = {}
            
            # Look for like count
            like_elements = article.select('div[data-testid="like"]')
            if like_elements:
                like_text = like_elements[0].get_text(strip=True)
                if like_text and like_text.lower() != 'like':
                    metrics["likes"] = like_text
            
            # Look for retweets
            retweet_elements = article.select('div[data-testid="retweet"]')
            if retweet_elements:
                retweet_text = retweet_elements[0].get_text(strip=True)
                if retweet_text and retweet_text.lower() != 'retweet':
                    metrics["retweets"] = retweet_text
            
            tweet["metrics"] = metrics
            
            # Add to tweets list
            tweets.append(tweet)
            
            # Log the tweet for debugging
            logger.info(f"Tweet {i+1}:")
            logger.info(f"  Date: {tweet.get('formatted_time', 'Unknown')}")
            logger.info(f"  Content: {tweet.get('content', 'No content')[:100]}..." if len(tweet.get('content', '')) > 100 else f"  Content: {tweet.get('content', 'No content')}")
            logger.info(f"  URL: {tweet.get('url', 'No URL')}")
            logger.info("  ---")
        
        # Calculate age of newest tweet
        if tweets and any(t.get('timestamp') for t in tweets):
            timestamps = [t.get('timestamp') for t in tweets if t.get('timestamp')]
            timestamps.sort()
            
            logger.info(f"Earliest tweet: {format_timestamp(timestamps[0])}")
            logger.info(f"Latest tweet: {format_timestamp(timestamps[-1])}")
            
            try:
                newest_dt = datetime.datetime.fromisoformat(timestamps[-1].replace('Z', '+00:00'))
                now = datetime.datetime.now(newest_dt.tzinfo)
                age = now - newest_dt
                logger.info(f"Newest tweet is {age.days} days and {age.seconds//3600} hours old")
            except Exception as e:
                logger.error(f"Error calculating tweet age: {e}")
        
        return tweets
    
    except Exception as e:
        logger.error(f"Error getting recent tweets: {e}")
        return []

def try_mobile_approach(username, token, count=10):
    """Try using the mobile version of Twitter which might show more recent tweets"""
    logger.info(f"Trying mobile approach for @{username}...")
    
    # Use mobile Twitter URL
    url = f"https://mobile.twitter.com/{username}"
    
    # Encode the URL
    encoded_url = requests.utils.quote(url, safe='')
    
    # Construct API URL with mobile user agent
    api_url = (f"https://api.crawlbase.com/?token={token}&url={encoded_url}&javascript=true&timeout=30000"
               f"&user_agent=Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1")
    
    # Make request
    try:
        logger.info("Sending mobile request...")
        response = requests.get(api_url)
        
        if response.status_code != 200:
            logger.error(f"Mobile request failed with status {response.status_code}")
            return []
        
        # Save response for debugging
        output_dir = "debug_output"
        os.makedirs(output_dir, exist_ok=True)
        mobile_file = os.path.join(output_dir, f"{username}_mobile.html")
        with open(mobile_file, "w", encoding="utf-8") as f:
            f.write(response.text)
        logger.info(f"Saved mobile HTML to {mobile_file}")
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try various mobile-specific selectors
        tweet_containers = (soup.select('div.tweet') or 
                            soup.select('div.timeline-tweet') or
                            soup.select('div[data-testid="tweet"]'))
        
        logger.info(f"Found {len(tweet_containers)} potential mobile tweets")
        
        # Extract basic info
        tweets = []
        for i, container in enumerate(tweet_containers[:count]):
            tweet = {"user": username, "source": "mobile"}
            
            # Extract content
            content_elements = (container.select('div.tweet-text') or 
                                container.select('p.tweet-text') or
                                container.select('div[data-testid="tweetText"]'))
            
            if content_elements:
                tweet["content"] = content_elements[0].get_text(strip=True)
            
            # Extract timestamp
            time_elements = container.select('time')
            if time_elements:
                tweet["timestamp"] = time_elements[0].get('datetime')
            
            tweets.append(tweet)
            
            # Log for debugging
            logger.info(f"Mobile Tweet {i+1}: {tweet.get('content', 'No content')[:50]}...")
        
        return tweets
        
    except Exception as e:
        logger.error(f"Error in mobile approach: {e}")
        return []

def main():
    # Load environment variables
    load_dotenv()
    
    # Crawlbase API token
    token = "GpsUqR7qNw5yasInk0N6AA"  # JavaScript token
    
    # Twitter handles to test
    handles = ["matt_willemsen", "karpathy", "naval"]
    
    for username in handles:
        logger.info(f"=" * 60)
        logger.info(f"TESTING WITH @{username}")
        logger.info(f"=" * 60)
        
        # Try regular approach
        tweets = get_recent_tweets(username, token, count=5)
        logger.info(f"Regular approach extracted {len(tweets)} tweets")
        
        # Try mobile approach
        mobile_tweets = try_mobile_approach(username, token, count=5)
        logger.info(f"Mobile approach extracted {len(mobile_tweets)} tweets")
        
        # Wait between users to avoid rate limiting
        if username != handles[-1]:
            logger.info("Waiting 5 seconds before the next user...")
            time.sleep(5)
        
        logger.info("\n")

if __name__ == "__main__":
    main() 