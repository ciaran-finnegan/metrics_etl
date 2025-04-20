import os
import sys
import logging
import requests
import json
import datetime
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

def crawl_twitter_profile(username, token, use_javascript=True):
    """
    Crawl a Twitter profile using the Crawlbase API.
    
    Args:
        username: Twitter username to crawl
        token: Crawlbase API token
        use_javascript: Whether to enable JavaScript rendering
        
    Returns:
        The response data
    """
    # Construct the URL
    url = f"https://twitter.com/{username}"
    
    # Encode the full URL
    encoded_url = requests.utils.quote(url, safe='')
    
    # Construct the Crawlbase API URL
    api_url = f"https://api.crawlbase.com/?token={token}&url={encoded_url}"
    
    # Add JavaScript rendering if enabled
    if use_javascript:
        api_url += "&javascript=true"
    
    # Hide token in logs for security
    safe_url = api_url.replace(token, f"{token[:4]}...{token[-4:]}")
    logger.info(f"Making request to: {safe_url}")
    
    try:
        logger.info("Sending request...")
        response = requests.get(api_url)
        status_code = response.status_code
        
        logger.info(f"Response status code: {status_code}")
        
        if status_code != 200:
            logger.error(f"API request failed with status {status_code}")
            return None
        
        logger.info(f"Success! Content length: {len(response.text)}")
        
        # Parse HTML with BeautifulSoup to extract data
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract some basic information
        title = soup.title.text if soup.title else "No title found"
        logger.info(f"Page title: {title}")
        
        # Find tweets (this will vary based on Twitter's current HTML structure)
        tweets = soup.select('article')
        logger.info(f"Found {len(tweets)} potential tweets")
        
        # Extract actual tweet content for a sample of tweets
        extracted_tweets = []
        for i, tweet_elem in enumerate(tweets[:3]):  # Only process the first 3 tweets
            tweet_content = ""
            
            # Try different approaches to get the text content
            # First approach: Look for data-testid="tweetText"
            text_elements = tweet_elem.select('div[data-testid="tweetText"]')
            if text_elements:
                for elem in text_elements:
                    tweet_content += elem.get_text(separator=' ', strip=True) + " "
            
            # Second approach: Try divs with lang attribute
            if not tweet_content.strip():
                lang_divs = tweet_elem.select('div[lang]')
                for elem in lang_divs:
                    tweet_content += elem.get_text(separator=' ', strip=True) + " "
            
            # Get tweet ID if possible
            tweet_id = None
            links = tweet_elem.select('a[href*="/status/"]')
            for link in links:
                href = link.get('href', '')
                if '/status/' in href:
                    tweet_id = href.split('/status/')[1].split('/')[0]
                    break
            
            # Extract timestamp
            timestamp = None
            time_elements = tweet_elem.select('time')
            if time_elements:
                timestamp = time_elements[0].get('datetime')
                logger.info(f"Tweet {i+1} timestamp: {timestamp} ({format_timestamp(timestamp)})")
            
            # Only add if we have actual content
            if tweet_content.strip():
                extracted_tweets.append({
                    "id": tweet_id,
                    "content": tweet_content.strip(),
                    "timestamp": timestamp,
                    "url": f"https://twitter.com/{username}/status/{tweet_id}" if tweet_id else None
                })
        
        # Output a sample of the HTML for debugging
        with open(f"twitter_{username}_sample.html", "w", encoding="utf-8") as f:
            f.write(response.text[:10000])  # First 10K characters
            
        logger.info(f"Saved sample HTML to twitter_{username}_sample.html")
        
        # Log extracted tweets
        if extracted_tweets:
            logger.info(f"Extracted {len(extracted_tweets)} sample tweets:")
            for i, tweet in enumerate(extracted_tweets):
                logger.info(f"  Tweet {i+1}:")
                logger.info(f"    Date: {format_timestamp(tweet.get('timestamp', ''))}")
                logger.info(f"    Content: {tweet['content'][:100]}..." if len(tweet['content']) > 100 else f"    Content: {tweet['content']}")
                logger.info(f"    URL: {tweet['url']}")
                logger.info("    ---")
            
            # Show earliest and latest tweets
            timestamps = [t.get('timestamp') for t in extracted_tweets if t.get('timestamp')]
            if timestamps:
                timestamps.sort()
                logger.info(f"Earliest tweet: {format_timestamp(timestamps[0])}")
                logger.info(f"Latest tweet: {format_timestamp(timestamps[-1])}")
                
                # Calculate age of newest tweet
                if timestamps[-1]:
                    try:
                        newest_dt = datetime.datetime.fromisoformat(timestamps[-1].replace('Z', '+00:00'))
                        now = datetime.datetime.now(newest_dt.tzinfo)
                        age = now - newest_dt
                        logger.info(f"Newest tweet is {age.days} days and {age.seconds//3600} hours old")
                    except Exception as e:
                        logger.error(f"Error calculating tweet age: {e}")
        
        return {
            "success": True,
            "status_code": status_code,
            "title": title,
            "potential_tweets_count": len(tweets),
            "sample_tweets": extracted_tweets
        }
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return None

def try_alternative_approach(username, token):
    """Try a more direct approach to get recent tweets"""
    logger.info(f"Trying alternative approach for @{username}'s recent tweets...")
    
    # Twitter's API endpoint URL pattern
    # We're targeting the user timeline API that Twitter's frontend uses
    url = f"https://twitter.com/i/api/graphql/some-hash/UserTweets?variables=%7B%22userId%22%3A%22{username}%22%2C%22count%22%3A20%7D"
    
    # For now, we'll just try to access the user's timeline more directly
    # This won't work directly since we don't have the correct hash and authentication,
    # but it demonstrates the approach we'd need to take
    
    # Instead, let's try to get the mobile version which might show more recent tweets
    mobile_url = f"https://mobile.twitter.com/{username}"
    
    # Encode the full URL
    encoded_url = requests.utils.quote(mobile_url, safe='')
    
    # Construct the Crawlbase API URL
    api_url = f"https://api.crawlbase.com/?token={token}&url={encoded_url}"
    
    # Add JavaScript rendering
    api_url += "&javascript=true&user_agent=Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    
    # Hide token in logs for security
    safe_url = api_url.replace(token, f"{token[:4]}...{token[-4:]}")
    logger.info(f"Making request to mobile Twitter via: {safe_url}")
    
    try:
        response = requests.get(api_url)
        
        if response.status_code == 200:
            logger.info(f"Mobile approach response received: {len(response.text)} bytes")
            
            # Save response for examination
            with open(f"twitter_mobile_{username}_sample.html", "w", encoding="utf-8") as f:
                f.write(response.text[:10000])
                
            logger.info(f"Saved mobile sample HTML to twitter_mobile_{username}_sample.html")
            
            # Try to find tweets in the mobile version
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Different selectors for mobile version
            tweet_elements = soup.select('div.tweet')
            logger.info(f"Found {len(tweet_elements)} potential mobile tweet elements")
            
            return True
        else:
            logger.error(f"Mobile approach failed with status {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error in mobile approach: {e}")
        return False

def main():
    # Load environment variables
    load_dotenv()
    
    # Tokens from Crawlbase
    js_token = "GpsUqR7qNw5yasInk0N6AA"  # JavaScript token
    
    # Test with a few different handles
    handles = ["karpathy", "matt_willemsen", "naval"]
    
    for username in handles:
        logger.info(f"=" * 50)
        logger.info(f"TESTING WITH @{username}")
        
        result = crawl_twitter_profile(username, js_token, use_javascript=True)
        
        if result:
            logger.info(f"Crawl successful for @{username}")
            # Only print a simplified version without the sample tweets
            simple_result = {k: v for k, v in result.items() if k != 'sample_tweets'}
            logger.info(json.dumps(simple_result, indent=2))
            
            # Try alternative approach
            try_alternative_approach(username, js_token)
        else:
            logger.error(f"Crawl failed for @{username}")
            
        logger.info(f"=" * 50)
        logger.info("\n")

if __name__ == "__main__":
    main() 