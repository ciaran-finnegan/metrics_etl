import os
import requests
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Add project root to Python path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def test_token(token, username, domain="twitter.com", use_javascript=True):
    """Test the Crawlbase API with the given token."""
    
    # Format the URL exactly as in the Dify.ai documentation
    url = f"https://{domain}/{username}"
    api_url = f"https://api.crawlbase.com/normal?token={token}&url={url}"
    
    if use_javascript:
        api_url += "&javascript=true"
    
    logger.info(f"Testing with token: {token[:4]}...{token[-4:]}")
    logger.info(f"Testing URL: {url}")
    logger.info(f"Full API URL: {api_url.replace(token, f'{token[:4]}...{token[-4:]}')}") 
    
    # Make the request
    try:
        logger.info("Sending request...")
        response = requests.get(api_url)
        status_code = response.status_code
        
        logger.info(f"Response status code: {status_code}")
        
        if status_code == 200:
            content_length = len(response.text)
            logger.info(f"Success! Content length: {content_length}")
            logger.info(f"First 500 characters of response: {response.text[:500]}")
        else:
            logger.error(f"Error response: {response.text[:500]}")
            
    except Exception as e:
        logger.error(f"Exception occurred: {str(e)}")
        
    logger.info("-" * 50)

def main():
    """Test the Crawlbase API using both tokens and domains."""
    
    # Tokens from Crawlbase
    js_token = "GpsUqR7qNw5yasInk0N6AA"  # JavaScript token
    normal_token = "Nact15Am3c7-Yf1Xq18sGA"  # Normal token
    
    # Twitter handle to test
    username = "karpathy"
    
    # Test Twitter.com domain with JavaScript token
    logger.info("TESTING TWITTER.COM WITH JAVASCRIPT TOKEN")
    test_token(js_token, username, domain="twitter.com", use_javascript=True)
    
    # Test X.com domain with JavaScript token
    logger.info("TESTING X.COM WITH JAVASCRIPT TOKEN")
    test_token(js_token, username, domain="x.com", use_javascript=True)
    
    # Test Twitter.com domain with normal token
    logger.info("TESTING TWITTER.COM WITH NORMAL TOKEN")
    test_token(normal_token, username, domain="twitter.com", use_javascript=True)
    
    # Test X.com domain with normal token
    logger.info("TESTING X.COM WITH NORMAL TOKEN")
    test_token(normal_token, username, domain="x.com", use_javascript=True)
    
    # Try a simpler URL format with path encoding
    url = f"https://twitter.com/{username}"
    encoded_url = requests.utils.quote(url, safe='')
    api_url = f"https://api.crawlbase.com/?token={js_token}&url={encoded_url}"
    
    logger.info("TESTING SIMPLIFIED FORMAT WITH PATH ENCODING")
    logger.info(f"Testing URL: {url}")
    logger.info(f"Encoded URL: {encoded_url}")
    logger.info(f"Full API URL: {api_url.replace(js_token, f'{js_token[:4]}...{js_token[-4:]}')}") 
    
    try:
        logger.info("Sending request...")
        response = requests.get(api_url)
        status_code = response.status_code
        
        logger.info(f"Response status code: {status_code}")
        
        if status_code == 200:
            content_length = len(response.text)
            logger.info(f"Success! Content length: {content_length}")
            logger.info(f"First 500 characters of response: {response.text[:500]}")
        else:
            logger.error(f"Error response: {response.text[:500]}")
            
    except Exception as e:
        logger.error(f"Exception occurred: {str(e)}")
        
    logger.info("-" * 50)

if __name__ == "__main__":
    main() 