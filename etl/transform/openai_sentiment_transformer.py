import os
import re
import json
import base64
import time
import logging
import datetime
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import requests
from urllib.parse import urlparse

from utils.logging_config import logger, log_plugin_info
from utils.exceptions import TransformationError

class OpenAISentimentTransformer:
    """
    Analyzes tweets using OpenAI's API to extract sentiment, financial insights,
    and investment-related information. Capable of processing both text content
    and images in tweets for a comprehensive analysis.
    
    Returns structured data suitable for financial analysis and investment decision making.
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize the OpenAI sentiment transformer.
        
        Args:
            params: Configuration parameters including:
                - api_key: OpenAI API key
                - model: OpenAI model to use (default: gpt-4o)
                - input_file: Optional path to input file
                - output_file: Optional path to output file
                - analyze_images: Whether to analyze images in tweets (default: True)
                - screenshots_dir: Directory containing screenshots (if analyze_images is True)
                - include_full_response: Whether to include full OpenAI response (default: False)
                - max_age_hours: Maximum age of tweets to process in hours (default: 48)
        """
        self.params = params or {}
        
        # OpenAI configuration
        api_key = self.params.get('api_key') or os.getenv('OPENAI_API_KEY', '')
        
        # Clean the API key - remove any curly braces or whitespace
        if api_key:
            # Remove curly braces and surrounding whitespace if present
            api_key = api_key.strip()
            if api_key.startswith('{') and api_key.endswith('}'):
                api_key = api_key[1:-1].strip()
        
        self.api_key = api_key
        
        if not self.api_key:
            logger.warning("No OpenAI API key provided. Set OPENAI_API_KEY environment variable.")
        
        self.model = self.params.get('model', 'gpt-4o')
        
        # Check if model supports vision
        self.vision_capable = "gpt-4" in self.model and ("vision" in self.model or "o" in self.model)
        
        # Input/output configuration
        self.input_file = self.params.get('input_file')
        self.output_file = self.params.get('output_file')
        
        # Image analysis configuration
        self.analyze_images = self.params.get('analyze_images', True)
        self.screenshots_dir = self.params.get('screenshots_dir', 'screenshots')
        self.include_full_response = self.params.get('include_full_response', False)
        
        # Rate limiting to avoid API rate limits
        self.min_request_interval = self.params.get('min_request_interval', 1.0)
        self._last_request_time = 0
        
        # Maximum age of tweets to process (in hours)
        self.max_age_hours = self.params.get('max_age_hours', 48)
        
    def _rate_limit(self) -> None:
        """Apply rate limiting to avoid hitting OpenAI API limits"""
        current_time = time.time()
        elapsed = current_time - self._last_request_time
        
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            logger.debug(f"Rate limiting: Sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            
        self._last_request_time = time.time()
        
    def _find_screenshot(self, tweet_id: str, username: str) -> Optional[str]:
        """
        Find a screenshot file for a given tweet.
        
        Args:
            tweet_id: The Twitter tweet ID
            username: The Twitter username
            
        Returns:
            Path to the screenshot if found, None otherwise
        """
        if not self.screenshots_dir or not os.path.exists(self.screenshots_dir):
            return None
            
        # Check for screenshots with tweet ID
        screenshot_patterns = [
            f"{tweet_id}.png",
            f"{tweet_id}.jpg",
            f"{tweet_id}.jpeg",
            f"tweet_{tweet_id}.png",
            f"tweet_{tweet_id}.jpg",
            f"tweet_{tweet_id}.jpeg",
            f"{username}_{tweet_id}.png",
            f"{username}_{tweet_id}.jpg",
            f"{username}_{tweet_id}.jpeg"
        ]
        
        for pattern in screenshot_patterns:
            screenshot_path = os.path.join(self.screenshots_dir, pattern)
            if os.path.exists(screenshot_path):
                logger.debug(f"Found screenshot for tweet {tweet_id}: {screenshot_path}")
                return screenshot_path
                
        # If no specific screenshot found, check for user's timeline screenshots
        username_patterns = [
            f"{username}.png",
            f"{username}.jpg",
            f"{username}.jpeg",
            f"profile_{username}.png",
            f"profile_{username}.jpg",
            f"profile_{username}.jpeg"
        ]
        
        for pattern in username_patterns:
            screenshot_path = os.path.join(self.screenshots_dir, pattern)
            if os.path.exists(screenshot_path):
                logger.debug(f"Found user timeline screenshot for {username}: {screenshot_path}")
                return screenshot_path
                
        return None
        
    def _encode_image(self, image_path: str) -> str:
        """
        Encode an image file to base64.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Base64 encoded image string
        """
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
            
    def _download_image(self, image_url: str, save_path: Optional[str] = None) -> Optional[str]:
        """
        Download an image from a URL.
        
        Args:
            image_url: URL of the image
            save_path: Path to save the downloaded image (optional)
            
        Returns:
            Path to the downloaded image or None if download failed
        """
        try:
            # Extract filename from URL
            parsed_url = urlparse(image_url)
            filename = os.path.basename(parsed_url.path)
            
            # Generate save path if not provided
            if not save_path:
                if not os.path.exists(self.screenshots_dir):
                    os.makedirs(self.screenshots_dir, exist_ok=True)
                save_path = os.path.join(self.screenshots_dir, filename)
            
            # Download the image
            response = requests.get(image_url, stream=True, timeout=10)
            if response.status_code == 200:
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.debug(f"Downloaded image from {image_url} to {save_path}")
                return save_path
            else:
                logger.warning(f"Failed to download image from {image_url}: {response.status_code}")
                return None
        except Exception as e:
            logger.warning(f"Error downloading image from {image_url}: {str(e)}")
            return None
            
    def _prepare_prompt(self, tweet: Dict[str, Any], include_image: bool = False) -> Union[str, List[Dict[str, Any]]]:
        """
        Prepare the prompt for OpenAI analysis.
        
        Args:
            tweet: Tweet data
            include_image: Whether to include image data in the prompt
            
        Returns:
            Prompt string or messages list with image content if applicable
        """
        username = tweet.get('username', '')
        content = tweet.get('content', tweet.get('text', ''))
        date = tweet.get('timestamp', tweet.get('created_at', 'unknown date'))
        likes = tweet.get('likes', tweet.get('like_count', 0))
        retweets = tweet.get('retweets', tweet.get('retweet_count', 0))
        
        # Build prompt text
        prompt_text = f"""Analyze this tweet from a financial perspective and provide structured insights:

Tweet by @{username} on {date}:
"{content}"

Engagement: {likes} likes, {retweets} retweets

Provide a JSON response with the following structure:
{{
  "sentiment": "bullish/bearish/neutral",
  "sentiment_score": [0-100 scale where 0=extremely bearish, 50=neutral, 100=extremely bullish],
  "confidence": [0-100 scale where 0=no confidence, 100=extremely confident],
  "timeframe": "short_term/medium_term/long_term/unspecified",
  "asset_classes": ["crypto", "stocks", "bonds", "forex", etc.],
  "specific_assets": ["BTC", "ETH", "AAPL", "TSLA", etc.],
  "risk_assessment": "high/medium/low/unspecified",
  "key_themes": ["inflation", "interest rates", "regulations", etc.],
  "catalysts": ["event or news that might impact markets"],
  "metrics_mentioned": ["P/E ratio", "market cap", "trading volume", etc.],
  "contains_price_prediction": true/false,
  "technical_analysis": true/false,
  "investment_advice": true/false,
  "credibility_assessment": [0-100 scale reflecting quality of financial analysis],
  "summary": "Brief 1-2 sentence summary of the financial insight"
}}

IMPORTANT: Provide only the JSON response, with no additional text.
If the tweet doesn't contain financial/investment content, assign "neutral" sentiment, 50 sentiment score, and describe this in summary.
"""

        # For text-only analysis
        if not include_image or not self.vision_capable:
            return prompt_text
            
        # For vision-enabled analysis with image
        messages = [
            {"role": "system", "content": "You are a financial analyst who extracts investment insights from social media content."},
            {"role": "user", "content": [{"type": "text", "text": prompt_text}]}
        ]
        
        # Find and attach image if available
        image_path = None
        tweet_id = tweet.get('id', '')
        
        # First try to find a local screenshot
        if tweet_id and username:
            image_path = self._find_screenshot(tweet_id, username)
            
        # If no local screenshot but tweet has image URL, try to download it
        if not image_path and 'image_url' in tweet:
            image_url = tweet.get('image_url')
            if image_url:
                image_path = self._download_image(image_url)
                
        # Attach image to message if found
        if image_path:
            try:
                base64_image = self._encode_image(image_path)
                messages[1]["content"].append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
                })
                logger.debug(f"Added image to analysis prompt: {image_path}")
            except Exception as e:
                logger.warning(f"Failed to encode image {image_path}: {str(e)}")
                
        return messages
        
    def _call_openai_api(self, prompt: Union[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Call the OpenAI API.
        
        Args:
            prompt: Text prompt or messages list including image data
            
        Returns:
            Parsed JSON response from OpenAI
        """
        if not self.api_key:
            raise TransformationError("OpenAI API key is required")
            
        self._rate_limit()  # Apply rate limiting
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        # Prepare the request body
        if isinstance(prompt, str):
            # Text-only request
            body = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "You are a financial analyst who extracts investment insights from social media content."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "response_format": {"type": "json_object"}
            }
        else:
            # Vision request with image
            body = {
                "model": self.model,
                "messages": prompt,
                "temperature": 0.3,
                "response_format": {"type": "json_object"}
            }
            
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=body,
                timeout=60
            )
            
            if response.status_code != 200:
                error_msg = f"OpenAI API error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise TransformationError(error_msg)
                
            result = response.json()
            
            if "choices" not in result or not result["choices"]:
                raise TransformationError("Invalid response from OpenAI API")
                
            message_content = result["choices"][0]["message"]["content"]
            
            try:
                # Parse JSON response
                parsed_data = json.loads(message_content)
                return parsed_data
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON from OpenAI response: {message_content}")
                # Return a basic structure if parsing fails
                return {
                    "sentiment": "neutral",
                    "sentiment_score": 50,
                    "confidence": 0,
                    "summary": "Failed to parse structured data from response"
                }
                
        except requests.RequestException as e:
            error_msg = f"Error calling OpenAI API: {str(e)}"
            logger.error(error_msg)
            raise TransformationError(error_msg)
            
    def analyze_tweet(self, tweet: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a single tweet using OpenAI.
        
        Args:
            tweet: Tweet data
            
        Returns:
            Analysis results
        """
        # Determine if we should include image in analysis
        include_image = self.analyze_images and self.vision_capable
        
        # Prepare the prompt
        prompt = self._prepare_prompt(tweet, include_image)
        
        # Call OpenAI API
        try:
            logger.debug(f"Analyzing tweet from @{tweet.get('username', '')}")
            analysis = self._call_openai_api(prompt)
            
            # Enrich with metadata
            enriched_analysis = {
                "tweet_id": tweet.get("id", ""),
                "username": tweet.get("username", ""),
                "timestamp": tweet.get("timestamp", tweet.get("created_at", "")),
                "content": tweet.get("content", tweet.get("text", "")),
                "url": tweet.get("url", ""),
                "likes": tweet.get("likes", tweet.get("like_count", 0)),
                "retweets": tweet.get("retweets", tweet.get("retweet_count", 0)),
                "analyzed_at": datetime.datetime.now().isoformat(),
                "analysis": analysis
            }
            
            return enriched_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing tweet: {str(e)}")
            # Return basic structure with error
            return {
                "tweet_id": tweet.get("id", ""),
                "username": tweet.get("username", ""),
                "content": tweet.get("content", tweet.get("text", "")),
                "analyzed_at": datetime.datetime.now().isoformat(),
                "analysis": {
                    "sentiment": "neutral",
                    "sentiment_score": 50,
                    "confidence": 0,
                    "error": str(e),
                    "summary": "Error during analysis"
                }
            }
            
    def _is_recent_tweet(self, tweet: Dict[str, Any]) -> bool:
        """
        Check if a tweet is recent (within the max_age_hours timeframe).
        
        Args:
            tweet: Tweet data containing timestamp
            
        Returns:
            True if tweet is recent, False otherwise
        """
        timestamp = tweet.get("timestamp", tweet.get("created_at", ""))
        if not timestamp:
            logger.warning(f"Tweet has no timestamp, skipping: {tweet.get('id', '')}")
            return False
            
        try:
            # Handle different timestamp formats
            if isinstance(timestamp, str):
                # Try multiple formats
                for fmt in [
                    "%Y-%m-%dT%H:%M:%S%z",  # ISO format with timezone
                    "%Y-%m-%dT%H:%M:%S.%f%z",  # ISO format with microseconds and timezone
                    "%Y-%m-%d %H:%M:%S",  # Simple format without timezone
                    "%a %b %d %H:%M:%S %z %Y"  # Twitter API format
                ]:
                    try:
                        tweet_time = datetime.datetime.strptime(timestamp, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    # If no format matched, try to parse with dateutil if available
                    try:
                        from dateutil import parser
                        tweet_time = parser.parse(timestamp)
                    except (ImportError, ValueError):
                        logger.warning(f"Could not parse timestamp: {timestamp}")
                        return False
            else:
                # Assume it's already a datetime object
                tweet_time = timestamp
                
            # Ensure timezone awareness
            if tweet_time.tzinfo is None:
                tweet_time = tweet_time.replace(tzinfo=datetime.timezone.utc)
                
            # Get current time
            now = datetime.datetime.now(datetime.timezone.utc)
            
            # Calculate age in hours
            age_hours = (now - tweet_time).total_seconds() / 3600
            
            return age_hours <= self.max_age_hours
            
        except Exception as e:
            logger.warning(f"Error checking tweet timestamp: {e}")
            return False
            
    def transform(self, raw_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Transform raw tweet data using OpenAI for sentiment and financial analysis.
        Creates one signal/metric per tweet, filtering to only include recent tweets.
        
        Args:
            raw_data: Raw tweet data from the extractor or input file
            
        Returns:
            Transformed data with metrics for each recent tweet
        """
        try:
            # Try to load data from input file if no raw_data provided
            if not raw_data and self.input_file:
                try:
                    logger.info(f"Loading data from input file: {self.input_file}")
                    with open(self.input_file, 'r', encoding='utf-8') as f:
                        raw_data = json.load(f)
                except Exception as e:
                    logger.error(f"Error loading input file: {e}")
                    raw_data = {}
            
            # Handle data from various extractor formats
            tweets = []
            
            # Check if raw_data contains 'tweets' key directly
            if raw_data and 'tweets' in raw_data and isinstance(raw_data['tweets'], list):
                tweets = raw_data['tweets']
                
            # Check if tweets_by_handle exists directly
            elif raw_data and 'tweets_by_handle' in raw_data:
                tweets_by_handle = raw_data.get('tweets_by_handle', {})
                
                # Collect all tweets in flat list
                for handle, handle_tweets in tweets_by_handle.items():
                    tweets.extend(handle_tweets)
            
            # If no proper data found, return empty result with warning
            if not tweets:
                logger.warning("No tweets found in raw data or empty input file")
                return {
                    "metrics": [],
                    "metadata": {
                        "source": "openai_sentiment",
                        "error": "No tweets found for analysis",
                        "handles_analyzed": [],
                        "total_tweets_analyzed": 0,
                        "last_updated_at": datetime.datetime.now().isoformat()
                    }
                }
            
            # Filter to recent tweets only
            recent_tweets = [tweet for tweet in tweets if self._is_recent_tweet(tweet)]
            
            if not recent_tweets:
                logger.warning(f"No recent tweets found (within {self.max_age_hours} hours)")
                return {
                    "metrics": [],
                    "metadata": {
                        "source": "openai_sentiment",
                        "error": f"No tweets found within the last {self.max_age_hours} hours",
                        "handles_analyzed": [],
                        "total_tweets_analyzed": 0,
                        "last_updated_at": datetime.datetime.now().isoformat()
                    }
                }
                
            logger.info(f"Processing {len(recent_tweets)} recent tweets (out of {len(tweets)} total)")
            
            # Process each tweet and create individual metrics
            metrics = []
            handles_analyzed = set()
            
            for tweet in recent_tweets:
                # Analyze tweet
                analysis_result = self.analyze_tweet(tweet)
                
                # Extract data
                tweet_id = analysis_result.get("tweet_id", "")
                username = analysis_result.get("username", "")
                timestamp = analysis_result.get("timestamp", "")
                content = analysis_result.get("content", "")
                analysis = analysis_result.get("analysis", {})
                
                # Determine handle/username for analysis count
                handle_for_count = username
                if not handle_for_count:
                    # Try to fall back to author field used by VisionTwitterExtractor
                    author_info = tweet.get("author") if isinstance(tweet, dict) else {}
                    if isinstance(author_info, dict):
                        handle_for_count = author_info.get("handle") or author_info.get("username") or ""

                if handle_for_count:
                    handles_analyzed.add(handle_for_count)
                
                # Convert timestamp to date
                try:
                    # Try to parse timestamp
                    if isinstance(timestamp, str):
                        # Try multiple formats
                        for fmt in [
                            "%Y-%m-%dT%H:%M:%S%z",  # ISO format with timezone
                            "%Y-%m-%dT%H:%M:%S.%f%z",  # ISO format with microseconds and timezone
                            "%Y-%m-%d %H:%M:%S",  # Simple format without timezone
                            "%a %b %d %H:%M:%S %z %Y"  # Twitter API format
                        ]:
                            try:
                                tweet_date = datetime.datetime.strptime(timestamp, fmt).strftime("%Y-%m-%d")
                                break
                            except ValueError:
                                continue
                        else:
                            # If no format matched, use current date
                            tweet_date = datetime.datetime.now().strftime("%Y-%m-%d")
                    else:
                        # Assume it's already a datetime object
                        tweet_date = timestamp.strftime("%Y-%m-%d") if hasattr(timestamp, "strftime") else datetime.datetime.now().strftime("%Y-%m-%d")
                except Exception:
                    tweet_date = datetime.datetime.now().strftime("%Y-%m-%d")
                
                # Extract sentiment score
                sentiment_score = analysis.get("sentiment_score", 50)
                
                # Image / link presence flags for downstream filtering
                has_image = bool(tweet.get("image_urls") or tweet.get("image_url"))
                # Simple link detection: external_links list or http substring in content
                has_link = bool(tweet.get("external_links") or ("http" in content if isinstance(content, str) else False))
                
                # Create metric for this tweet
                metric = {
                    "date": tweet_date,
                    "value": sentiment_score,
                    "unit": "sentiment score (0-100)",
                    "units": "sentiment score (0-100)",  # Add units field for compatibility
                    "metadata": {
                        "source": "openai_sentiment",
                        "tweet_id": tweet_id,
                        "username": username,
                        "content": content,
                        "timestamp": timestamp,
                        "sentiment": analysis.get("sentiment", "neutral"),
                        "confidence": analysis.get("confidence", 0),
                        "timeframe": analysis.get("timeframe", "unspecified"),
                        "asset_classes": analysis.get("asset_classes", []),
                        "specific_assets": analysis.get("specific_assets", []),
                        "key_themes": analysis.get("key_themes", []),
                        "summary": analysis.get("summary", ""),
                        "has_image": has_image,
                        "has_link": has_link,
                        "analyzed_at": analysis_result.get("analyzed_at", datetime.datetime.now().isoformat())
                    }
                }
                
                metrics.append(metric)
            
            # Final result
            result = {
                "metrics": metrics,
                "metadata": {
                    "source": "openai_sentiment",
                    "handles_analyzed": list(handles_analyzed),
                    "total_tweets_analyzed": len(metrics),
                    "max_age_hours": self.max_age_hours,
                    "last_updated_at": datetime.datetime.now().isoformat()
                }
            }
            
            # Log result
            log_plugin_info('transform', 'OpenAISentimentTransformer', 
                           f"Analyzed {len(metrics)} recent tweets from {len(handles_analyzed)} handles")
            
            # Save to output file if specified
            if self.output_file:
                self.save(result)
            
            return result
            
        except Exception as e:
            logger.error(f"OpenAI sentiment transformation failed: {e}")
            raise TransformationError(f"OpenAI sentiment transformation failed: {e}")
            
    def save(self, data: Dict[str, Any]) -> None:
        """
        Save the transformed data to the output file.
        
        Args:
            data: Transformed data to save
        """
        if not self.output_file:
            logger.warning("No output file specified, not saving data")
            return
            
        try:
            # Ensure directory exists
            output_dir = os.path.dirname(self.output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Saved transformed data to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving transformed data: {e}")
            raise TransformationError(f"Error saving transformed data: {e}") 