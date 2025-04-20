"""
Twitter transformer for processing tweets collected via Crawlbase or other extractors.
"""
import json
import datetime
from typing import Dict, Any, List
import statistics
import logging
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Configure logging
logger = logging.getLogger(__name__)

class TwitterTransformer:
    """
    Transforms raw Twitter data into a structured format.
    Can extract sentiment if enabled.
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize the Twitter transformer.
        
        Args:
            params: Parameters dictionary including:
                extract_sentiment: Whether to perform sentiment analysis (default: False)
        """
        self.params = params or {}
        self.extract_sentiment = self.params.get("extract_sentiment", False)
        
        # Initialize sentiment analyzer if sentiment extraction is enabled
        if self.extract_sentiment:
            try:
                import nltk
                # Ensure NLTK resources are downloaded
                try:
                    nltk.data.find('sentiment/vader_lexicon.zip')
                except LookupError:
                    logger.info("Downloading NLTK vader lexicon for sentiment analysis")
                    nltk.download('vader_lexicon', quiet=True)
                
                self.sia = SentimentIntensityAnalyzer()
                
                # Add custom crypto and finance terms to lexicon
                self.sia.lexicon.update({
                    'hodl': 2.0,           # Very positive
                    'moon': 3.0,           # Extremely positive
                    'bullish': 2.5,        # Very positive
                    'bearish': -2.5,       # Very negative
                    'fud': -2.0,           # Negative
                    'pump': 1.5,           # Positive
                    'dump': -1.5,          # Negative
                    'crash': -3.0,         # Extremely negative
                    'scam': -3.0,          # Extremely negative
                    'rally': 2.0,          # Very positive
                    'correction': -1.0,    # Slightly negative
                })
                logger.info("Sentiment analysis initialized successfully")
            except ImportError:
                logger.warning("NLTK package not found. Sentiment analysis disabled.")
                self.extract_sentiment = False
    
    def _analyze_sentiment(self, text: str) -> float:
        """
        Analyze sentiment of a tweet text.
        
        Args:
            text: The tweet text
            
        Returns:
            A sentiment score between -1 (very negative) and 1 (very positive)
        """
        if not self.extract_sentiment or not hasattr(self, 'sia'):
            return 0.0
            
        sentiment = self.sia.polarity_scores(text)
        return sentiment['compound']
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Twitter data into structured format.
        
        Args:
            raw_data: Raw data from a Twitter extractor
            
        Returns:
            Transformed data suitable for loading
        """
        logger.info("Transforming Twitter data")
        
        try:
            # Check operation type from extractor
            operation = raw_data.get("operation", "unknown")
            transformed_data = []
            
            if operation == "user_tweets":
                username = raw_data.get("username", "unknown")
                tweets = raw_data.get("tweets", [])
                
                logger.info(f"Processing {len(tweets)} tweets from user @{username}")
                
                for tweet in tweets:
                    tweet_id = tweet.get("id")
                    text = tweet.get("text", "")
                    created_at = tweet.get("created_at")
                    
                    # Skip tweets without an ID
                    if not tweet_id:
                        continue
                    
                    # Calculate sentiment if enabled
                    sentiment = self._analyze_sentiment(text) if self.extract_sentiment else 0.0
                    
                    transformed_data.append({
                        "tweet_id": tweet_id,
                        "username": username,
                        "text": text,
                        "created_at": created_at,
                        "sentiment": sentiment,
                        "collected_at": datetime.datetime.now().isoformat()
                    })
            
            elif operation == "search":
                query = raw_data.get("query", "unknown")
                tweets = raw_data.get("tweets", [])
                
                logger.info(f"Processing {len(tweets)} tweets from search '{query}'")
                
                for tweet in tweets:
                    tweet_id = tweet.get("id")
                    username = tweet.get("username", "unknown")
                    text = tweet.get("text", "")
                    created_at = tweet.get("created_at")
                    
                    # Skip tweets without an ID
                    if not tweet_id:
                        continue
                    
                    # Calculate sentiment if enabled
                    sentiment = self._analyze_sentiment(text) if self.extract_sentiment else 0.0
                    
                    transformed_data.append({
                        "tweet_id": tweet_id,
                        "username": username,
                        "text": text,
                        "created_at": created_at,
                        "sentiment": sentiment,
                        "search_query": query,
                        "collected_at": datetime.datetime.now().isoformat()
                    })
            
            else:
                logger.warning(f"Unknown operation type: {operation}")
            
            # Calculate average sentiment if we have tweets
            avg_sentiment = 0.0
            if transformed_data and self.extract_sentiment:
                sentiments = [t.get("sentiment", 0) for t in transformed_data]
                avg_sentiment = statistics.mean(sentiments)
                logger.info(f"Average sentiment score: {avg_sentiment:.2f}")
            
            return {
                "tweets": transformed_data,
                "count": len(transformed_data),
                "avg_sentiment": avg_sentiment,
                "operation": operation,
                "timestamp": datetime.datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error transforming Twitter data: {str(e)}")
            # Return minimal valid data
            return {
                "tweets": [],
                "count": 0,
                "error": str(e),
                "timestamp": datetime.datetime.now().isoformat()
            } 