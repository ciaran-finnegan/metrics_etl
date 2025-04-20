import re
from typing import Dict, Any, List
from utils.logging_config import logger, log_plugin_info
from utils.exceptions import TransformationError
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import datetime
import statistics
import json

# Ensure NLTK resources are downloaded (would need to be done at setup)
# nltk.download('vader_lexicon')

class TwitterSentimentTransformer:
    """
    Analyzes sentiment of tweets from financial influencers.
    Produces a market sentiment indicator based on the collective sentiment.
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        self.params = params or {}
        self.crypto_keywords = self.params.get('crypto_keywords', [
            'bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'blockchain', 
            'defi', 'nft', 'altcoin', 'token', 'web3', 'mining'
        ])
        self.macro_keywords = self.params.get('macro_keywords', [
            'inflation', 'fed', 'interest rate', 'economy', 'gdp', 'recession',
            'market', 'stock', 'bond', 'dollar', 'treasury', 'debt', 'currency'
        ])
        
        # Configuration for resilience
        self.min_tweets_required = self.params.get('min_tweets_required', 5)
        self.input_file = self.params.get('input_file', None)
        self.output_file = self.params.get('output_file', None)
        
        # Initialize sentiment analyzer
        self.sia = SentimentIntensityAnalyzer()
        
        # Add custom crypto and finance terms to lexicon with associated sentiments
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
            'bubble': -1.5,        # Negative
            'rally': 2.0,          # Very positive
            'correction': -1.0,    # Slightly negative
            'accumulate': 1.5,     # Positive
            'adoption': 2.0,       # Very positive
            'inflation': -1.0,     # Slightly negative
            'recession': -2.5,     # Very negative
            'interest rate': -0.5, # Slightly negative
            'debt': -1.0,          # Slightly negative
        })
    
    def _contains_keywords(self, text: str, keywords: List[str]) -> bool:
        """Check if text contains any of the keywords (case insensitive)"""
        text_lower = text.lower()
        return any(keyword.lower() in text_lower for keyword in keywords)
    
    def _clean_tweet_text(self, text: str) -> str:
        """Clean tweet text for sentiment analysis"""
        # Remove URLs
        text = re.sub(r'http\S+', '', text)
        # Remove user mentions
        text = re.sub(r'@\w+', '', text)
        # Remove hashtags symbol (but keep the text)
        text = re.sub(r'#(\w+)', r'\1', text)
        # Remove special characters
        text = re.sub(r'[^\w\s]', '', text)
        # Remove extra spaces
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def _analyze_sentiment(self, text: str) -> Dict[str, float]:
        """Analyze sentiment of a given text using VADER"""
        clean_text = self._clean_tweet_text(text)
        return self.sia.polarity_scores(clean_text)
    
    def _analyze_tweet_batch(self, tweets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze sentiment metrics for a batch of tweets"""
        if not tweets:
            return {
                'sentiment_compound_avg': 0,
                'sentiment_positive_avg': 0,
                'sentiment_negative_avg': 0,
                'sentiment_neutral_avg': 0,
                'crypto_tweets_count': 0,
                'macro_tweets_count': 0,
                'crypto_sentiment_avg': 0,
                'macro_sentiment_avg': 0,
                'tweets_analyzed': 0
            }
        
        compound_scores = []
        positive_scores = []
        negative_scores = []
        neutral_scores = []
        
        crypto_scores = []
        macro_scores = []
        
        for tweet in tweets:
            text = tweet.get('content', '')
            sentiment = self._analyze_sentiment(text)
            
            compound_scores.append(sentiment['compound'])
            positive_scores.append(sentiment['pos'])
            negative_scores.append(sentiment['neg'])
            neutral_scores.append(sentiment['neu'])
            
            # Track crypto-specific sentiment
            if self._contains_keywords(text, self.crypto_keywords):
                crypto_scores.append(sentiment['compound'])
            
            # Track macro-specific sentiment
            if self._contains_keywords(text, self.macro_keywords):
                macro_scores.append(sentiment['compound'])
        
        # Calculate averages
        return {
            'sentiment_compound_avg': statistics.mean(compound_scores) if compound_scores else 0,
            'sentiment_positive_avg': statistics.mean(positive_scores) if positive_scores else 0,
            'sentiment_negative_avg': statistics.mean(negative_scores) if negative_scores else 0,
            'sentiment_neutral_avg': statistics.mean(neutral_scores) if neutral_scores else 0,
            'crypto_tweets_count': len(crypto_scores),
            'macro_tweets_count': len(macro_scores),
            'crypto_sentiment_avg': statistics.mean(crypto_scores) if crypto_scores else 0,
            'macro_sentiment_avg': statistics.mean(macro_scores) if macro_scores else 0,
            'tweets_analyzed': len(tweets)
        }
    
    def transform(self, raw_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Transform raw Twitter data into sentiment indicators for market analysis.
        
        The output includes:
        - Overall sentiment score (-1 to +1) across all influencers
        - Crypto-specific sentiment
        - Macro-economic sentiment
        - Engagement-weighted sentiment
        - Individual tweet content with sentiment scores
        
        Args:
            raw_data: Raw Twitter data from extractor or input file
            
        Returns:
            Transformed data with sentiment analysis
        """
        try:
            # Try to load data from input file if no raw_data provided
            if not raw_data and self.input_file:
                try:
                    logger.info(f"Loading data from input file: {self.input_file}")
                    with open(self.input_file, 'r', encoding='utf-8') as f:
                        raw_data = json.loads(f.read())
                except Exception as e:
                    logger.error(f"Error loading input file: {e}")
                    raw_data = {}
            
            # Handle data from various extractor formats
            tweets = []
            tweets_by_handle = {}
            
            # Check if raw_data contains 'tweets' key directly (DirectNitterTwitterExtractor format)
            if raw_data and 'tweets' in raw_data and isinstance(raw_data['tweets'], list):
                tweets = raw_data['tweets']
                
                # Group tweets by handle
                for tweet in tweets:
                    handle = tweet.get('username', '')
                    if handle:
                        if handle not in tweets_by_handle:
                            tweets_by_handle[handle] = []
                        
                        # Ensure content field exists (mapping from text if needed)
                        if 'content' not in tweet and 'text' in tweet:
                            tweet['content'] = tweet['text']
                            
                        tweets_by_handle[handle].append(tweet)
                
            # Check if tweets_by_handle exists directly
            elif raw_data and 'tweets_by_handle' in raw_data:
                tweets_by_handle = raw_data.get('tweets_by_handle', {})
                
                # Collect all tweets in flat list
                for handle, handle_tweets in tweets_by_handle.items():
                    tweets.extend(handle_tweets)
            
            # If no proper data found, raise error
            if not tweets_by_handle and not tweets:
                logger.warning("No tweets found in raw data or empty input file")
                # Instead of raising an error, return empty result with warning
                return {
                    "date": datetime.datetime.now().strftime("%Y-%m-%d"),
                    "value": 50,  # Neutral value
                    "unit": "sentiment score (0-100)",
                    "metadata": {
                        "source": "twitter_sentiment",
                        "error": "No tweets found for analysis",
                        "handles_analyzed": [],
                        "total_tweets_analyzed": 0,
                        "sentiment_category": "neutral",
                        "last_updated_at": datetime.datetime.now().isoformat()
                    }
                }
            
            # Individual handle metrics
            handle_metrics = {}
            all_tweets = []
            
            # Store individual tweet sentiment scores
            tweets_with_sentiment = []
            
            for handle, handle_tweets in tweets_by_handle.items():
                if handle_tweets:
                    handle_tweet_data = []
                    
                    # Process individual tweets and store their sentiment data
                    for tweet in handle_tweets:
                        # Handle different tweet formats
                        text = tweet.get('content', tweet.get('text', ''))
                        sentiment = self._analyze_sentiment(text)
                        
                        # Create enriched tweet object with sentiment
                        tweet_with_sentiment = {
                            'tweet_id': tweet.get('id', ''),
                            'handle': handle,
                            'text': text,  # Store as 'text' for consistency
                            'timestamp': tweet.get('timestamp', tweet.get('created_at', '')),
                            'sentiment': sentiment['compound'],
                            'sentiment_confidence': max(sentiment['pos'], sentiment['neg']),
                            'sentiment_details': {
                                'compound': sentiment['compound'],
                                'positive': sentiment['pos'],
                                'negative': sentiment['neg'],
                                'neutral': sentiment['neu']
                            },
                            'is_crypto_related': self._contains_keywords(text, self.crypto_keywords),
                            'is_macro_related': self._contains_keywords(text, self.macro_keywords),
                            'url': tweet.get('url', ''),
                            'likes': tweet.get('likes', tweet.get('stats', {}).get('likes', 0)),
                            'retweets': tweet.get('retweets', tweet.get('stats', {}).get('retweets', 0))
                        }
                        
                        handle_tweet_data.append(tweet_with_sentiment)
                        tweets_with_sentiment.append(tweet_with_sentiment)
                    
                    # Calculate aggregate metrics for the handle
                    handle_metrics[handle] = self._analyze_tweet_batch(handle_tweets)
                    # Store individual tweet data with sentiment
                    handle_metrics[handle]['tweets'] = handle_tweet_data
                    
                    # Add tweets to overall collection
                    all_tweets.extend(handle_tweets)
            
            # Check if we have enough tweets for meaningful analysis
            total_tweets = len(all_tweets)
            logger.info(f"Found {total_tweets} tweets for sentiment analysis")
            
            if total_tweets < self.min_tweets_required:
                logger.warning(f"Not enough tweets for reliable analysis (got {total_tweets}, need at least {self.min_tweets_required})")
                # Continue with analysis, but add warning to metadata
                analysis_warning = f"Limited data: Only {total_tweets} tweets available for analysis (minimum recommended: {self.min_tweets_required})"
            else:
                analysis_warning = None
            
            # Overall metrics (all tweets combined)
            overall_metrics = self._analyze_tweet_batch(all_tweets)
            
            # Extract current date
            dt_object = datetime.datetime.now()
            date_str = dt_object.strftime("%Y-%m-%d")
            
            # Map sentiment to a market sentiment indicator (0-100 scale)
            # where 50 is neutral, >50 is bullish, <50 is bearish
            crypto_sentiment_scaled = 50 + (overall_metrics['crypto_sentiment_avg'] * 50)
            macro_sentiment_scaled = 50 + (overall_metrics['macro_sentiment_avg'] * 50)
            overall_sentiment_scaled = 50 + (overall_metrics['sentiment_compound_avg'] * 50)
            
            # Determine market sentiment category
            sentiment_category = "neutral"
            if overall_sentiment_scaled >= 70:
                sentiment_category = "very bullish"
            elif overall_sentiment_scaled >= 55:
                sentiment_category = "bullish"
            elif overall_sentiment_scaled <= 30:
                sentiment_category = "very bearish"
            elif overall_sentiment_scaled <= 45:
                sentiment_category = "bearish"
            
            log_plugin_info('transform', 'TwitterSentimentTransformer', 
                           f"Analyzed {len(all_tweets)} tweets, overall sentiment: {sentiment_category} "
                           f"({overall_sentiment_scaled:.1f}/100)")
            
            result = {
                "date": date_str,
                "value": overall_sentiment_scaled,
                "unit": "sentiment score (0-100)",
                "metadata": {
                    "source": "twitter_sentiment",
                    "handles_analyzed": list(tweets_by_handle.keys()),
                    "total_tweets_analyzed": len(all_tweets),
                    "crypto_sentiment": crypto_sentiment_scaled,
                    "macro_sentiment": macro_sentiment_scaled,
                    "sentiment_category": sentiment_category,
                    "handle_metrics": handle_metrics,
                    "tweets": tweets_with_sentiment,  # Include individual tweets with sentiment
                    "last_updated_at": datetime.datetime.now().isoformat()
                }
            }
            
            # Add warning if applicable
            if analysis_warning:
                result["metadata"]["warning"] = analysis_warning
            
            # Save to output file if specified
            if self.output_file:
                try:
                    import os
                    os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
                    with open(self.output_file, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)
                    logger.info(f"Saved transformed data to {self.output_file}")
                except Exception as e:
                    logger.error(f"Error saving output file: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"Twitter sentiment transformation failed: {e}")
            raise TransformationError(f"Twitter sentiment transformation failed: {e}")
            
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
            import os
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved transformed data to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving transformed data: {e}")
            raise 