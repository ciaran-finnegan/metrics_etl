from typing import Dict, Any, List, Counter
from utils.logging_config import logger, log_plugin_info
from utils.exceptions import TransformationError
import re
import datetime
from collections import Counter
import json

class TwitterTopicsTransformer:
    """
    Analyzes Twitter data to identify trending topics among crypto/finance influencers.
    Useful for spotting emerging narratives and potential investment opportunities.
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        self.params = params or {}
        
        # Keywords to track and categorize (customizable via params)
        self.keyword_categories = self.params.get('keyword_categories', {
            'Layer 1': ['bitcoin', 'ethereum', 'solana', 'avalanche', 'cardano', 'polygon'],
            'Layer 2': ['arbitrum', 'optimism', 'starknet', 'zksync', 'polygon', 'lightning'],
            'DeFi': ['defi', 'uniswap', 'aave', 'compound', 'curve', 'lending', 'yield', 'staking'],
            'NFTs': ['nft', 'collection', 'art', 'artist', 'jpeg', 'pfp', 'marketplace'],
            'AI': ['ai', 'artificial intelligence', 'machine learning', 'ml', 'gpt', 'llm', 'neural'],
            'Regulation': ['sec', 'regulation', 'regulator', 'compliance', 'law', 'government', 'legal'],
            'Macro': ['inflation', 'rate', 'fed', 'economy', 'recession', 'market', 'stocks', 'bonds'],
            'Monetary': ['money', 'dollar', 'currency', 'gold', 'silver', 'printing', 'monetary']
        })
        
        self.min_topic_freq = self.params.get('min_topic_freq', 3)  # Minimum frequency to consider a topic trending
        self.top_n_topics = self.params.get('top_n_topics', 20)  # Number of top topics to extract
        self.exclude_words = self.params.get('exclude_words', [
            'the', 'a', 'an', 'and', 'or', 'but', 'on', 'in', 'with', 'for', 'to', 'from',
            'of', 'at', 'by', 'as', 'it', 'its', 'is', 'am', 'are', 'was', 'were', 'be',
            'this', 'that', 'these', 'those', 'my', 'your', 'his', 'her', 'our', 'their',
            'i', 'you', 'he', 'she', 'we', 'they', 'has', 'have', 'had', 'do', 'does',
            'did', 'will', 'would', 'should', 'could', 'can', 'may', 'might', 'must'
        ])
    
    def _clean_tweet_text(self, text: str) -> str:
        """Clean tweet text for topic extraction"""
        # Remove URLs
        text = re.sub(r'http\S+', '', text)
        # Remove user mentions
        text = re.sub(r'@\w+', '', text)
        # Remove special characters but keep # for hashtags
        text = re.sub(r'[^\w\s#]', '', text)
        # Remove extra spaces
        text = re.sub(r'\s+', ' ', text).strip()
        return text.lower()
    
    def _extract_hashtags(self, text: str) -> List[str]:
        """Extract hashtags from a tweet"""
        return re.findall(r'#(\w+)', text)
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Extract meaningful keywords from a tweet"""
        words = text.split()
        return [word for word in words 
                if word not in self.exclude_words 
                and len(word) > 3  # Ignore very short words
                and not word.startswith('#')]  # Hashtags are handled separately
    
    def _categorize_topics(self, topics: List[str]) -> Dict[str, int]:
        """Categorize topics into predefined categories"""
        category_counts = {category: 0 for category in self.keyword_categories.keys()}
        
        for topic in topics:
            for category, keywords in self.keyword_categories.items():
                if any(keyword in topic.lower() for keyword in keywords):
                    category_counts[category] += 1
                    break
        
        return category_counts
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Twitter data into topic trends and analysis.
        
        The output includes:
        - Top mentioned topics and hashtags
        - Category analysis (crypto, defi, macro, etc.)
        - Change in topic frequency (if historical data available)
        """
        try:
            tweets_by_handle = raw_data.get('tweets_by_handle', {})
            
            if not tweets_by_handle:
                raise TransformationError("No tweets found in raw data")
            
            # Collect all tweets
            all_tweets = []
            for handle, tweets in tweets_by_handle.items():
                all_tweets.extend(tweets)
            
            if not all_tweets:
                raise TransformationError("No tweets found in any handle")
            
            # Extract hashtags and keywords
            all_hashtags = []
            all_keywords = []
            
            for tweet in all_tweets:
                text = tweet.get('content', '')
                clean_text = self._clean_tweet_text(text)
                
                # Extract hashtags and keywords
                hashtags = self._extract_hashtags(clean_text)
                keywords = self._extract_keywords(clean_text)
                
                all_hashtags.extend(hashtags)
                all_keywords.extend(keywords)
            
            # Count frequencies
            hashtag_counter = Counter(all_hashtags)
            keyword_counter = Counter(all_keywords)
            
            # Get top hashtags and keywords
            top_hashtags = hashtag_counter.most_common(self.top_n_topics)
            top_keywords = keyword_counter.most_common(self.top_n_topics)
            
            # Combine topics (hashtags and keywords)
            all_topics = all_hashtags + all_keywords
            
            # Categorize topics
            category_counts = self._categorize_topics(all_topics)
            
            # Calculate dominant categories
            total_categorized = sum(category_counts.values())
            category_percentages = {
                category: (count / total_categorized * 100) if total_categorized > 0 else 0
                for category, count in category_counts.items()
            }
            
            # Sort categories by percentage
            sorted_categories = sorted(
                category_percentages.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Extract the top 3 categories
            top_categories = sorted_categories[:3] if len(sorted_categories) >= 3 else sorted_categories
            top_categories_names = [category for category, _ in top_categories]
            
            # Extract current date
            dt_object = datetime.datetime.now()
            date_str = dt_object.strftime("%Y-%m-%d")
            
            # Create a market narrative summary
            top_hashtag_text = f"#{top_hashtags[0][0]}" if top_hashtags else "none"
            narrative = f"Top focus: {', '.join(top_categories_names)}. " \
                        f"Most popular topic: {top_keywords[0][0] if top_keywords else 'none'}. " \
                        f"Trending hashtag: {top_hashtag_text}."
            
            log_plugin_info('transform', 'TwitterTopicsTransformer', 
                           f"Analyzed {len(all_tweets)} tweets. {narrative}")
            
            return {
                "date": date_str,
                "value": json.dumps(top_categories_names),  # Store top categories as the main value
                "unit": "trending topics",
                "metadata": {
                    "source": "twitter_topics",
                    "total_tweets_analyzed": len(all_tweets),
                    "narrative": narrative,
                    "top_hashtags": dict(top_hashtags[:10]),  # Convert to dict for easier storage
                    "top_keywords": dict(top_keywords[:10]),
                    "category_percentages": category_percentages,
                    "handles_analyzed": list(tweets_by_handle.keys()),
                    "last_updated_at": datetime.datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Twitter topics transformation failed: {e}")
            raise TransformationError(f"Twitter topics transformation failed: {e}") 