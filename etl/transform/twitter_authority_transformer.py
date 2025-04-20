from typing import Dict, Any, List, Tuple
from utils.logging_config import logger, log_plugin_info
from utils.exceptions import TransformationError
import datetime
import json
import math

class TwitterAuthorityTransformer:
    """
    Analyzes Twitter influencers to rank their authority and impact in the crypto space.
    This helps identify whose opinions are most likely to move markets or signal
    important shifts in market sentiment.
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        self.params = params or {}
        
        # Configure weighting factors for influence score
        self.follower_weight = self.params.get('follower_weight', 0.4)
        self.engagement_weight = self.params.get('engagement_weight', 0.3)
        self.content_weight = self.params.get('content_weight', 0.3)
        
        # Keywords for identifying authoritative content
        self.authority_keywords = self.params.get('authority_keywords', [
            'predict', 'forecast', 'expect', 'outlook', 'analysis', 'thesis',
            'conviction', 'opportunity', 'risk', 'asymmetric', 'allocation', 'bull',
            'bear', 'position', 'accumulate', 'sell', 'buy', 'long', 'short'
        ])
    
    def _calculate_engagement_rate(self, tweet: Dict[str, Any]) -> float:
        """Calculate the engagement rate for a tweet"""
        followers = tweet.get('user_followers', 0)
        if followers <= 0:
            return 0
            
        # Sum of all engagement metrics
        engagement = (
            tweet.get('reply_count', 0) + 
            tweet.get('retweet_count', 0) * 2 +  # Weight retweets higher
            tweet.get('like_count', 0) + 
            tweet.get('quote_count', 0) * 1.5    # Weight quotes higher than likes
        )
        
        # Normalize by follower count with log scaling to prevent extreme skew
        # from very high follower counts
        return engagement / (math.log10(followers + 10) * 5)
    
    def _extract_authority_signals(self, text: str) -> int:
        """Count authority signals in a tweet"""
        text_lower = text.lower()
        return sum(1 for keyword in self.authority_keywords if keyword in text_lower)
    
    def _analyze_influencer(self, handle: str, tweets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze a single influencer's tweets and calculate authority metrics"""
        if not tweets:
            return {
                'handle': handle,
                'tweet_count': 0,
                'authority_score': 0,
                'engagement_rate': 0,
                'authority_signals': 0,
                'follower_count': 0
            }
        
        # Extract consistent user data from the first tweet
        first_tweet = tweets[0]
        follower_count = first_tweet.get('user_followers', 0)
        verified = first_tweet.get('user_verified', False)
        
        # Calculate engagement metrics across all tweets
        total_engagement_rate = 0
        total_authority_signals = 0
        
        for tweet in tweets:
            engagement_rate = self._calculate_engagement_rate(tweet)
            total_engagement_rate += engagement_rate
            
            content = tweet.get('content', '')
            authority_signals = self._extract_authority_signals(content)
            total_authority_signals += authority_signals
        
        # Calculate averages
        avg_engagement_rate = total_engagement_rate / len(tweets) if tweets else 0
        avg_authority_signals = total_authority_signals / len(tweets) if tweets else 0
        
        # Calculate base components of authority score
        follower_score = min(math.log10(follower_count + 10) / 6.0, 1.0) if follower_count > 0 else 0
        engagement_score = min(avg_engagement_rate * 10, 1.0)  # Cap at 1.0
        content_score = min(avg_authority_signals / 2.0, 1.0)  # Cap at 1.0
        
        # Verified bonus
        verified_bonus = 0.1 if verified else 0
        
        # Calculate weighted authority score
        authority_score = (
            (follower_score * self.follower_weight) +
            (engagement_score * self.engagement_weight) +
            (content_score * self.content_weight) +
            verified_bonus
        ) * 100  # Scale to 0-100
        
        return {
            'handle': handle,
            'tweet_count': len(tweets),
            'authority_score': round(authority_score, 2),
            'engagement_rate': round(avg_engagement_rate, 4),
            'authority_signals': total_authority_signals,
            'follower_count': follower_count,
            'verified': verified
        }
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Twitter data into authority rankings for influencers.
        
        The output includes:
        - Ranked list of influencers by authority score
        - Component breakdown of what makes each influencer authoritative
        - Suggested weighting for incorporating their opinions into investment decisions
        """
        try:
            tweets_by_handle = raw_data.get('tweets_by_handle', {})
            
            if not tweets_by_handle:
                raise TransformationError("No tweets found in raw data")
            
            # Analyze each influencer
            influencer_metrics = []
            for handle, tweets in tweets_by_handle.items():
                metrics = self._analyze_influencer(handle, tweets)
                influencer_metrics.append(metrics)
            
            # Sort by authority score
            ranked_influencers = sorted(
                influencer_metrics,
                key=lambda x: x['authority_score'],
                reverse=True
            )
            
            # Extract top influencers
            top_influencers = ranked_influencers[:5] if len(ranked_influencers) >= 5 else ranked_influencers
            
            # Calculate influence distribution (for weighting opinions)
            total_authority = sum(inf['authority_score'] for inf in ranked_influencers)
            
            influence_weights = {}
            if total_authority > 0:
                for inf in ranked_influencers:
                    influence_weights[inf['handle']] = inf['authority_score'] / total_authority
            
            # Get top influencer handles and their scores
            top_influencer_data = [
                {"handle": inf['handle'], "score": inf['authority_score']}
                for inf in top_influencers
            ]
            
            # Extract current date
            dt_object = datetime.datetime.now()
            date_str = dt_object.strftime("%Y-%m-%d")
            
            # Create a summary of top voices
            top_voices = ", ".join([inf['handle'] for inf in top_influencers[:3]])
            log_plugin_info('transform', 'TwitterAuthorityTransformer', 
                           f"Analyzed {len(ranked_influencers)} influencers. " 
                           f"Top voices: {top_voices}")
            
            return {
                "date": date_str,
                "value": json.dumps(top_influencer_data),  # Store top influencers as the main value
                "unit": "authority ranking",
                "metadata": {
                    "source": "twitter_authority",
                    "influencers_analyzed": len(ranked_influencers),
                    "all_rankings": ranked_influencers,
                    "influence_weights": influence_weights,
                    "top_voices": top_voices,
                    "last_updated_at": datetime.datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Twitter authority transformation failed: {e}")
            raise TransformationError(f"Twitter authority transformation failed: {e}") 