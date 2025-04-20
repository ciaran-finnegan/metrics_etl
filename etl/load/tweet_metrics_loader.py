import logging
import re
import os
from datetime import datetime
from supabase import create_client
from utils.exceptions import LoadError
from utils.logging_config import logger, log_plugin_info
from typing import Dict, Any, List, Optional

class TweetMetricsLoader:
    """
    Specialized loader for tweet metrics that supports:
    1. Loading individual tweet metrics to a dedicated tweet_metrics table
    2. Loading an aggregated sentiment value to the financial_signals table
    """
    
    def __init__(self, params: Dict[str, Any]):
        """
        Initializes the TweetMetricsLoader with configuration parameters.
        
        Args:
            params: Dictionary containing:
                - url (str): Supabase project URL (can use {{ SUPABASE_URL }} template)
                - key (str): Supabase API key (can use {{ SUPABASE_KEY }} template)
                - tweet_metrics_table (str, optional): Table for individual tweet metrics (default: "tweet_metrics")
                - signals_table (str, optional): Table for aggregated signals (default: "financial_signals")
                - signal_name (str, optional): Name for the aggregated signal (default: "twitter_sentiment")
        """
        try:
            # Extract and resolve configuration
            self.url = self._get_env_value(params.get("url", params.get("connection_string")))
            self.api_key = self._get_env_value(params.get("key", params.get("api_key")))
            self.tweet_metrics_table = params.get("tweet_metrics_table", "tweet_metrics")
            self.signals_table = params.get("signals_table", params.get("table", "financial_signals"))
            self.signal_name = params.get("signal_name", "twitter_sentiment")
            
            # Validate configuration
            if not self.url or not self.api_key:
                raise ValueError("Supabase URL and Key must be provided in params (as url/key or connection_string/api_key)")
                
            # Validate URL format
            if not self.url.startswith(('http://', 'https://')):
                raise ValueError(f"Invalid Supabase URL format: {self.url}")
                
            # Initialize Supabase client
            self.client = create_client(self.url, self.api_key)
            logger.info(f"Supabase connected for tweet metrics loading")
            
        except ValueError as ve:
            logger.error(f"TweetMetricsLoader configuration error: {ve}")
            raise LoadError(f"TweetMetricsLoader configuration error: {ve}")
        except Exception as e:
            logger.error(f"TweetMetricsLoader initialization failed: {e}")
            raise LoadError(f"TweetMetricsLoader initialization error: {e}")
            
    def _get_env_value(self, value: Optional[str]) -> Optional[str]:
        """
        Get value from environment variable if template is used.
        
        Args:
            value: The value to check for environment variable template
            
        Returns:
            The resolved value
        """
        if not value:
            return None
            
        # Check for {{ ENV_VAR }} pattern
        match = re.match(r'{{\s*(\w+)\s*}}', value)
        if match:
            env_var = match.group(1)
            env_value = os.environ.get(env_var)
            if not env_value:
                logger.error(f"Environment variable {env_var} not found")
                return None
            logger.info(f"Using {env_var} from environment variables")
            return env_value
            
        # Handle case where URL has curly braces around it
        if value.startswith('{') and value.endswith('}'):
            value = value[1:-1]
            
        return value

    def _create_tweet_metrics_table_if_not_exists(self) -> None:
        """
        Create the tweet_metrics table if it doesn't exist yet.
        This is a utility method to help with initial setup.
        """
        try:
            # Using Supabase SQL to create table if needed
            # Note: This requires appropriate database permissions
            sql = f"""
            CREATE TABLE IF NOT EXISTS "{self.tweet_metrics_table}" (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                date DATE NOT NULL,
                username TEXT NOT NULL,
                tweet_id TEXT,
                tweet_url TEXT,
                content TEXT,
                sentiment_score NUMERIC NOT NULL,
                sentiment_category TEXT,
                confidence NUMERIC,
                timeframe TEXT,
                asset_classes JSONB,
                specific_assets JSONB,
                key_themes JSONB,
                summary TEXT,
                has_image BOOLEAN,
                has_link BOOLEAN,
                primary_asset TEXT,
                primary_theme TEXT,
                metadata JSONB
            );

            CREATE INDEX IF NOT EXISTS idx_{self.tweet_metrics_table}_primary_asset ON "{self.tweet_metrics_table}"(primary_asset);
            CREATE INDEX IF NOT EXISTS idx_{self.tweet_metrics_table}_primary_theme ON "{self.tweet_metrics_table}"(primary_theme);
            """
            
            self.client.rpc('execute_sql', {'query': sql}).execute()
            logger.info(f"Ensured tweet_metrics table exists")
            
        except Exception as e:
            logger.warning(f"Failed to create tweet_metrics table: {e}")
            logger.warning("Will attempt to insert data anyway in case table already exists")
            
    def _calculate_aggregate_sentiment(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate aggregated sentiment metrics from individual tweet metrics.
        
        Args:
            metrics: List of individual tweet metrics
            
        Returns:
            Dictionary with aggregated sentiment data
        """
        if not metrics:
            return {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "signal_name": self.signal_name,
                "value": 50,  # Neutral default value
                "units": "sentiment score (0-100)",
                "metadata": {
                    "source": "openai_sentiment",
                    "sentiment_category": "neutral",
                    "tweets_analyzed": 0,
                    "handles_analyzed": [],
                    "last_updated_at": datetime.now().isoformat()
                }
            }
            
        # Calculate average sentiment score
        sentiment_scores = [m.get("value", 50) for m in metrics]
        avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 50
        
        # Determine sentiment category
        sentiment_category = "neutral"
        if avg_sentiment >= 70:
            sentiment_category = "very bullish"
        elif avg_sentiment >= 55:
            sentiment_category = "bullish"
        elif avg_sentiment <= 30:
            sentiment_category = "very bearish"
        elif avg_sentiment <= 45:
            sentiment_category = "bearish"
            
        # Collect handles
        handles = set()
        for metric in metrics:
            username = metric.get("metadata", {}).get("username")
            if username:
                handles.add(username)
                
        # Collect asset classes and themes
        asset_classes = set()
        specific_assets = set()
        key_themes = set()
        
        for metric in metrics:
            metadata = metric.get("metadata", {})
            # Add asset classes
            classes = metadata.get("asset_classes", [])
            if isinstance(classes, list):
                asset_classes.update(classes)
                
            # Add specific assets
            assets = metadata.get("specific_assets", [])
            if isinstance(assets, list):
                specific_assets.update(assets)
                
            # Add key themes
            themes = metadata.get("key_themes", [])
            if isinstance(themes, list):
                key_themes.update(themes)
                
        # Use most recent date
        today = datetime.now().strftime("%Y-%m-%d")
        dates = [m.get("date", today) for m in metrics]
        latest_date = max(dates) if dates else today
        
        # Derive primary asset / theme for quick filtering
        asset_classes_list = metadata.get("specific_assets", [])
        primary_asset = asset_classes_list[0] if isinstance(asset_classes_list, list) and asset_classes_list else None
        themes_list = metadata.get("key_themes", [])
        primary_theme = themes_list[0] if isinstance(themes_list, list) and themes_list else None
        
        # Construct aggregate record
        return {
            "date": latest_date,
            "signal_name": self.signal_name,
            "value": avg_sentiment,
            "units": "sentiment score (0-100)",
            "metadata": {
                "source": "openai_sentiment",
                "sentiment_category": sentiment_category,
                "tweets_analyzed": len(metrics),
                "handles_analyzed": list(handles),
                "prevalent_asset_classes": list(asset_classes),
                "prevalent_specific_assets": list(specific_assets),
                "prevalent_themes": list(key_themes),
                "last_updated_at": datetime.now().isoformat()
            }
        }
        
    def _insert_tweet_metric(self, metric: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert a single tweet metric into the tweet_metrics table.
        
        Args:
            metric: The tweet metric to insert
            
        Returns:
            Supabase response data
        """
        try:
            # Extract data from metric
            metadata = metric.get("metadata", {})
            
            # Construct tweet URL from username and tweet_id
            tweet_url = None
            username = metadata.get("username")
            tweet_id = metadata.get("tweet_id")
            if username and tweet_id:
                tweet_url = f"https://twitter.com/{username}/status/{tweet_id}"
            
            # Derive primary asset / theme for quick filtering
            asset_classes_list = metadata.get("specific_assets", [])
            primary_asset = asset_classes_list[0] if isinstance(asset_classes_list, list) and asset_classes_list else None
            themes_list = metadata.get("key_themes", [])
            primary_theme = themes_list[0] if isinstance(themes_list, list) and themes_list else None
            
            # Prepare record (including image/link flags)
            record = {
                "date": metric.get("date"),
                "username": username or "unknown",
                "tweet_id": tweet_id or "",
                "tweet_url": tweet_url,
                "content": metadata.get("content", ""),
                "sentiment_score": metric.get("value", 50),
                "sentiment_category": metadata.get("sentiment", "neutral"),
                "confidence": metadata.get("confidence", 0),
                "timeframe": metadata.get("timeframe", "unspecified"),
                "asset_classes": metadata.get("asset_classes", []),
                "specific_assets": metadata.get("specific_assets", []),
                "key_themes": metadata.get("key_themes", []),
                "summary": metadata.get("summary", ""),
                # Presence flags
                "has_image": metadata.get("has_image", False),
                "has_link": metadata.get("has_link", False),
                "primary_asset": primary_asset,
                "primary_theme": primary_theme,
                "metadata": {
                    k: v for k, v in metadata.items() 
                    if k not in ["username", "tweet_id", "content", "sentiment", 
                                "confidence", "timeframe", "asset_classes", 
                                "specific_assets", "key_themes", "summary", 
                                "has_image", "has_link"]
                }
            }
            
            # Skip if this tweet_id already exists (avoids duplicates when job re‑runs)
            if tweet_id:
                exists_query = (
                    self.client.table(self.tweet_metrics_table)
                    .select("id")
                    .eq("tweet_id", tweet_id)
                    .limit(1)
                    .execute()
                )
                if getattr(exists_query, "data", None):
                    logger.debug(f"Tweet {tweet_id} already in DB – skipping insert")
                    return exists_query.data

            # Insert record
            result = self.client.table(self.tweet_metrics_table).insert(record).execute()
            
            # Check for errors
            if hasattr(result, 'error') and result.error:
                logger.error(f"Error inserting tweet metric: {result.error}")
                return None
                
            return result.data
            
        except Exception as e:
            logger.error(f"Error inserting tweet metric: {e}")
            return None
            
    def _insert_aggregate_signal(self, aggregate: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert aggregate sentiment data into the financial_signals table.
        
        Args:
            aggregate: The aggregate sentiment data
            
        Returns:
            Supabase response data
        """
        try:
            # Upsert by (date, signal_name) to avoid duplicates if job reruns
            result = (
                self.client.table(self.signals_table)
                .upsert(aggregate, on_conflict="date,signal_name")
                .execute()
            )
            
            # Check for errors
            if hasattr(result, 'error') and result.error:
                logger.error(f"Error inserting aggregate signal: {result.error}")
                return None
                
            return result.data
            
        except Exception as e:
            logger.error(f"Error inserting aggregate signal: {e}")
            return None
    
    def load(self, data: Dict[str, Any]) -> bool:
        """
        Load tweet metrics data into the database.
        
        Args:
            data: The data from the transformer containing individual tweet metrics
            
        Returns:
            True if loading was successful, False otherwise
        """
        try:
            # Ensure we have metrics to process
            metrics = data.get("metrics", [])
            
            if not metrics:
                logger.warning("No metrics found in transformer output")
                return False
                
            # Ensure table exists
            self._create_tweet_metrics_table_if_not_exists()
            
            # Process each metric
            successful_inserts = 0
            for metric in metrics:
                if self._insert_tweet_metric(metric):
                    successful_inserts += 1
                    
            # Calculate and insert aggregate sentiment
            aggregate = self._calculate_aggregate_sentiment(metrics)
            aggregate_result = self._insert_aggregate_signal(aggregate)
            
            # Recalculate day/week change & z‑scores
            try:
                # Call the stats-refresh RPC; an empty params dict is required by Supabase client
                self.client.rpc('refresh_financial_signal_stats', {}).execute()
            except Exception as e:
                logger.warning(f"Could not refresh financial signal stats: {e}")
            
            # Log results
            log_plugin_info("load", "TweetMetricsLoader", 
                          f"Loaded {successful_inserts}/{len(metrics)} tweet metrics and aggregate sentiment")
            
            return successful_inserts > 0 or aggregate_result is not None
            
        except Exception as e:
            logger.error(f"TweetMetricsLoader load failed: {e}")
            raise LoadError(f"TweetMetricsLoader load error: {e}") 