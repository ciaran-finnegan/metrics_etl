#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for OpenAISentimentTransformer.

This script tests the OpenAISentimentTransformer class by analyzing tweets
extracted from Twitter for investment insights using OpenAI's API.
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.transform.openai_sentiment_transformer import OpenAISentimentTransformer
from utils.logging_config import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def main():
    """
    Test the OpenAISentimentTransformer class.
    
    This function initializes the transformer with configuration parameters,
    loads tweet data from a file, analyzes it, and outputs the results.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test the OpenAISentimentTransformer')
    parser.add_argument('--input-file', type=str, help='Input file containing extracted tweets')
    parser.add_argument('--output-file', type=str, help='Output file to save analyzed data')
    parser.add_argument('--screenshots-dir', type=str, default='screenshots', help='Directory containing screenshots')
    parser.add_argument('--model', type=str, default='gpt-4o', help='OpenAI model to use')
    parser.add_argument('--no-images', action='store_true', help='Skip image analysis')
    parser.add_argument('--include-full-response', action='store_true', help='Include full detailed analysis in output')
    
    args = parser.parse_args()
    
    # Use default input file if not specified
    input_file = args.input_file or 'twitter_output.json'
    
    # Use default output file if not specified
    output_file = args.output_file or 'twitter_openai_analysis.json'
    
    # Check if input file exists
    if not os.path.exists(input_file):
        logger.error(f"Input file {input_file} does not exist")
        return
    
    # Print configuration
    logger.info(f"Testing OpenAISentimentTransformer with the following configuration:")
    logger.info(f"- Input file: {input_file}")
    logger.info(f"- Output file: {output_file}")
    logger.info(f"- Screenshots directory: {args.screenshots_dir}")
    logger.info(f"- OpenAI model: {args.model}")
    logger.info(f"- Analyze images: {not args.no_images}")
    logger.info(f"- Include full response: {args.include_full_response}")
    
    # Initialize the transformer
    transformer = OpenAISentimentTransformer(params={
        'input_file': input_file,
        'output_file': output_file,
        'model': args.model,
        'analyze_images': not args.no_images,
        'screenshots_dir': args.screenshots_dir,
        'include_full_response': args.include_full_response,
        'min_request_interval': 1.0  # 1 second between API calls to avoid rate limits
    })
    
    # Perform transformation
    logger.info("Starting sentiment analysis with OpenAI...")
    try:
        result = transformer.transform()
        
        # Log results
        logger.info(f"Analysis complete. Overall sentiment: {result['metadata']['sentiment_category']} ({result['value']:.1f}/100)")
        logger.info(f"Analyzed {result['metadata']['total_tweets_analyzed']} tweets from {len(result['metadata']['handles_analyzed'])} handles")
        
        # Log prevalent themes and assets
        if result['metadata'].get('prevalent_themes'):
            logger.info(f"Key themes detected: {', '.join(result['metadata']['prevalent_themes'][:5])}")
        
        if result['metadata'].get('prevalent_specific_assets'):
            logger.info(f"Assets mentioned: {', '.join(result['metadata']['prevalent_specific_assets'][:5])}")
        
        # Log sample analysis from first tweet if available
        if 'handle_metrics' in result['metadata']:
            for handle, metrics in result['metadata']['handle_metrics'].items():
                if 'tweets' in metrics and metrics['tweets']:
                    sample = metrics['tweets'][0]
                    logger.info(f"\nSample tweet analysis from @{handle}:")
                    logger.info(f"Tweet: {sample['content'][:100]}..." if len(sample['content']) > 100 else f"Tweet: {sample['content']}")
                    logger.info(f"Sentiment: {sample['analysis']['sentiment']} ({sample['analysis']['sentiment_score']}/100)")
                    logger.info(f"Summary: {sample['analysis']['summary']}")
                    break
        
        logger.info(f"Full results saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error during transformation: {e}")

if __name__ == "__main__":
    main() 