#!/usr/bin/env python3
"""
Setup script to download required NLTK resources for sentiment analysis.
"""

import nltk
import os
import sys

def download_nltk_resources():
    print("Setting up NLTK resources...")
    
    # Create a directory for NLTK data if it doesn't exist
    nltk_data_dir = os.path.expanduser("~/nltk_data")
    if not os.path.exists(nltk_data_dir):
        os.makedirs(nltk_data_dir)
        print(f"Created NLTK data directory at {nltk_data_dir}")
    
    # Download required resources for sentiment analysis
    try:
        nltk.download('vader_lexicon')
        print("Downloaded VADER lexicon for sentiment analysis")
        
        nltk.download('punkt')
        print("Downloaded Punkt tokenizer models")
        
        nltk.download('stopwords')
        print("Downloaded stopwords corpus")
        
        print("NLTK setup completed successfully")
        return True
    except Exception as e:
        print(f"Error downloading NLTK resources: {e}")
        return False

if __name__ == "__main__":
    success = download_nltk_resources()
    sys.exit(0 if success else 1) 