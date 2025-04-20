from core.pipeline import ETLEngine
import argparse
import sys
import traceback
from utils.logging_config import logger, log_pipeline_start, log_pipeline_end
import os
import datetime

def main():
    """Main entry point for the ETL pipeline with robust error handling"""
    start_time = datetime.datetime.now()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Financial Metrics ETL Pipeline")
    parser.add_argument("--config", default="config/signals.yaml", help="Path to signals configuration file")
    parser.add_argument("--signal", help="Run a specific signal only")
    args = parser.parse_args()
    
    # Log environment info
    logger.info(f"Running ETL pipeline in environment: {os.environ.get('ENV', 'development')}")
    logger.info(f"Using configuration file: {args.config}")
    if args.signal:
        logger.info(f"Running only signal: {args.signal}")
    
    try:
        # Initialize and run the ETL engine
        engine = ETLEngine(args.config)
        
        if args.signal:
            # Run a single signal if specified
            engine.run_signal(args.signal)
        else:
            # Run all signals
            engine.run()
        
        # Log completion statistics
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
        return 0
        
    except FileNotFoundError as e:
        logger.critical(f"Configuration file not found: {e}")
        return 1
        
    except Exception as e:
        logger.critical(f"Unhandled exception in ETL pipeline: {e}")
        logger.critical(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())