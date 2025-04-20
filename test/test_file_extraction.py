import os
import json
from datetime import datetime
from dotenv import load_dotenv
from etl.extract.coingecko_extractor import CoinGeckoExtractor
from etl.load.file_loader import FileLoader
from utils.logging_config import logger, setup_logging

def test_file_extraction_loading():
    """Test the full extraction to file and loading from file workflow"""
    load_dotenv()
    setup_logging()
    
    logger.info("Starting file extraction and loading test...")
    
    # Test extraction with file output
    extractor_params = {
        "coin_id": "bitcoin",
        "output_file": "data/test/bitcoin_test.json"
    }
    
    # Ensure directory exists
    os.makedirs("data/test", exist_ok=True)
    
    try:
        # 1. Test extraction with file saving
        logger.info("Testing extraction with file saving...")
        extractor = CoinGeckoExtractor(params=extractor_params)
        
        # Extract data which should also save to file
        extracted_data = extractor.fetch()
        logger.info(f"Extracted data: {extracted_data}")
        
        # 2. Verify file was created
        if not os.path.exists(extractor_params["output_file"]):
            raise AssertionError(f"Output file was not created at {extractor_params['output_file']}")
        
        logger.info(f"File successfully created at {extractor_params['output_file']}")
        
        # 3. Transform data (simple transformation for testing)
        transformed_data = {
            "date": datetime.now().date().isoformat(),
            "value": extracted_data.get("usd", 0),
            "signal_name": "bitcoin_price_test",
            "units": "USD"
        }
        
        logger.info(f"Transformed data: {transformed_data}")
        
        # 4. Test file loader
        file_loader_config = {
            "file_path": "data/test/bitcoin_transformed.json",
            "format": "json"
        }
        
        logger.info(f"Testing file loader with config: {file_loader_config}")
        loader = FileLoader(config=file_loader_config)
        loader.load(transformed_data)
        
        # 5. Verify transformed file was created
        if not os.path.exists(file_loader_config["file_path"]):
            raise AssertionError(f"Transformed output file was not created at {file_loader_config['file_path']}")
        
        # 6. Read and verify file content
        with open(file_loader_config["file_path"], "r") as f:
            loaded_data = json.load(f)
            
        if loaded_data.get("value") != transformed_data.get("value"):
            raise AssertionError(f"Loaded data does not match transformed data. Expected {transformed_data}, got {loaded_data}")
        
        logger.info("All tests passed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
    
if __name__ == "__main__":
    test_file_extraction_loading() 