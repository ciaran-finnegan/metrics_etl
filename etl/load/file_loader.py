import os
import json
import csv
from typing import Dict, Any
from utils.exceptions import LoadError
from utils.logging_config import logger

class FileLoader:
    """Loader that saves transformed data to files in various formats"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the file loader with configuration
        
        Args:
            config: Dictionary with:
                - file_path: Path to save the file
                - format: File format (json, csv)
        """
        self.file_path = config.get("file_path")
        self.format = config.get("format", "json").lower()
        
        if not self.file_path:
            raise ValueError("file_path is required in FileLoader config")
        
        if self.format not in ["json", "csv"]:
            raise ValueError(f"Unsupported format '{self.format}'. Supported: json, csv")
    
    def load(self, data: Dict[str, Any]) -> None:
        """Save the transformed data to a file"""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            
            if self.format == "json":
                self._save_as_json(data)
            elif self.format == "csv":
                self._save_as_csv(data)
                
            logger.info(f"Data saved to {self.file_path}")
        except Exception as e:
            logger.error(f"Failed to save data to {self.file_path}: {e}")
            raise LoadError(f"File load error: {e}")
    
    def _save_as_json(self, data: Dict[str, Any]) -> None:
        """Save data as JSON file"""
        with open(self.file_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _save_as_csv(self, data: Dict[str, Any]) -> None:
        """Save data as CSV file"""
        # Extract headers from the data
        headers = list(data.keys())
        
        with open(self.file_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerow(data) 