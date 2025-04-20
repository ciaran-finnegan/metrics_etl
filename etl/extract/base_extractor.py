import os
import json
from abc import ABC, abstractmethod
from typing import Dict, Any
from utils.exceptions import ExtractionError
from utils.logging_config import logger

class BaseExtractor(ABC):
    """Base class for all extractors with file saving capability"""
    
    def __init__(self, params: Dict[str, Any]):
        """Initialize with parameters including output_file from signals.yaml"""
        self.params = params
        self.output_file = params.get("output_file")
        
    @abstractmethod
    def extract(self) -> Dict[str, Any]:
        """Extract data from source - must be implemented by subclasses"""
        pass
    
    def fetch(self) -> Dict[str, Any]:
        """Extract data and save to file if output_file is specified"""
        data = self.extract()
        
        if self.output_file:
            self._save_to_file(data)
            
        return data
    
    def _save_to_file(self, data: Dict[str, Any]) -> None:
        """Save extracted data to JSON file"""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            # Save data to file
            with open(self.output_file, 'w') as f:
                json.dump(data, f, indent=2)
                
            logger.info(f"Data saved to {self.output_file}")
        except Exception as e:
            logger.error(f"Failed to save data to {self.output_file}: {e}")
            # Don't raise exception as this is non-critical functionality 