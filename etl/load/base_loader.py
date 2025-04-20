from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseLoader(ABC):
    """Base class for all loaders"""
    
    @abstractmethod
    def init(self) -> None:
        """Initialize the loader"""
        pass
    
    @abstractmethod
    def append_row(self, data: Dict[str, Any]) -> None:
        """Append a row of data"""
        pass 