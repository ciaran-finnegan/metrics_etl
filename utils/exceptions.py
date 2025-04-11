class MissingSecretError(Exception):
    """Raised when a required environment variable is missing"""
    pass

class ExtractionError(Exception):
    """Raised when data extraction fails"""
    pass

class TransformationError(Exception):
    """Raised when data transformation fails"""
    pass

class LoadError(Exception):
    """Raised when data loading fails"""
    pass