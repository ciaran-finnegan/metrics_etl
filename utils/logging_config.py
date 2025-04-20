import logging
import colorama
from colorama import Fore, Style
import datetime

def setup_logging():
    # Initialize colorama
    colorama.init()
    
    # Custom formatter to make logs more readable
    class ColoredFormatter(logging.Formatter):
        FORMATS = {
            logging.DEBUG: Fore.CYAN + "[DEBUG] %(message)s" + Style.RESET_ALL,
            logging.INFO: Fore.GREEN + "[INFO] %(message)s" + Style.RESET_ALL,
            logging.WARNING: Fore.YELLOW + "[WARNING] %(message)s" + Style.RESET_ALL,
            logging.ERROR: Fore.RED + "[ERROR] %(message)s" + Style.RESET_ALL,
            logging.CRITICAL: Fore.RED + Style.BRIGHT + "[CRITICAL] %(message)s" + Style.RESET_ALL
        }

        def format(self, record):
            log_format = self.FORMATS.get(record.levelno)
            formatter = logging.Formatter(log_format)
            return formatter.format(record)

    # Console handler with colored output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter())
    
    # File handler with more details for debugging
    file_handler = logging.FileHandler('etl.log')
    file_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    file_handler.setFormatter(logging.Formatter(file_format))
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove any existing handlers and add ours
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

# Create a named logger for this module
logger = logging.getLogger("metrics_etl")

# Set up logging immediately
setup_logging()

# -------------------- Pipeline stage logging helpers --------------------

def _get_timestamp():
    """Get current timestamp for logging"""
    return datetime.datetime.now().strftime("%H:%M:%S")

def log_signal_start(signal_name):
    """Log the start of a signal's processing with visual separator"""
    timestamp = _get_timestamp()
    logger.info(f"{Fore.BLUE}╔{'═'*70}╗{Style.RESET_ALL}")
    logger.info(f"{Fore.BLUE}║ SIGNAL: {signal_name:<60} {timestamp} ║{Style.RESET_ALL}")
    logger.info(f"{Fore.BLUE}╚{'═'*70}╝{Style.RESET_ALL}")

def log_extract_start(extractor_name):
    """Log the start of extraction stage"""
    timestamp = _get_timestamp()
    logger.info(f"{Fore.MAGENTA}┌{'─'*70}┐{Style.RESET_ALL}")
    logger.info(f"{Fore.MAGENTA}│ EXTRACT: {extractor_name:<58} {timestamp} │{Style.RESET_ALL}")
    logger.info(f"{Fore.MAGENTA}└{'─'*70}┘{Style.RESET_ALL}")

def log_transform_start(transformer_name):
    """Log the start of transformation stage"""
    timestamp = _get_timestamp()
    logger.info(f"{Fore.YELLOW}┌{'─'*70}┐{Style.RESET_ALL}")
    logger.info(f"{Fore.YELLOW}│ TRANSFORM: {transformer_name:<56} {timestamp} │{Style.RESET_ALL}")
    logger.info(f"{Fore.YELLOW}└{'─'*70}┘{Style.RESET_ALL}")

def log_load_start(loader_name):
    """Log the start of loading stage"""
    timestamp = _get_timestamp()
    logger.info(f"{Fore.CYAN}┌{'─'*70}┐{Style.RESET_ALL}")
    logger.info(f"{Fore.CYAN}│ LOAD: {loader_name:<60} {timestamp} │{Style.RESET_ALL}")
    logger.info(f"{Fore.CYAN}└{'─'*70}┘{Style.RESET_ALL}")

def log_etl_success(signal_name):
    """Log successful ETL completion for a signal"""
    timestamp = _get_timestamp()
    logger.info(f"{Fore.GREEN}✓ SUCCESS: Signal '{signal_name}' processed successfully at {timestamp}{Style.RESET_ALL}")

def log_etl_failure(signal_name, error):
    """Log ETL failure for a signal"""
    timestamp = _get_timestamp()
    logger.error(f"{Fore.RED}✗ FAILURE: Signal '{signal_name}' failed at {timestamp}{Style.RESET_ALL}")
    logger.error(f"{Fore.RED}  Error: {error}{Style.RESET_ALL}")

def log_pipeline_start():
    """Log the start of the entire ETL pipeline run"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"{Fore.BLUE}╔{'═'*70}╗{Style.RESET_ALL}")
    logger.info(f"{Fore.BLUE}║ {'ETL PIPELINE STARTED':<60} {timestamp} ║{Style.RESET_ALL}")
    logger.info(f"{Fore.BLUE}╚{'═'*70}╝{Style.RESET_ALL}")

def log_pipeline_end():
    """Log the end of the entire ETL pipeline run"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"{Fore.BLUE}╔{'═'*70}╗{Style.RESET_ALL}")
    logger.info(f"{Fore.BLUE}║ {'ETL PIPELINE COMPLETED':<60} {timestamp} ║{Style.RESET_ALL}")
    logger.info(f"{Fore.BLUE}╚{'═'*70}╝{Style.RESET_ALL}")

def log_plugin_info(stage, plugin_name, message):
    """Log plugin-specific information during processing"""
    plugin_type_color = {
        'extract': Fore.MAGENTA,
        'transform': Fore.YELLOW,
        'load': Fore.CYAN
    }.get(stage.lower(), Fore.WHITE)
    
    logger.info(f"{plugin_type_color}[{stage.upper()}:{plugin_name}] {message}{Style.RESET_ALL}")