import yaml
from importlib import import_module, reload
from utils.logging_config import (
    logger, setup_logging, log_signal_start, log_extract_start, 
    log_transform_start, log_load_start, log_etl_success, log_etl_failure,
    log_pipeline_start, log_pipeline_end, log_plugin_info
)
from core.secrets_manager import SecretsManager
from utils.exceptions import ExtractionError, TransformationError, LoadError
import os
import sys
import inspect
import json
import re

# --- Import Plugin Classes Directly --- 
from etl.extract.fred_extractor import FredExtractor
from etl.extract.alternative_extractor import AlternativeExtractor
from etl.extract.coingecko_extractor import CoinGeckoExtractor
from etl.extract.alternative_global_extractor import AlternativeGlobalExtractor

from etl.transform.m2_transformer import M2Transformer
from etl.transform.fear_greed_transformer import FearGreedTransformer
from etl.transform.bitcoin_price_transformer import BitcoinPriceTransformer
from etl.transform.total_market_cap_transformer import TotalMarketCapTransformer
from etl.transform.bitcoin_24h_change_transformer import Bitcoin24hChangeTransformer
from etl.transform.bitcoin_7d_change_transformer import Bitcoin7dChangeTransformer
from etl.transform.bitcoin_30d_change_transformer import Bitcoin30dChangeTransformer
from etl.transform.bitcoin_market_cap_transformer import BitcoinMarketCapTransformer
from etl.transform.bitcoin_24h_volume_transformer import Bitcoin24hVolumeTransformer
# Add other transformers if they exist and are used

# --- Loaders are still loaded dynamically via _load_plugin --- 

class ETLEngine:
    def __init__(self, config_path: str):
        """Initialize the ETL engine with a config file path"""
        
        # Setup logging
        setup_logging()
        
        # Load configuration from YAML file
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Process environment variables in the configuration
        self.config = self._process_env_vars(self.config)
        
        # Initialize the secrets manager
        self.secrets_manager = SecretsManager()
        
        # Initialize cache for extractors
        self.extractor_cache = {}

        # --- Direct Import Plugin Mapping ---
        self.extractor_map = {
            "fred_extractor": FredExtractor,
            "alternative_extractor": AlternativeExtractor,
            "coingecko_extractor": CoinGeckoExtractor,
            "alternative_global_extractor": AlternativeGlobalExtractor
        }
        self.transformer_map = {
            "m2_transformer": M2Transformer,
            "fear_greed_transformer": FearGreedTransformer,
            "bitcoin_price_transformer": BitcoinPriceTransformer,
            "total_market_cap_transformer": TotalMarketCapTransformer,
            "bitcoin_24h_change_transformer": Bitcoin24hChangeTransformer,
            "bitcoin_7d_change_transformer": Bitcoin7dChangeTransformer,
            "bitcoin_30d_change_transformer": Bitcoin30dChangeTransformer,
            "bitcoin_market_cap_transformer": BitcoinMarketCapTransformer,
            "bitcoin_24h_volume_transformer": Bitcoin24hVolumeTransformer
        }

    def _process_env_vars(self, config_data):
        """
        Recursively process configuration data to replace environment variable templates.
        Supports both {{ VAR_NAME }} and ${VAR_NAME} formats.
        """
        if isinstance(config_data, dict):
            return {k: self._process_env_vars(v) for k, v in config_data.items()}
        elif isinstance(config_data, list):
            return [self._process_env_vars(item) for item in config_data]
        elif isinstance(config_data, str):
            # Handle {{ VAR_NAME }} format
            pattern1 = r'\{\{\s*([A-Za-z0-9_]+)\s*\}\}'
            matches = re.findall(pattern1, config_data)
            result = config_data
            for var_name in matches:
                env_value = os.getenv(var_name)
                if env_value:
                    result = result.replace(f'{{ {var_name} }}', env_value)
                    result = result.replace(f'{{{{{var_name}}}}}', env_value)  # Handle no spaces
            
            # Handle ${VAR_NAME} format
            pattern2 = r'\$\{([A-Za-z0-9_]+)\}'
            matches = re.findall(pattern2, result)
            for var_name in matches:
                env_value = os.getenv(var_name)
                if env_value:
                    result = result.replace(f'${{{var_name}}}', env_value)
            
            return result
        else:
            return config_data

    def _load_plugin(self, plugin_type: str, plugin_name: str):
        # This is now only used for loaders
        if plugin_type != "load":
            logger.error(f"_load_plugin called for non-loader type: {plugin_type}")
        module_path = f"etl.{plugin_type}.{plugin_name}"
        class_name = ''.join(word.title() for word in plugin_name.split('_'))
        logger.debug(f"Attempting to import {module_path}")
        module = import_module(module_path)
        logger.debug(f"Attempting to get attribute {class_name}")
        plugin_class = getattr(module, class_name)
        return plugin_class
    
    def _make_hashable_key(self, params):
        """Convert potentially unhashable parameters into a hashable form"""
        if isinstance(params, dict):
            # Convert dict to a tuple of tuples (key, value) sorted by key
            try:
                # Try the direct approach first (if all values are hashable)
                return tuple(sorted((k, self._make_hashable_key(v)) for k, v in params.items()))
            except TypeError:
                # Fall back to JSON string if direct conversion fails
                return json.dumps(params, sort_keys=True)
        elif isinstance(params, list):
            # Convert list to a tuple, making each element hashable
            return tuple(self._make_hashable_key(item) for item in params)
        elif isinstance(params, (str, int, float, bool, type(None))):
            # These types are already hashable
            return params
        else:
            # For other types, convert to string
            return str(params)

    def run(self):
        log_pipeline_start()
        self.extractor_cache.clear()

        for signal_name, signal_config in self.config.get("signals", {}).items():
            log_signal_start(signal_name)
            try:
                # --- Get Extractor Class from Map --- 
                extractor_def = signal_config["extractor"]
                
                # Handle both string and dictionary style extractor definitions
                if isinstance(extractor_def, dict):
                    # Dictionary-style definition (module/class/params pattern)
                    module_name = extractor_def.get("module")
                    class_name = extractor_def.get("class")
                    log_extract_start(f"{class_name}")
                    extractor_params = extractor_def.get("params", {}).copy()
                    
                    # Import the module and get the class
                    try:
                        module = import_module(module_name)
                        extractor_class = getattr(module, class_name)
                    except (ImportError, AttributeError) as e:
                        logger.error(f"Failed to import extractor for signal '{signal_name}': {e}")
                        continue
                else:
                    # String-style definition (use the mapping)
                    extractor_name = extractor_def
                    log_extract_start(f"{extractor_name}")
                    extractor_class = self.extractor_map.get(extractor_name)
                    if not extractor_class:
                        logger.error(f"Unknown extractor '{extractor_name}' specified for signal '{signal_name}'. Check mapping in ETLEngine.")
                        continue # Skip this signal
                    
                    # --- Extractor Params & Secrets --- 
                    extractor_params = signal_config.get("extractor_params", {}).copy()
                    secrets = self.secrets_manager.get_secrets(signal_config.get("secrets", []))
                    secret_mapping = signal_config.get("secret_mapping", {})
                    for param_name, secret_name in secret_mapping.items():
                        if secret_name in secrets:
                            extractor_params[param_name] = secrets[secret_name]
                        else:
                            logger.warning(f"Secret '{secret_name}' not found for signal '{signal_name}'")

                # --- Caching Logic --- 
                # Use our special hashable function to handle any unhashable types
                try:
                    if isinstance(extractor_def, dict):
                        cache_key = (module_name, class_name, self._make_hashable_key(extractor_params))
                    else:
                        cache_key = (extractor_name, self._make_hashable_key(extractor_params))
                        
                    if cache_key in self.extractor_cache:
                        log_plugin_info('extract', extractor_class.__name__, "Cache hit, using cached data")
                        raw_data = self.extractor_cache[cache_key]
                    else:
                        log_plugin_info('extract', extractor_class.__name__, "Cache miss, fetching new data")
                        # Instantiate using mapped class
                        extractor = extractor_class(params=extractor_params) 
                        raw_data = extractor.fetch()
                        self.extractor_cache[cache_key] = raw_data
                        log_plugin_info('extract', extractor_class.__name__, f"Fetched data: {type(raw_data).__name__}")
                except Exception as e:
                    logger.error(f"Cache key generation failed: {e}. Proceeding without caching.")
                    # Fall back to direct extraction without caching
                    extractor = extractor_class(params=extractor_params)
                    raw_data = extractor.fetch()
                    log_plugin_info('extract', extractor_class.__name__, f"Fetched data without caching: {type(raw_data).__name__}")

                # --- Get Transformer Class from Map --- 
                transformer_def = signal_config["transformer"]
                
                # Handle both string and list style transformer definitions
                if isinstance(transformer_def, dict) or isinstance(transformer_def, list):
                    # Handle newer style transformer definition
                    # For now, just use the first transformer in the list
                    if isinstance(transformer_def, list):
                        transformer_def = transformer_def[0]
                    
                    # Dictionary-style definition
                    module_name = transformer_def.get("module")
                    class_name = transformer_def.get("class")
                    log_transform_start(f"{class_name}")
                    transformer_params = transformer_def.get("params", {})
                    
                    # Import the module and get the class
                    try:
                        module = import_module(module_name)
                        transformer_class = getattr(module, class_name)
                        transformer = transformer_class(params=transformer_params) if transformer_params else transformer_class()
                    except (ImportError, AttributeError) as e:
                        logger.error(f"Failed to import transformer for signal '{signal_name}': {e}")
                        continue
                else:
                    # String-style definition (use the mapping)
                    transformer_name = transformer_def
                    log_transform_start(f"{transformer_name}")
                    transformer_class = self.transformer_map.get(transformer_name)
                    if not transformer_class:
                        logger.error(f"Unknown transformer '{transformer_name}' specified for signal '{signal_name}'. Check mapping in ETLEngine.")
                        continue # Skip this signal
                        
                    # Instantiate transformer
                    transformer = transformer_class()
                
                log_plugin_info('transform', transformer.__class__.__name__, "Transforming data")
                transformed_data = transformer.transform(raw_data)
                transformed_data.update({"signal_name": signal_name})
                log_plugin_info('transform', transformer.__class__.__name__, f"Transformed data: {transformed_data.get('value')} ({transformed_data.get('unit', 'no unit')})")

                # --- Loaders (Still Dynamic) --- 
                loaders = []
                # Define default loaders (can be overridden in signals.yaml)
                default_loaders = [
                    {
                        "type": "supabase_loader",
                        "config": {
                            "table": "financial_signals" # URL/Key from env vars
                        }
                    }
                    # Google Sheets loader removed
                ]
                loader_configs = signal_config.get("loaders", default_loaders)
                for loader_config in loader_configs:
                    try:
                        if "module" in loader_config:
                            # Module/class style loader definition
                            module_name = loader_config.get("module")
                            class_name = loader_config.get("class")
                            
                            # Skip Google Sheets loader
                            if "google_sheets_loader" in module_name.lower():
                                log_plugin_info('load', 'GoogleSheetsLoader', f"Skipping for signal '{signal_name}' as it has been disabled")
                                continue
                                
                            loader_params = loader_config.get("params", {}).copy()
                            
                            # For specific loaders, inject common environment variables
                            if "supabase_loader" in module_name.lower():
                                loader_params["url"] = os.getenv("SUPABASE_URL")
                                loader_params["key"] = os.getenv("SUPABASE_KEY")
                                log_plugin_info('load', 'SupabaseLoader', f"Using Supabase URL: {loader_params['url'][:10]}...")
                            elif "google_sheets_loader" in module_name.lower():
                                if "sheet_name" not in loader_params:
                                    loader_params["sheet_name"] = os.getenv("GOOGLE_SHEET_NAME", "Financial Signals")
                                if "worksheet" not in loader_params:
                                    loader_params["worksheet"] = os.getenv("GOOGLE_SHEET_WORKSHEET", "raw_data")
                                loader_params["GOOGLE_SHEET_ID"] = os.getenv("GOOGLE_SHEET_ID")
                            
                            # Import the module and get the class
                            try:
                                module = import_module(module_name)
                                loader_class = getattr(module, class_name)
                                
                                # Try to match the expected parameter format (config vs params)
                                try:
                                    loader_instance = loader_class(params=loader_params)
                                except TypeError:
                                    # If params doesn't work, try with config
                                    loader_instance = loader_class(config=loader_params)
                            except (ImportError, AttributeError) as e:
                                logger.error(f"Failed to import loader for signal '{signal_name}': {e}")
                                continue
                        else:
                            # Type style loader definition
                            loader_type = loader_config["type"]
                            
                            # Skip Google Sheets loader
                            if loader_type == "google_sheets_loader":
                                log_plugin_info('load', 'GoogleSheetsLoader', f"Skipping for signal '{signal_name}' as it has been disabled")
                                continue
                                
                            loader_specific_config = loader_config.get("config", {}).copy()
                            
                            # Inject common secrets/env vars if needed by loaders
                            if loader_type == "supabase_loader":
                                 loader_specific_config["url"] = os.getenv("SUPABASE_URL")
                                 loader_specific_config["key"] = os.getenv("SUPABASE_KEY")
                                 log_plugin_info('load', 'SupabaseLoader', f"Using Supabase URL: {loader_specific_config['url'][:10]}...")
                            elif loader_type == "google_sheets_loader":
                                loader_specific_config["sheet_name"] = os.getenv("GOOGLE_SHEET_NAME", "Financial Signals")
                                loader_specific_config["worksheet"] = os.getenv("GOOGLE_SHEET_WORKSHEET", "raw_data")
                                loader_specific_config["GOOGLE_SHEET_ID"] = os.getenv("GOOGLE_SHEET_ID") # Pass ID for loader

                            # Use _load_plugin ONLY for loaders
                            loader_class = self._load_plugin("load", loader_type)
                            loader_instance = loader_class(config=loader_specific_config)
                        
                        loaders.append(loader_instance)
                    except Exception as e:
                        logger.error(f"Failed to initialize loader for signal '{signal_name}': {e}")
                        continue
                
                # --- Execute Loading --- 
                load_successful = True
                for loader in loaders:
                    log_load_start(loader.__class__.__name__)
                    try:
                        log_plugin_info('load', loader.__class__.__name__, f"Loading data for signal '{signal_name}'")
                        loader.load(transformed_data)
                        log_plugin_info('load', loader.__class__.__name__, f"Successfully loaded data for signal '{signal_name}'")
                    except LoadError as e:
                        logger.error(f"Loader {loader.__class__.__name__} failed for signal '{signal_name}': {e}")
                        load_successful = False
                    except Exception as e:
                         logger.error(f"Unexpected error in loader {loader.__class__.__name__} for signal '{signal_name}': {e}")
                         load_successful = False

                if load_successful:
                     log_etl_success(signal_name)
                else:
                     logger.warning(f"Signal '{signal_name}' processed but failed to load to one or more destinations.")

            except Exception as e:
                # Format error message for readability
                error_message = f"Failed to process signal '{signal_name}': {str(e)}"
                error_type = type(e).__name__
                
                # Add context if it's a known error type
                if isinstance(e, TransformationError):
                    error_message = f"Transform error for signal '{signal_name}': {str(e)}"
                elif isinstance(e, LoadError):
                    error_message = f"Load error for signal '{signal_name}': {str(e)}"
                elif isinstance(e, ExtractionError):
                    error_message = f"Extract error for signal '{signal_name}': {str(e)}"
                
                log_etl_failure(signal_name, f"{error_type}: {error_message}")
                continue

        log_pipeline_end()
        logger.info("ETL pipeline run finished.")

    def run_signal(self, signal_name: str):
        """Run only a specific signal by name"""
        log_pipeline_start()
        self.extractor_cache.clear()

        if signal_name not in self.config.get("signals", {}):
            logger.error(f"Signal '{signal_name}' not found in configuration")
            return False

        signal_config = self.config["signals"][signal_name]
        log_signal_start(signal_name)
        try:
            # --- Get Extractor Class from Map --- 
            extractor_def = signal_config["extractor"]
            
            # Handle both string and dictionary style extractor definitions
            if isinstance(extractor_def, dict):
                # Dictionary-style definition (module/class/params pattern)
                module_name = extractor_def.get("module")
                class_name = extractor_def.get("class")
                log_extract_start(f"{class_name}")
                extractor_params = extractor_def.get("params", {}).copy()
                
                # Import the module and get the class
                try:
                    module = import_module(module_name)
                    extractor_class = getattr(module, class_name)
                except (ImportError, AttributeError) as e:
                    logger.error(f"Failed to import extractor for signal '{signal_name}': {e}")
                    return False
            else:
                # String-style definition (use the mapping)
                extractor_name = extractor_def
                log_extract_start(f"{extractor_name}")
                extractor_class = self.extractor_map.get(extractor_name)
                if not extractor_class:
                    logger.error(f"Unknown extractor '{extractor_name}' specified for signal '{signal_name}'. Check mapping in ETLEngine.")
                    return False
                
                # --- Extractor Params & Secrets --- 
                extractor_params = signal_config.get("extractor_params", {}).copy()
                secrets = self.secrets_manager.get_secrets(signal_config.get("secrets", []))
                secret_mapping = signal_config.get("secret_mapping", {})
                for param_name, secret_name in secret_mapping.items():
                    if secret_name in secrets:
                        extractor_params[param_name] = secrets[secret_name]
                    else:
                        logger.warning(f"Secret '{secret_name}' not found for signal '{signal_name}'")

            # --- Caching Logic --- 
            # Use our special hashable function to handle any unhashable types
            try:
                if isinstance(extractor_def, dict):
                    cache_key = (module_name, class_name, self._make_hashable_key(extractor_params))
                else:
                    cache_key = (extractor_name, self._make_hashable_key(extractor_params))
                    
                if cache_key in self.extractor_cache:
                    log_plugin_info('extract', extractor_class.__name__, "Cache hit, using cached data")
                    raw_data = self.extractor_cache[cache_key]
                else:
                    log_plugin_info('extract', extractor_class.__name__, "Cache miss, fetching new data")
                    # Instantiate using mapped class
                    extractor = extractor_class(params=extractor_params) 
                    raw_data = extractor.fetch()
                    self.extractor_cache[cache_key] = raw_data
                    log_plugin_info('extract', extractor_class.__name__, f"Fetched data: {type(raw_data).__name__}")
            except Exception as e:
                logger.error(f"Cache key generation failed: {e}. Proceeding without caching.")
                # Fall back to direct extraction without caching
                extractor = extractor_class(params=extractor_params)
                raw_data = extractor.fetch()
                log_plugin_info('extract', extractor_class.__name__, f"Fetched data without caching: {type(raw_data).__name__}")

            # --- Get Transformer Class from Map --- 
            transformer_def = signal_config["transformer"]
            
            # Handle both string and list style transformer definitions
            if isinstance(transformer_def, dict) or isinstance(transformer_def, list):
                # Handle newer style transformer definition
                # For now, just use the first transformer in the list
                if isinstance(transformer_def, list):
                    transformer_def = transformer_def[0]
                
                # Dictionary-style definition
                module_name = transformer_def.get("module")
                class_name = transformer_def.get("class")
                log_transform_start(f"{class_name}")
                transformer_params = transformer_def.get("params", {})
                
                # Import the module and get the class
                try:
                    module = import_module(module_name)
                    transformer_class = getattr(module, class_name)
                    transformer = transformer_class(params=transformer_params) if transformer_params else transformer_class()
                except (ImportError, AttributeError) as e:
                    logger.error(f"Failed to import transformer for signal '{signal_name}': {e}")
                    return False
            else:
                # String-style definition (use the mapping)
                transformer_name = transformer_def
                log_transform_start(f"{transformer_name}")
                transformer_class = self.transformer_map.get(transformer_name)
                if not transformer_class:
                    logger.error(f"Unknown transformer '{transformer_name}' specified for signal '{signal_name}'. Check mapping in ETLEngine.")
                    return False
                    
                # Instantiate transformer
                transformer = transformer_class()
            
            log_plugin_info('transform', transformer.__class__.__name__, "Transforming data")
            transformed_data = transformer.transform(raw_data)
            transformed_data.update({"signal_name": signal_name})
            log_plugin_info('transform', transformer.__class__.__name__, f"Transformed data: {transformed_data.get('value')} ({transformed_data.get('unit', 'no unit')})")

            # --- Loaders (Still Dynamic) --- 
            loaders = []
            # Define default loaders (can be overridden in signals.yaml)
            default_loaders = [
                {
                    "type": "supabase_loader",
                    "config": {
                        "table": "financial_signals" # URL/Key from env vars
                    }
                }
                # Google Sheets loader removed
            ]
            loader_configs = signal_config.get("loaders", default_loaders)
            for loader_config in loader_configs:
                try:
                    if "module" in loader_config:
                        # Module/class style loader definition
                        module_name = loader_config.get("module")
                        class_name = loader_config.get("class")
                        
                        # Skip Google Sheets loader
                        if "google_sheets_loader" in module_name.lower():
                            log_plugin_info('load', 'GoogleSheetsLoader', f"Skipping for signal '{signal_name}' as it has been disabled")
                            continue
                            
                        loader_params = loader_config.get("params", {}).copy()
                        
                        # For specific loaders, inject common environment variables
                        if "supabase_loader" in module_name.lower():
                            loader_params["url"] = os.getenv("SUPABASE_URL")
                            loader_params["key"] = os.getenv("SUPABASE_KEY")
                            log_plugin_info('load', 'SupabaseLoader', f"Using Supabase URL: {loader_params['url'][:10]}...")
                        elif "google_sheets_loader" in module_name.lower():
                            if "sheet_name" not in loader_params:
                                loader_params["sheet_name"] = os.getenv("GOOGLE_SHEET_NAME", "Financial Signals")
                            if "worksheet" not in loader_params:
                                loader_params["worksheet"] = os.getenv("GOOGLE_SHEET_WORKSHEET", "raw_data")
                            loader_params["GOOGLE_SHEET_ID"] = os.getenv("GOOGLE_SHEET_ID")
                        
                        # Import the module and get the class
                        try:
                            module = import_module(module_name)
                            loader_class = getattr(module, class_name)
                            
                            # Try to match the expected parameter format (config vs params)
                            try:
                                loader_instance = loader_class(params=loader_params)
                            except TypeError:
                                # If params doesn't work, try with config
                                loader_instance = loader_class(config=loader_params)
                        except (ImportError, AttributeError) as e:
                            logger.error(f"Failed to import loader for signal '{signal_name}': {e}")
                            continue
                    else:
                        # Type style loader definition
                        loader_type = loader_config["type"]
                        
                        # Skip Google Sheets loader
                        if loader_type == "google_sheets_loader":
                            log_plugin_info('load', 'GoogleSheetsLoader', f"Skipping for signal '{signal_name}' as it has been disabled")
                            continue
                            
                        loader_specific_config = loader_config.get("config", {}).copy()
                        
                        # Inject common secrets/env vars if needed by loaders
                        if loader_type == "supabase_loader":
                             loader_specific_config["url"] = os.getenv("SUPABASE_URL")
                             loader_specific_config["key"] = os.getenv("SUPABASE_KEY")
                             log_plugin_info('load', 'SupabaseLoader', f"Using Supabase URL: {loader_specific_config['url'][:10]}...")
                        elif loader_type == "google_sheets_loader":
                            loader_specific_config["sheet_name"] = os.getenv("GOOGLE_SHEET_NAME", "Financial Signals")
                            loader_specific_config["worksheet"] = os.getenv("GOOGLE_SHEET_WORKSHEET", "raw_data")
                            loader_specific_config["GOOGLE_SHEET_ID"] = os.getenv("GOOGLE_SHEET_ID") # Pass ID for loader

                        # Use _load_plugin ONLY for loaders
                        loader_class = self._load_plugin("load", loader_type)
                        loader_instance = loader_class(config=loader_specific_config)
                    
                    loaders.append(loader_instance)
                except Exception as e:
                    logger.error(f"Failed to initialize loader for signal '{signal_name}': {e}")
                    continue
            
            # --- Execute Loading --- 
            load_successful = True
            for loader in loaders:
                log_load_start(loader.__class__.__name__)
                try:
                    log_plugin_info('load', loader.__class__.__name__, f"Loading data for signal '{signal_name}'")
                    loader.load(transformed_data)
                    log_plugin_info('load', loader.__class__.__name__, f"Successfully loaded data for signal '{signal_name}'")
                except LoadError as e:
                    logger.error(f"Loader {loader.__class__.__name__} failed for signal '{signal_name}': {e}")
                    load_successful = False
                except Exception as e:
                     logger.error(f"Unexpected error in loader {loader.__class__.__name__} for signal '{signal_name}': {e}")
                     load_successful = False

            if load_successful:
                 log_etl_success(signal_name)
            else:
                 logger.warning(f"Signal '{signal_name}' processed but failed to load to one or more destinations.")

            return True
            
        except ExtractionError as e:
            logger.error(f"Extraction error for signal '{signal_name}': {e}")
            log_etl_failure(signal_name, "extraction", str(e))
            return False
        except TransformationError as e:
            logger.error(f"Transformation error for signal '{signal_name}': {e}")
            log_etl_failure(signal_name, "transformation", str(e))
            return False
        except LoadError as e:
            logger.error(f"Load error for signal '{signal_name}': {e}")
            log_etl_failure(signal_name, "loading", str(e))
            return False
        except Exception as e:
            logger.error(f"Unexpected error for signal '{signal_name}': {e}")
            log_etl_failure(signal_name, "unknown", str(e))
            return False