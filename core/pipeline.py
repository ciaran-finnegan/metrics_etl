import yaml
from etl.load.supabase_loader import SupabaseLoader
from etl.load.google_sheets_loader import GoogleSheetsLoader
from importlib import import_module
from utils.logging_config import logger, setup_logging
from core.secrets_manager import SecretsManager
from utils.exceptions import LoadError
import os

class ETLEngine:
    def __init__(self, config_path: str):
        setup_logging()
        self.secrets_manager = SecretsManager()
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

    def _load_plugin(self, plugin_type: str, plugin_name: str):
        try:
            module = import_module(f"etl.{plugin_type}.{plugin_name}")
            return getattr(module, plugin_name.title().replace("_", ""))
        except Exception as e:
            logger.error(f"Failed to load {plugin_type} plugin '{plugin_name}': {e}")
            raise

    def run(self):
        for signal_name, signal_config in self.config["signals"].items():
            logger.info(f"Processing signal: {signal_name}")
            
            try:
                # Get secrets first
                secrets = self.secrets_manager.get_secrets(
                    signal_config.get("secrets", [])
                )
                
                # Prepare extractor params with secrets
                extractor_params = signal_config.get("extractor_params", {}).copy()
                
                # Map secrets to parameters based on config
                secret_mapping = signal_config.get("secret_mapping", {})
                for param_name, secret_name in secret_mapping.items():
                    if secret_name in secrets:
                        extractor_params[param_name] = secrets[secret_name]
                
                # Load plugins
                extractor = self._load_plugin("extract", signal_config["extractor"])(
                    **extractor_params
                )
                transformer = self._load_plugin("transform", signal_config["transformer"])()
                
                # ETL process
                raw_data = extractor.fetch()
                transformed_data = transformer.transform(raw_data)
                transformed_data.update({"signal_name": signal_name})
                
                # Load data
                loaders = [
                    SupabaseLoader(
                        url=os.getenv("SUPABASE_URL"),
                        key=os.getenv("SUPABASE_KEY")
                    ),
                    GoogleSheetsLoader(
                        creds_path="credentials.json",
                        sheet_name="Financial Signals"
                    )
                ]

                for loader in loaders:
                    try:
                        loader.load(transformed_data)
                    except LoadError as e:
                        logger.error(f"Loader failed: {e}")
                
                logger.info(f"Processed {signal_name}: {transformed_data}")
                
            except Exception as e:
                logger.error(f"Failed to process {signal_name}: {e}")
                continue