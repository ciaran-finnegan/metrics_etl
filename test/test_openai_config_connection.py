import pytest
import yaml
import os
import re
import openai
from dotenv import load_dotenv
import sys

# Add project root to sys.path to allow finding config/signals.yaml relative to project root
# Adjust the number of '..' based on where the test runner executes from.
# If running pytest from the project root, this might not be strictly needed, 
# but it helps ensure robustness.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Load environment variables from .env file in the project root
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

# --- Copy _process_env_vars function from core/pipeline.py ---
# This ensures we use the *exact* same logic as the ETLEngine
def _process_env_vars(config_data):
    """
    Recursively process configuration data to replace environment variable templates.
    Supports both {{ VAR_NAME }} and ${VAR_NAME} formats.
    """
    if isinstance(config_data, dict):
        return {k: _process_env_vars(v) for k, v in config_data.items()}
    elif isinstance(config_data, list):
        return [_process_env_vars(item) for item in config_data]
    elif isinstance(config_data, str):
        # Handle {{ VAR_NAME }} format (allowing spaces around variable)
        pattern_curly = r'\{\{\s*([A-Za-z0-9_]+)\s*\}\}'
        result = config_data
        matches_curly = re.findall(pattern_curly, result)
        for var_name in matches_curly:
            env_value = os.getenv(var_name)
            if env_value is not None: # Check if env var exists
                 # Replace both spaced and non-spaced versions defensively
                result = result.replace(f'{{{{ {var_name} }}}}', env_value) 
                result = result.replace(f'{{{{{var_name}}}}}', env_value) 
            else:
                 print(f"Warning: Environment variable '{var_name}' not found for template in config.")

        # Handle ${VAR_NAME} format
        pattern_dollar = r'\$\{([A-Za-z0-9_]+)\}'
        matches_dollar = re.findall(pattern_dollar, result)
        for var_name in matches_dollar:
            env_value = os.getenv(var_name)
            if env_value is not None: # Check if env var exists
                result = result.replace(f'${{{var_name}}}', env_value)
            else:
                print(f"Warning: Environment variable '{var_name}' not found for template in config.")
        
        return result
    else:
        return config_data

# --- Test Function ---
def test_openai_api_connection_from_config():
    """
    Tests OpenAI API connection using the key resolved from config/signals.yaml 
    and environment variable processing.
    """
    config_path = os.path.join(project_root, "config/signals.yaml")
    assert os.path.exists(config_path), f"Config file not found at {config_path}"

    print(f"Loading config from: {config_path}")
    with open(config_path, 'r') as f:
        raw_config = yaml.safe_load(f)

    # Process environment variables in the loaded config using the copied function
    print("Processing environment variables in config...")
    processed_config = _process_env_vars(raw_config)
    print("Finished processing environment variables.")

    # Extract the API key from a relevant section 
    # Using financial_tweets_openai transformer as an example
    api_key = None
    try:
        # Adjust path if your config structure is different or you want to test another signal
        api_key = processed_config['signals']['financial_tweets_openai']['transformer']['params']['api_key']
        print("Successfully extracted API key path from config.")
    except KeyError as e:
        pytest.fail(f"Could not find OpenAI API key path in processed config. Missing key: {e}")
    except TypeError as e:
         pytest.fail(f"Error accessing processed config - potentially None or wrong type: {e}. Processed config: {processed_config}")


    assert api_key, "OpenAI API key is empty after processing config."
    assert isinstance(api_key, str), f"Expected API key to be a string, but got {type(api_key)}"
    assert "{{" not in api_key, "OpenAI API key template {{...}} was not replaced."
    assert "${" not in api_key, "OpenAI API key template ${...} was not replaced."

    print(f"Found API Key (masked): {api_key[:5]}...{api_key[-4:]}")

    # Initialize OpenAI client and test connection
    print("Initializing OpenAI client...")
    try:
        client = openai.OpenAI(api_key=api_key)
        # Make a simple API call to test connection/authentication (listing models is cheap)
        print("Attempting to list OpenAI models...")
        models = client.models.list()
        assert models, "API call to list models returned empty result."
        assert hasattr(models, 'data') and len(models.data) > 0, "No models returned from API."
        print(f"Successfully connected and listed models (found {len(models.data)} models).")
        print("OpenAI API connection successful using config-resolved key.")

    except openai.AuthenticationError:
        pytest.fail("OpenAI AuthenticationError: Check API key validity and permissions.")
    except openai.RateLimitError:
         pytest.fail("OpenAI RateLimitError: Check your usage limits and billing.")
    except openai.APIConnectionError as e:
        pytest.fail(f"OpenAI APIConnectionError: Check network connectivity to api.openai.com. Details: {e}")
    except Exception as e:
        pytest.fail(f"OpenAI API connection failed unexpectedly: {type(e).__name__} - {e}") 