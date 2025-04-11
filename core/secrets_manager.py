import os
from dotenv import load_dotenv
from utils.exceptions import MissingSecretError

class SecretsManager:
    def __init__(self):
        load_dotenv()

    def get_secrets(self, required_secrets: list[str]) -> dict[str, str]:
        secrets = {}
        for secret_name in required_secrets:
            secret_value = os.getenv(secret_name)
            if not secret_value:
                raise MissingSecretError(f"Missing secret: {secret_name}")
            secrets[secret_name] = secret_value
        return secrets