import os
from typing import List

from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Manual parsing untuk nilai boolean
def parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).lower() in ("true", "1", "t", "yes")

class Settings:
    def __init__(self):
        self.PROJECT_NAME = "Database Connector Storage Service"
        self.CORS_ORIGINS = ["*"]
        self.DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./connector_storage.db")
        self.API_KEY = os.getenv("API_KEY", "")
        self.SECRET_KEY = os.getenv("SECRET_KEY", "default-insecure-key-please-change-in-production")
        self.ENCRYPT_CONFIG = parse_bool(os.getenv("ENCRYPT_CONFIG"), True)
        self.DEBUG = parse_bool(os.getenv("DEBUG"), False)
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Create settings instance
settings = Settings()