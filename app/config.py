import os
from pydantic_settings import BaseSettings
from typing import List
from dotenv import load_dotenv

# Load .env file
load_dotenv()

class Settings(BaseSettings):
    PROJECT_NAME: str = "Database Connector Storage Service"

    # CORS
    CORS_ORIGINS: List[str] = ["*"]

    # Internal Storage Database (SQLite by default, can be changed to other DB in production)
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./connector_storage.db")

    # Security
    API_KEY: str = os.getenv("API_KEY", "")  # Optional API key for protection
    SECRET_KEY: str = os.getenv("SECRET_KEY", "default-insecure-key-please-change-in-production")
    ENCRYPT_CONFIG: bool = os.getenv("ENCRYPT_CONFIG", "True").lower() in ("true", "1", "t")

    # Logging
    DEBUG: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    class Config:
        case_sensitive = True

# Create settings instance
settings = Settings()