import os
from typing import List

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Load .env file
load_dotenv()

def parse_comma_separated_list(value: str) -> List[str]:
    """Parse a comma-separated string into a list of strings."""
    if not value:
        return ["*"]
    return [item.strip() for item in value.split(",")]

class Settings(BaseSettings):
    PROJECT_NAME: str = "Database Connector Storage Service"

    # CORS
    CORS_ORIGINS: List[str] = ["*"]

    # Internal Storage Database (SQLite by default, can be changed to other DB in production)
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./connector_storage.db")

    # Security
    API_KEY: str = os.getenv("API_KEY", "")  # Optional API key for protection
    SECRET_KEY: str = os.getenv("SECRET_KEY", "default-insecure-key-please-change-in-production")

    # Fixed boolean parsing
    ENCRYPT_CONFIG: bool = os.getenv("ENCRYPT_CONFIG", "True").lower() in ("true", "1", "t", "yes")

    # Fixed boolean parsing
    DEBUG: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "t", "yes")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Override model init to handle CORS_ORIGINS
    def model_post_init(self, __context):
        cors_origins_str = os.getenv("CORS_ORIGINS")
        if cors_origins_str:
            self.CORS_ORIGINS = parse_comma_separated_list(cors_origins_str)

    class Config:
        case_sensitive = True

# Create settings instance
settings = Settings()