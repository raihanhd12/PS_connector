"""
Configuration settings for the Database Connector Service
"""
import os
from typing import List
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings(BaseSettings):
    """Application settings"""
    # App info
    APP_NAME: str = "Database Connector Service"
    APP_VERSION: str = "1.0.0"
    APP_DESCRIPTION: str = "API for managing connections to various data sources"

    # Environment
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    # Database URL for storing connections
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./database_connector.db")
    
    # Add SECRET_KEY for encryption
    SECRET_KEY: str = os.getenv("SECRET_KEY", "wX9l9X.IG]V.SMjXVx1AysRO:ZM5U+G)Zf33[5\h:-hu1wEKoRh1[zEh|B;5:YAG")
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    

# Create global settings object
settings = Settings()