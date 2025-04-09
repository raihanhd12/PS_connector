from dotenv import dotenv_values

# Load environment variables from .env file
config = dotenv_values(".env")

DEBUG=config.get("DEBUG", "False").lower() == "true"

LOG_LEVEL=config.get("LOG_LEVEL", "INFO").upper()

DATABASE_URL=config.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in the environment variables.")

SECRET_KEY=config.get("SECRET_KEY", "")
if not SECRET_KEY:
    raise ValueError("SECRET_KEY is not set in the environment variables.")

API_KEY=config.get("API_KEY", "")
if not API_KEY:
    raise ValueError("API_KEY is not set in the environment variables.")

ENCRYPT_CONFIG=config.get("ENCRYPT_CONFIG", "False").lower() == "true"

CORS_ORIGINS=config.get("CORS_ORIGINS", "*")