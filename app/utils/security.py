from dotenv import dotenv_values
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

# Membaca isi file .env
config = dotenv_values(".env")

API_KEY = config.get("API_KEY", "")

# API Key security scheme
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def validate_api_key(api_key: str = Security(api_key_header)):
    """
    Validate the API key if it's configured

    If no API key is configured in the settings, this is a no-op.
    Otherwise, it checks that the request contains a valid API key.
    """
    if API_KEY and API_KEY != "":
        if api_key != API_KEY:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Invalid API Key"
            )
    return api_key