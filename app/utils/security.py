from fastapi import HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

from app.config import settings

# API Key security scheme
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def validate_api_key(api_key: str = Security(api_key_header)):
    """
    Validate the API key if it's configured

    If no API key is configured in the settings, this is a no-op.
    Otherwise, it checks that the request contains a valid API key.
    """
    if settings.API_KEY and settings.API_KEY != "":
        if api_key != settings.API_KEY:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Invalid API Key"
            )
    return api_key