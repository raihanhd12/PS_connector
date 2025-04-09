from typing import Any, Dict, Optional

from fastapi import HTTPException
from pydantic import ValidationError

from app.models.connector import (GoogleSheetsConfig, MongoDBConfig,
                                  MySQLConfig, PostgreSQLConfig, RedisConfig)

# Map connector types to their config models
CONFIG_VALIDATORS = {
    "mysql": MySQLConfig,
    "postgresql": PostgreSQLConfig,
    "mongodb": MongoDBConfig,
    "redis": RedisConfig,
    "googlesheets": GoogleSheetsConfig,
    # Add more as needed
}

def validate_connector_config(connector_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates that the provided config matches the expected schema for the connector type.

    Args:
        connector_type: Type of connector (mysql, postgresql, etc.)
        config: Configuration dictionary to validate

    Returns:
        The validated config dict (may have default values added)

    Raises:
        HTTPException: If the config fails validation
    """
    # Get validator for this connector type
    validator = CONFIG_VALIDATORS.get(connector_type.lower())

    if not validator:
        # If we don't have a specific validator, just return the config as-is
        # (useful for custom or not-yet-defined connector types)
        return config

    try:
        # Validate the config
        validated_config = validator(**config)
        # Return as dict
        return validated_config.model_dump()
    except ValidationError as e:
        # Convert validation error to HTTP exception with details
        error_details = []
        for error in e.errors():
            field = ".".join(str(x) for x in error["loc"])
            error_details.append(f"{field}: {error['msg']}")

        error_message = f"Invalid configuration for {connector_type} connector: {', '.join(error_details)}"
        raise HTTPException(status_code=400, detail=error_message)