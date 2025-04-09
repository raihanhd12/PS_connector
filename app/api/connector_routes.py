from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.connectors.manager import ConnectorManager
from app.models.connector import Connector, ConnectorCreate, ConnectorUpdate
from app.storage.repository import ConnectorRepository
from app.utils.security import validate_api_key
from app.utils.validator import validate_connector_config

router = APIRouter(tags=["connectors"], dependencies=[Depends(validate_api_key)])

@router.get("/connectors", response_model=List[Connector])
async def get_connectors(
    type: Optional[str] = Query(None, description="Filter by connector type")
):
    """
    Get all connectors or filter by type
    """
    if type:
        return await ConnectorRepository.get_by_type(type)
    return await ConnectorRepository.get_all()

@router.get("/connectors/types", response_model=Dict[str, List[str]])
async def get_connector_types():
    """
    Get all available connector types
    """
    # This should return a list of supported connector types
    # For now, we will return a hardcoded list
    return {
        "types": [
            "mysql",
            "postgresql",
            "mongodb",
            "redis",
            # Add more as needed
        ]
    }

@router.get("/connectors/{connector_id}", response_model=Connector)
async def get_connector(connector_id: str):
    """
    Get a specific connector by ID
    """
    connector = await ConnectorRepository.get_by_id(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    return connector

@router.get("/connectors/name/{name}", response_model=Connector)
async def get_connector_by_name(name: str):
    """
    Get a specific connector by name
    """
    connector = await ConnectorRepository.get_by_name(name)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    return connector

@router.post("/connectors", response_model=Connector)
async def create_connector(connector: ConnectorCreate):
    """
    Create a new connector
    """
    # Check if a connector with the same name already exists
    existing = await ConnectorRepository.get_by_name(connector.name)
    if existing:
        raise HTTPException(status_code=400, detail="A connector with this name already exists")

    # Validate the configuration
    validated_config = validate_connector_config(connector.type, connector.config)
    connector.config = validated_config

    return await ConnectorRepository.create(connector)

@router.put("/connectors/{connector_id}", response_model=Connector)
async def update_connector(connector_id: str, connector: ConnectorUpdate):
    """
    Update an existing connector
    """
    # Check if connector exists
    existing = await ConnectorRepository.get_by_id(connector_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Connector not found")

    # Check name uniqueness if name is being updated
    if connector.name and connector.name != existing.name:
        name_check = await ConnectorRepository.get_by_name(connector.name)
        if name_check:
            raise HTTPException(status_code=400, detail="A connector with this name already exists")

    # Validate the configuration if it's being updated
    if connector.config is not None:
        validated_config = validate_connector_config(existing.type, connector.config)
        connector.config = validated_config

    updated = await ConnectorRepository.update(connector_id, connector)
    return updated

@router.delete("/connectors/{connector_id}", response_model=bool)
async def delete_connector(connector_id: str):
    """
    Delete a connector
    """
    # Check if connector exists
    existing = await ConnectorRepository.get_by_id(connector_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Connector not found")

    deleted = await ConnectorRepository.delete(connector_id)
    return deleted

@router.post("/connectors/test", response_model=Dict[str, Any])
async def test_connection(connector: ConnectorCreate):
    """
    Test a connection without saving it

    Validates the configuration and attempts to connect to the data source.
    """
    # Validate the configuration
    validated_config = validate_connector_config(connector.type, connector.config)

    # Test the connection
    result = await ConnectorManager.test_connection(connector.type, validated_config)
    return result

@router.post("/connectors/{connector_id}/test", response_model=Dict[str, Any])
async def test_existing_connection(connector_id: str):
    """
    Test an existing connector

    Attempts to connect to the data source using the stored configuration.
    """
    # Get the connector
    connector = await ConnectorRepository.get_by_id(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    # Test the connection
    result = await ConnectorManager.test_connection(connector.type, connector.config)
    return result

@router.post("/connectors/{connector_id}/metadata", response_model=Dict[str, Any])
async def get_connector_metadata(connector_id: str):
    """
    Get metadata for a connector

    Retrieves schema information from the data source (tables, columns, etc.)
    """
    # Get the connector
    connector = await ConnectorRepository.get_by_id(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    # Get the metadata
    result = await ConnectorManager.get_metadata(connector.type, connector.config)
    return result

