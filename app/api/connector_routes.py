from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional

from app.models.connector import Connector, ConnectorCreate, ConnectorUpdate
from app.storage.repository import ConnectorRepository

router = APIRouter(tags=["connectors"])

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