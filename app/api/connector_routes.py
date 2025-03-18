"""
API routes for connector operations
"""
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import BaseModel, Field

from app.connectors.registry import ConnectorRegistry
from app.storage.database import get_db
from app.storage.repository import ConnectionRepository

router = APIRouter(prefix="/api/connectors", tags=["Connectors"])


class ConnectorInfo(BaseModel):
    """Model for connector information"""
    type: str
    name: str
    description: str


class ConnectorTestRequest(BaseModel):
    """Model for testing a connector"""
    connector_type: str = Field(..., description="Type of connector to test")
    connection_params: Dict[str, Any] = Field(..., description="Connection parameters")


class ConnectorTestResponse(BaseModel):
    """Model for connector test response"""
    success: bool
    message: str


class ConnectorMetadataRequest(BaseModel):
    """Model for getting connector metadata"""
    connector_type: str = Field(..., description="Type of connector")
    connection_params: Dict[str, Any] = Field(..., description="Connection parameters")


class ConnectorSchemaRequest(BaseModel):
    """Model for getting connector schema"""
    connector_type: str = Field(..., description="Type of connector")
    connection_params: Dict[str, Any] = Field(..., description="Connection parameters")
    params: Optional[Dict[str, Any]] = Field(default={}, description="Additional parameters")


@router.get("/types", response_model=List[ConnectorInfo])
def list_connector_types():
    """List all available connector types"""
    try:
        connectors = ConnectorRegistry.get_connector_info()
        return connectors
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list connector types: {str(e)}")


@router.post("/test", response_model=ConnectorTestResponse)
def test_connector(request: ConnectorTestRequest):
    """Test a connector connection"""
    try:
        connector_class = ConnectorRegistry.get_connector(request.connector_type)
        connector_class.test_connection(request.connection_params)
        
        return {
            "success": True,
            "message": f"{connector_class.connector_name()} connection successful"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        return {
            "success": False,
            "message": f"Connection failed: {str(e)}"
        }


@router.post("/metadata", response_model=Dict[str, Any])
def get_connector_metadata(request: ConnectorMetadataRequest):
    """Get metadata about a data source"""
    try:
        connector_class = ConnectorRegistry.get_connector(request.connector_type)
        metadata = connector_class.get_metadata(request.connection_params)
        return metadata
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get metadata: {str(e)}")


@router.post("/schema", response_model=List[Dict[str, Any]])
def get_connector_schema(request: ConnectorSchemaRequest):
    """Get schema information from a data source"""
    try:
        connector_class = ConnectorRegistry.get_connector(request.connector_type)
        schema = connector_class.get_schema(
            request.connection_params,
            **request.params
        )
        return schema
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get schema: {str(e)}")


# Create a connection using a specific connector
class ConnectionCreate(BaseModel):
    """Model for creating a connection"""
    name: str = Field(..., description="Friendly name for this connection")
    connector_type: str = Field(..., description="Type of connector")
    connection_params: Dict[str, Any] = Field(..., description="Connection parameters")
    description: Optional[str] = Field(None, description="Optional description")


class ConnectionResponse(BaseModel):
    """Model for connection response"""
    id: int
    name: str
    connector_type: str
    description: Optional[str] = None
    created_at: str


@router.post("/connections", response_model=ConnectionResponse, status_code=201)
def create_connection(connection: ConnectionCreate, db = Depends(get_db)):
    """Create a new connection"""
    try:
        # Validate the connector type and params
        connector_class = ConnectorRegistry.get_connector(connection.connector_type)
        connector_class.validate_connection_params(connection.connection_params)
        
        # Create the connection
        repo = ConnectionRepository(db)
        result = repo.create(
            name=connection.name,
            connector_type=connection.connector_type,
            connection_params=connection.connection_params,
            description=connection.description
        )
        
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create connection: {str(e)}")


@router.get("/connections", response_model=List[ConnectionResponse])
def list_connections(
    connector_type: Optional[str] = None,
    db = Depends(get_db)
):
    """List connections"""
    try:
        repo = ConnectionRepository(db)
        connections = repo.list_all(connector_type=connector_type)
        return connections
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list connections: {str(e)}")


@router.get("/connections/{connection_id}", response_model=ConnectionResponse)
def get_connection(connection_id: int, db = Depends(get_db)):
    """Get a connection by ID"""
    try:
        repo = ConnectionRepository(db)
        connection = repo.get_by_id(connection_id)
        return connection
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get connection: {str(e)}")


class ConnectionUpdate(BaseModel):
    """Model for updating a connection"""
    name: Optional[str] = Field(None, description="Friendly name for this connection")
    connection_params: Optional[Dict[str, Any]] = Field(None, description="Connection parameters")
    description: Optional[str] = Field(None, description="Optional description")


@router.patch("/connections/{connection_id}", response_model=ConnectionResponse)
def update_connection(
    connection_id: int,
    connection: ConnectionUpdate,
    db = Depends(get_db)
):
    """Update a connection"""
    try:
        repo = ConnectionRepository(db)
        
        # Get the existing connection to validate connector type with updated params
        existing = repo.get_by_id(connection_id)
        
        # Validate params if provided
        if connection.connection_params is not None:
            connector_class = ConnectorRegistry.get_connector(existing.connector_type)
            connector_class.validate_connection_params(connection.connection_params)
        
        # Update the connection
        result = repo.update(
            connection_id=connection_id,
            name=connection.name,
            connection_params=connection.connection_params,
            description=connection.description
        )
        
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=404 if "not found" in str(e) else 400, 
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update connection: {str(e)}")


@router.delete("/connections/{connection_id}", status_code=204)
def delete_connection(connection_id: int, db = Depends(get_db)):
    """Delete a connection"""
    try:
        repo = ConnectionRepository(db)
        repo.delete(connection_id)
        return None
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete connection: {str(e)}")