"""
API routes for connection operations (Legacy API, use connector_routes instead)
"""
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.storage.database import get_db
from app.storage.repository import ConnectionRepository
from app.connectors.registry import ConnectorRegistry

router = APIRouter(prefix="/api/connections", tags=["Legacy Connections"])

# Pydantic models for request/response
class SpreadsheetCreate(BaseModel):
    name: str
    credentials: Dict[str, Any]
    spreadsheet_id: Optional[str] = None
    description: Optional[str] = None

class SpreadsheetUpdate(BaseModel):
    name: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    spreadsheet_id: Optional[str] = None
    description: Optional[str] = None

class SpreadsheetResponse(BaseModel):
    id: int
    name: str
    spreadsheet_id: Optional[str]
    description: Optional[str]
    created_at: Any
    updated_at: Any

    class Config:
        from_attributes = True

class DatabaseCreate(BaseModel):
    name: str
    connection_string: str
    db_type: str
    description: Optional[str] = None

class DatabaseUpdate(BaseModel):
    name: Optional[str] = None
    connection_string: Optional[str] = None
    db_type: Optional[str] = None
    description: Optional[str] = None

class DatabaseResponse(BaseModel):
    id: int
    name: str
    db_type: str
    description: Optional[str]
    created_at: Any
    updated_at: Any

    class Config:
        from_attributes = True

class ConnectionTestRequest(BaseModel):
    """Request model for testing a connection"""
    connection_id: Optional[int] = None
    connection_string: Optional[str] = None  # For database connections
    credentials: Optional[Dict[str, Any]] = None  # For spreadsheet connections
    spreadsheet_id: Optional[str] = None  # For spreadsheet connections

class ConnectionTestResponse(BaseModel):
    """Response model for connection test"""
    success: bool
    message: str

# Spreadsheet routes
@router.post("/spreadsheets", response_model=SpreadsheetResponse, status_code=201)
def create_spreadsheet(
    spreadsheet: SpreadsheetCreate,
    db: Session = Depends(get_db)
):
    try:
        repo = ConnectionRepository(db)
        # Create a connection with connector_type 'google_sheets'
        connection_params = spreadsheet.credentials.copy()
        if spreadsheet.spreadsheet_id:
            connection_params["spreadsheet_id"] = spreadsheet.spreadsheet_id
            
        connection = repo.create(
            name=spreadsheet.name,
            connector_type="google_sheets",
            connection_params=connection_params,
            description=spreadsheet.description
        )
        
        # Convert to SpreadsheetResponse
        return SpreadsheetResponse(
            id=connection.id,
            name=connection.name,
            spreadsheet_id=connection_params.get("spreadsheet_id"),
            description=connection.description,
            created_at=connection.created_at,
            updated_at=connection.updated_at
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/spreadsheets", response_model=List[SpreadsheetResponse])
def list_spreadsheets(db: Session = Depends(get_db)):
    repo = ConnectionRepository(db)
    connections = repo.list_all(connector_type="google_sheets")
    
    # Convert to SpreadsheetResponse
    result = []
    for conn in connections:
        conn_params = repo.get_connection_params(conn.id)
        result.append(SpreadsheetResponse(
            id=conn.id,
            name=conn.name,
            spreadsheet_id=conn_params.get("spreadsheet_id"),
            description=conn.description,
            created_at=conn.created_at,
            updated_at=conn.updated_at
        ))
    
    return result

@router.get("/spreadsheets/{connection_id}", response_model=SpreadsheetResponse)
def get_spreadsheet(connection_id: int, db: Session = Depends(get_db)):
    try:
        repo = ConnectionRepository(db)
        conn = repo.get_by_id(connection_id)
        
        # Verify it's a Google Sheets connection
        if conn.connector_type != "google_sheets":
            raise ValueError(f"Connection {connection_id} is not a Google Sheets connection")
        
        # Get connection params
        conn_params = repo.get_connection_params(conn.id)
        
        return SpreadsheetResponse(
            id=conn.id,
            name=conn.name,
            spreadsheet_id=conn_params.get("spreadsheet_id"),
            description=conn.description,
            created_at=conn.created_at,
            updated_at=conn.updated_at
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.patch("/spreadsheets/{connection_id}", response_model=SpreadsheetResponse)
def update_spreadsheet(
    connection_id: int,
    spreadsheet: SpreadsheetUpdate,
    db: Session = Depends(get_db)
):
    try:
        repo = ConnectionRepository(db)
        conn = repo.get_by_id(connection_id)
        
        # Verify it's a Google Sheets connection
        if conn.connector_type != "google_sheets":
            raise ValueError(f"Connection {connection_id} is not a Google Sheets connection")
        
        # Get existing connection params
        conn_params = repo.get_connection_params(conn.id)
        
        # Update connection params
        if spreadsheet.credentials is not None:
            conn_params.update(spreadsheet.credentials)
        
        if spreadsheet.spreadsheet_id is not None:
            conn_params["spreadsheet_id"] = spreadsheet.spreadsheet_id
        
        # Update connection
        conn = repo.update(
            connection_id=connection_id,
            name=spreadsheet.name,
            connection_params=conn_params,
            description=spreadsheet.description
        )
        
        return SpreadsheetResponse(
            id=conn.id,
            name=conn.name,
            spreadsheet_id=conn_params.get("spreadsheet_id"),
            description=conn.description,
            created_at=conn.created_at,
            updated_at=conn.updated_at
        )
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e))

@router.delete("/spreadsheets/{connection_id}", status_code=204)
def delete_spreadsheet(connection_id: int, db: Session = Depends(get_db)):
    try:
        repo = ConnectionRepository(db)
        conn = repo.get_by_id(connection_id)
        
        # Verify it's a Google Sheets connection
        if conn.connector_type != "google_sheets":
            raise ValueError(f"Connection {connection_id} is not a Google Sheets connection")
        
        repo.delete(connection_id)
        return None
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# Database routes
@router.post("/databases", response_model=DatabaseResponse, status_code=201)
def create_database(
    database: DatabaseCreate,
    db: Session = Depends(get_db)
):
    try:
        repo = ConnectionRepository(db)
        # Create a connection with the specified db_type
        connection_params = {
            "connection_string": database.connection_string
        }
            
        connection = repo.create(
            name=database.name,
            connector_type=database.db_type,
            connection_params=connection_params,
            description=database.description
        )
        
        # Convert to DatabaseResponse
        return DatabaseResponse(
            id=connection.id,
            name=connection.name,
            db_type=connection.connector_type,
            description=connection.description,
            created_at=connection.created_at,
            updated_at=connection.updated_at
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/databases", response_model=List[DatabaseResponse])
def list_databases(db: Session = Depends(get_db)):
    repo = ConnectionRepository(db)
    # Get all connections except Google Sheets
    all_connections = repo.list_all()
    
    # Filter to only db connections (everything except google_sheets)
    connections = [conn for conn in all_connections if conn.connector_type != "google_sheets"]
    
    # Convert to DatabaseResponse
    result = []
    for conn in connections:
        result.append(DatabaseResponse(
            id=conn.id,
            name=conn.name,
            db_type=conn.connector_type,
            description=conn.description,
            created_at=conn.created_at,
            updated_at=conn.updated_at
        ))
    
    return result

@router.get("/databases/{connection_id}", response_model=DatabaseResponse)
def get_database(connection_id: int, db: Session = Depends(get_db)):
    try:
        repo = ConnectionRepository(db)
        conn = repo.get_by_id(connection_id)
        
        # Verify it's not a Google Sheets connection
        if conn.connector_type == "google_sheets":
            raise ValueError(f"Connection {connection_id} is not a database connection")
        
        return DatabaseResponse(
            id=conn.id,
            name=conn.name,
            db_type=conn.connector_type,
            description=conn.description,
            created_at=conn.created_at,
            updated_at=conn.updated_at
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.patch("/databases/{connection_id}", response_model=DatabaseResponse)
def update_database(
    connection_id: int,
    database: DatabaseUpdate,
    db: Session = Depends(get_db)
):
    try:
        repo = ConnectionRepository(db)
        conn = repo.get_by_id(connection_id)
        
        # Verify it's not a Google Sheets connection
        if conn.connector_type == "google_sheets":
            raise ValueError(f"Connection {connection_id} is not a database connection")
        
        # Get existing connection params
        conn_params = repo.get_connection_params(conn.id)
        
        # Update connection params
        if database.connection_string is not None:
            conn_params["connection_string"] = database.connection_string
        
        # Update connection (we don't allow changing the db_type)
        conn = repo.update(
            connection_id=connection_id,
            name=database.name,
            connection_params=conn_params,
            description=database.description
        )
        
        return DatabaseResponse(
            id=conn.id,
            name=conn.name,
            db_type=conn.connector_type,
            description=conn.description,
            created_at=conn.created_at,
            updated_at=conn.updated_at
        )
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e))

@router.delete("/databases/{connection_id}", status_code=204)
def delete_database(connection_id: int, db: Session = Depends(get_db)):
    try:
        repo = ConnectionRepository(db)
        conn = repo.get_by_id(connection_id)
        
        # Verify it's not a Google Sheets connection
        if conn.connector_type == "google_sheets":
            raise ValueError(f"Connection {connection_id} is not a database connection")
        
        repo.delete(connection_id)
        return None
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# Connection testing routes
@router.post("/test/database", response_model=ConnectionTestResponse)
def test_database_connection(
    request: ConnectionTestRequest,
    db: Session = Depends(get_db)
):
    """Test a database connection"""
    try:
        connection_params = {}
        
        # Get connection params from repository or use provided ones
        if request.connection_id is not None:
            repo = ConnectionRepository(db)
            conn = repo.get_by_id(request.connection_id)
            
            # Verify it's not a Google Sheets connection
            if conn.connector_type == "google_sheets":
                raise ValueError(f"Connection {request.connection_id} is not a database connection")
            
            connection_params = repo.get_connection_params(request.connection_id)
            connector_type = conn.connector_type
        elif request.connection_string is not None:
            # Try to determine the connector type from the connection string
            if "postgresql://" in request.connection_string or "postgres://" in request.connection_string:
                connector_type = "postgresql"
            elif "mysql://" in request.connection_string or "mysql+pymysql://" in request.connection_string:
                connector_type = "mysql"
            else:
                raise ValueError("Unable to determine database type from connection string")
            
            connection_params = {"connection_string": request.connection_string}
        else:
            raise HTTPException(
                status_code=400,
                detail="Either connection_id or connection_string must be provided"
            )
        
        # Test connection
        connector_class = ConnectorRegistry.get_connector(connector_type)
        connector_class.test_connection(connection_params)
        
        return {
            "success": True,
            "message": f"{connector_class.connector_name()} connection successful"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        return {
            "success": False,
            "message": f"Connection failed: {str(e)}"
        }

@router.post("/test/spreadsheet", response_model=ConnectionTestResponse)
def test_spreadsheet_connection(
    request: ConnectionTestRequest,
    db: Session = Depends(get_db)
):
    """Test a Google Sheets connection"""
    try:
        connection_params = {}
        
        # Get connection params from repository or use provided ones
        if request.connection_id is not None:
            repo = ConnectionRepository(db)
            conn = repo.get_by_id(request.connection_id)
            
            # Verify it's a Google Sheets connection
            if conn.connector_type != "google_sheets":
                raise ValueError(f"Connection {request.connection_id} is not a Google Sheets connection")
            
            connection_params = repo.get_connection_params(request.connection_id)
        elif request.credentials is not None:
            connection_params = {"credentials": request.credentials}
            
            if request.spreadsheet_id:
                connection_params["spreadsheet_id"] = request.spreadsheet_id
        else:
            raise HTTPException(
                status_code=400,
                detail="Either connection_id or credentials must be provided"
            )
        
        # Test connection
        connector_class = ConnectorRegistry.get_connector("google_sheets")
        connector_class.test_connection(connection_params)
        
        return {
            "success": True,
            "message": "Google Sheets connection successful"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        return {
            "success": False,
            "message": f"Connection failed: {str(e)}"
        }