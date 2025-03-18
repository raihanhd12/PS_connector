"""
API routes for connection operations
"""
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.storage.models import ConnectorSpreadsheet, ConnectorDatabase
from app.storage.database import get_db
from app.storage.repository import SpreadsheetRepository, DatabaseRepository
from app.services.google_sheets import GoogleSheetsService
from app.services.database import DatabaseService

router = APIRouter(prefix="/api/connections", tags=["Connections"])

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
        orm_mode = True

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
        orm_mode = True

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
        repo = SpreadsheetRepository(db)
        connection = repo.create(
            name=spreadsheet.name,
            credentials=spreadsheet.credentials,
            spreadsheet_id=spreadsheet.spreadsheet_id,
            description=spreadsheet.description
        )
        return connection
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/spreadsheets", response_model=List[SpreadsheetResponse])
def list_spreadsheets(db: Session = Depends(get_db)):
    repo = SpreadsheetRepository(db)
    return repo.list_all()

@router.get("/spreadsheets/{connection_id}", response_model=SpreadsheetResponse)
def get_spreadsheet(connection_id: int, db: Session = Depends(get_db)):
    try:
        repo = SpreadsheetRepository(db)
        return repo.get_by_id(connection_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.patch("/spreadsheets/{connection_id}", response_model=SpreadsheetResponse)
def update_spreadsheet(
    connection_id: int,
    spreadsheet: SpreadsheetUpdate,
    db: Session = Depends(get_db)
):
    try:
        repo = SpreadsheetRepository(db)
        return repo.update(
            connection_id=connection_id,
            name=spreadsheet.name,
            credentials=spreadsheet.credentials,
            spreadsheet_id=spreadsheet.spreadsheet_id,
            description=spreadsheet.description
        )
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e))

@router.delete("/spreadsheets/{connection_id}", status_code=204)
def delete_spreadsheet(connection_id: int, db: Session = Depends(get_db)):
    try:
        repo = SpreadsheetRepository(db)
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
        repo = DatabaseRepository(db)
        connection = repo.create(
            name=database.name,
            connection_string=database.connection_string,
            db_type=database.db_type,
            description=database.description
        )
        return connection
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/databases", response_model=List[DatabaseResponse])
def list_databases(db: Session = Depends(get_db)):
    repo = DatabaseRepository(db)
    return repo.list_all()

@router.get("/databases/{connection_id}", response_model=DatabaseResponse)
def get_database(connection_id: int, db: Session = Depends(get_db)):
    try:
        repo = DatabaseRepository(db)
        return repo.get_by_id(connection_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.patch("/databases/{connection_id}", response_model=DatabaseResponse)
def update_database(
    connection_id: int,
    database: DatabaseUpdate,
    db: Session = Depends(get_db)
):
    try:
        repo = DatabaseRepository(db)
        return repo.update(
            connection_id=connection_id,
            name=database.name,
            connection_string=database.connection_string,
            db_type=database.db_type,
            description=database.description
        )
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e))

@router.delete("/databases/{connection_id}", status_code=204)
def delete_database(connection_id: int, db: Session = Depends(get_db)):
    try:
        repo = DatabaseRepository(db)
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
        connection_string = None
        
        # Get connection string from repository or use provided one
        if request.connection_id is not None:
            repo = DatabaseRepository(db)
            connection_string = repo.get_connection_string(request.connection_id)
        elif request.connection_string is not None:
            connection_string = request.connection_string
        else:
            raise HTTPException(
                status_code=400,
                detail="Either connection_id or connection_string must be provided"
            )
        
        # Test connection
        DatabaseService.test_connection(connection_string)
        
        return {
            "success": True,
            "message": "Database connection successful"
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
        credentials = None
        
        # Get credentials from repository or use provided ones
        if request.connection_id is not None:
            repo = SpreadsheetRepository(db)
            credentials = repo.get_credentials(request.connection_id)
        elif request.credentials is not None:
            credentials = request.credentials
        else:
            raise HTTPException(
                status_code=400,
                detail="Either connection_id or credentials must be provided"
            )
        
        # Test connection
        GoogleSheetsService.test_connection(
            credentials=credentials,
            spreadsheet_id=request.spreadsheet_id
        )
        
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