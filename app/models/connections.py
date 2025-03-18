"""
Pydantic models for connection endpoints
"""
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List


class SpreadsheetConnectionCreate(BaseModel):
    """Model for creating a spreadsheet connection"""
    name: str = Field(..., description="Friendly name for this connection")
    credentials: Dict[str, Any] = Field(..., description="Google service account credentials")
    spreadsheet_id: Optional[str] = Field(None, description="Optional default spreadsheet ID")
    description: Optional[str] = Field(None, description="Optional description")


class SpreadsheetConnectionResponse(BaseModel):
    """Model for spreadsheet connection response"""
    id: int
    name: str
    spreadsheet_id: Optional[str] = None
    description: Optional[str] = None
    created_at: str


class SpreadsheetConnectionList(BaseModel):
    """Model for listing spreadsheet connections"""
    connections: List[SpreadsheetConnectionResponse]


class SpreadsheetConnectionTest(BaseModel):
    """Model for testing a spreadsheet connection"""
    connection_id: Optional[int] = Field(None, description="ID of stored connection")
    credentials: Optional[Dict[str, Any]] = Field(None, description="Or provide credentials directly")
    spreadsheet_id: str = Field(..., description="Spreadsheet ID to test")


class DatabaseConnectionCreate(BaseModel):
    """Model for creating a database connection"""
    name: str = Field(..., description="Friendly name for this connection")
    connection_string: str = Field(..., description="Database connection string")
    db_type: str = Field(..., description="Database type (postgresql, mysql, etc.)")
    description: Optional[str] = Field(None, description="Optional description")


class DatabaseConnectionResponse(BaseModel):
    """Model for database connection response"""
    id: int
    name: str
    db_type: str
    description: Optional[str] = None
    created_at: str


class DatabaseConnectionList(BaseModel):
    """Model for listing database connections"""
    connections: List[DatabaseConnectionResponse]


class DatabaseConnectionTest(BaseModel):
    """Model for testing a database connection"""
    connection_id: Optional[int] = Field(None, description="ID of stored connection")
    connection_string: Optional[str] = Field(None, description="Or provide connection string directly")


class ConnectionTestResponse(BaseModel):
    """Model for connection test response"""
    success: bool
    message: str