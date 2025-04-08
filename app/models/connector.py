from typing import Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, model_validator
import uuid

class ConnectorBase(BaseModel):
    """Base model for connector information"""
    name: str
    type: str  # mysql, postgresql, mongodb, etc.
    config: Dict[str, Any]  # Connection details (host, port, user, password, etc.)
    description: Optional[str] = None

class ConnectorCreate(ConnectorBase):
    """Model for creating a new connector"""
    pass

class ConnectorUpdate(BaseModel):
    """Model for updating an existing connector"""
    name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None

class ConnectorInDB(ConnectorBase):
    """Model for connector stored in the database"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class Connector(ConnectorInDB):
    """Full connector model returned in API responses"""
    pass

# Configuration models for specific connector types
class MySQLConfig(BaseModel):
    """MySQL connection configuration"""
    host: str
    port: int = 3306
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"

class PostgreSQLConfig(BaseModel):
    """PostgreSQL connection configuration"""
    host: str
    port: int = 5432
    user: str
    password: str
    database: str
    sslmode: str = "prefer"

class MongoDBConfig(BaseModel):
    """MongoDB connection configuration"""
    uri: str
    database: str
    auth_source: Optional[str] = None
    auth_mechanism: Optional[str] = None

class RedisConfig(BaseModel):
    """Redis connection configuration"""
    host: str
    port: int = 6379
    password: Optional[str] = None
    db: int = 0
    ssl: bool = False

class GoogleSheetsConfig(BaseModel):
    """Google Sheets connection configuration - supports both OAuth and Service Account"""
    # Either OAuth credentials or service account info is required
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None

    # Service account alternative
    service_account_info: Optional[Dict[str, Any]] = None

    # Common fields
    spreadsheet_id: Optional[str] = None
    range: Optional[str] = None

    # Custom validator
    @model_validator(mode='after')
    def check_auth_method(self):
        # Check if OAuth credentials are provided
        has_oauth = all(getattr(self, k) for k in ['client_id', 'client_secret', 'access_token', 'refresh_token'])

        # Check if service account info is provided
        has_service_account = self.service_account_info is not None

        if not (has_oauth or has_service_account):
            raise ValueError("Either OAuth credentials (client_id, client_secret, access_token, refresh_token) "
                            "or service_account_info must be provided")

        return self