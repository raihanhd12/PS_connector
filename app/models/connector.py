from typing import Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field
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

# Add more connector configs as needed