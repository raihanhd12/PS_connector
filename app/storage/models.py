"""
SQLAlchemy models for data storage
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.sql import func

from app.storage.database import Base


class Connection(Base):
    """Model for storing connection information for any connector type"""
    __tablename__ = "connections"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, unique=True)
    connector_type = Column(String(50), nullable=False)  # postgresql, mysql, google_sheets, etc.
    connection_params = Column(Text, nullable=False)  # Encrypted JSON
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    is_active = Column(Boolean, default=True)