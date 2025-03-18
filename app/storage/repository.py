"""
Repository for database operations
"""
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from app.storage.models import Connection
from app.utils.security import encrypt_data, decrypt_data


class ConnectionRepository:
    """Repository for connections"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create(
        self, 
        name: str, 
        connector_type: str,
        connection_params: Dict[str, Any], 
        description: Optional[str] = None
    ) -> Connection:
        """
        Create a new connection
        
        Args:
            name: Friendly name for the connection
            connector_type: Type of connector (postgresql, mysql, google_sheets, etc.)
            connection_params: Connection parameters
            description: Optional description
            
        Returns:
            Connection: Created connection
            
        Raises:
            ValueError: If a connection with the same name exists
        """
        try:
            # Encrypt connection parameters
            encrypted_params = encrypt_data(connection_params)
            
            # Create connection
            connection = Connection(
                name=name,
                connector_type=connector_type,
                connection_params=encrypted_params,
                description=description
            )
            
            self.db.add(connection)
            self.db.commit()
            self.db.refresh(connection)
            
            return connection
            
        except IntegrityError:
            self.db.rollback()
            raise ValueError(f"A connection with name '{name}' already exists")
    
    def get_by_id(self, connection_id: int) -> Connection:
        """
        Get a connection by ID
        
        Args:
            connection_id: Connection ID
            
        Returns:
            Connection: Connection object
            
        Raises:
            ValueError: If connection not found
        """
        connection = self.db.query(Connection).filter(
            Connection.id == connection_id,
            Connection.is_active == True
        ).first()
        
        if not connection:
            raise ValueError(f"Connection with ID {connection_id} not found")
            
        return connection
    
    def list_all(self, connector_type: Optional[str] = None) -> List[Connection]:
        """
        List all active connections
        
        Args:
            connector_type: Filter by connector type (optional)
            
        Returns:
            List[Connection]: List of connections
        """
        query = self.db.query(Connection).filter(Connection.is_active == True)
        
        if connector_type:
            query = query.filter(Connection.connector_type == connector_type)
            
        return query.all()
    
    def update(
        self, 
        connection_id: int, 
        name: Optional[str] = None,
        connection_params: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None
    ) -> Connection:
        """
        Update a connection
        
        Args:
            connection_id: Connection ID
            name: New name (optional)
            connection_params: New connection parameters (optional)
            description: New description (optional)
            
        Returns:
            Connection: Updated connection
            
        Raises:
            ValueError: If connection not found or name exists
        """
        connection = self.get_by_id(connection_id)
        
        try:
            if name is not None:
                connection.name = name
                
            if connection_params is not None:
                connection.connection_params = encrypt_data(connection_params)
                
            if description is not None:
                connection.description = description
                
            connection.updated_at = datetime.now()
            
            self.db.commit()
            self.db.refresh(connection)
            
            return connection
            
        except IntegrityError:
            self.db.rollback()
            raise ValueError(f"A connection with name '{name}' already exists")
    
    def delete(self, connection_id: int) -> bool:
        """
        Delete a connection (soft delete)
        
        Args:
            connection_id: Connection ID
            
        Returns:
            bool: True if successful
            
        Raises:
            ValueError: If connection not found
        """
        connection = self.get_by_id(connection_id)
        
        connection.is_active = False
        connection.updated_at = datetime.now()
        
        self.db.commit()
        
        return True
    
    def get_connection_params(self, connection_id: int) -> Dict[str, Any]:
        """
        Get decrypted connection parameters
        
        Args:
            connection_id: Connection ID
            
        Returns:
            Dict[str, Any]: Decrypted connection parameters
            
        Raises:
            ValueError: If connection not found
        """
        connection = self.get_by_id(connection_id)
        
        # Decrypt connection parameters
        try:
            return decrypt_data(connection.connection_params)
        except Exception as e:
            raise ValueError(f"Failed to decrypt connection parameters: {str(e)}")