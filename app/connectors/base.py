"""
Base connector interface for the connector service
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional


class BaseConnector(ABC):
    """Base interface for all connectors"""
    
    @classmethod
    @abstractmethod
    def connector_type(cls) -> str:
        """Return the type of connector"""
        pass
    
    @classmethod
    @abstractmethod
    def connector_name(cls) -> str:
        """Return the name of connector"""
        pass
    
    @classmethod
    @abstractmethod
    def connector_description(cls) -> str:
        """Return the description of connector"""
        pass
    
    @classmethod
    @abstractmethod
    def test_connection(cls, connection_params: Dict[str, Any]) -> bool:
        """
        Test the connection
        
        Args:
            connection_params: Parameters needed for the connection
            
        Returns:
            bool: True if connection successful
            
        Raises:
            Exception: If connection fails
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_metadata(cls, connection_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get metadata about the data source
        
        Args:
            connection_params: Parameters needed for the connection
            
        Returns:
            Dict[str, Any]: Metadata about the data source
            
        Raises:
            Exception: If metadata retrieval fails
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_schema(cls, connection_params: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        """
        Get schema information from the data source
        
        Args:
            connection_params: Parameters needed for the connection
            **kwargs: Additional parameters for specific implementations
            
        Returns:
            List[Dict[str, Any]]: Schema information
            
        Raises:
            Exception: If schema retrieval fails
        """
        pass
    
    @classmethod
    @abstractmethod
    def validate_connection_params(cls, connection_params: Dict[str, Any]) -> bool:
        """
        Validate connection parameters
        
        Args:
            connection_params: Parameters to validate
            
        Returns:
            bool: True if parameters are valid
            
        Raises:
            ValueError: If parameters are invalid
        """
        pass