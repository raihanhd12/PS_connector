"""
Registry for connector implementations
"""
from typing import Dict, Type, List

from app.connectors.base import BaseConnector


class ConnectorRegistry:
    """Registry for connector implementations"""
    
    _connectors: Dict[str, Type[BaseConnector]] = {}
    
    @classmethod
    def register(cls, connector_class: Type[BaseConnector]):
        """
        Register a connector implementation
        
        Args:
            connector_class: Connector class to register
        """
        connector_type = connector_class.connector_type()
        cls._connectors[connector_type] = connector_class
    
    @classmethod
    def get_connector(cls, connector_type: str) -> Type[BaseConnector]:
        """
        Get a connector by type
        
        Args:
            connector_type: Type of connector
            
        Returns:
            Type[BaseConnector]: Connector class
            
        Raises:
            ValueError: If connector type is not registered
        """
        if connector_type not in cls._connectors:
            raise ValueError(f"Connector type '{connector_type}' is not registered")
        
        return cls._connectors[connector_type]
    
    @classmethod
    def list_connector_types(cls) -> List[str]:
        """
        List all registered connector types
        
        Returns:
            List[str]: List of connector types
        """
        return list(cls._connectors.keys())
    
    @classmethod
    def get_connector_info(cls) -> List[Dict[str, str]]:
        """
        Get information about all registered connectors
        
        Returns:
            List[Dict[str, str]]: List of connector information
        """
        result = []
        for connector_type, connector_class in cls._connectors.items():
            result.append({
                "type": connector_type,
                "name": connector_class.connector_name(),
                "description": connector_class.connector_description()
            })
        
        return result