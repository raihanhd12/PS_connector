"""
PostgreSQL connector implementation
"""
from typing import Dict, Any, List, Optional
from sqlalchemy import create_engine, inspect, text

from app.connectors.base import BaseConnector
from app.connectors.registry import ConnectorRegistry


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL connector implementation"""
    
    @classmethod
    def connector_type(cls) -> str:
        return "postgresql"
    
    @classmethod
    def connector_name(cls) -> str:
        return "PostgreSQL"
    
    @classmethod
    def connector_description(cls) -> str:
        return "Connect to PostgreSQL databases"
    
    @classmethod
    def validate_connection_params(cls, connection_params: Dict[str, Any]) -> bool:
        """
        Validate connection parameters for PostgreSQL
        
        Args:
            connection_params: Parameters to validate
            
        Returns:
            bool: True if parameters are valid
            
        Raises:
            ValueError: If parameters are invalid
        """
        required_params = ["connection_string"]
        
        for param in required_params:
            if param not in connection_params:
                raise ValueError(f"Missing required parameter: {param}")
        
        # Ensure connection string starts with postgresql://
        conn_str = connection_params["connection_string"]
        if not conn_str.startswith("postgresql://") and not conn_str.startswith("postgres://"):
            raise ValueError("PostgreSQL connection string must start with postgresql:// or postgres://")
        
        return True
    
    @classmethod
    def test_connection(cls, connection_params: Dict[str, Any]) -> bool:
        """
        Test connection to PostgreSQL
        
        Args:
            connection_params: Parameters for the connection
            
        Returns:
            bool: True if connection successful
            
        Raises:
            Exception: If connection fails
        """
        cls.validate_connection_params(connection_params)
        
        try:
            connection_string = connection_params["connection_string"]
            engine = create_engine(connection_string)
            
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            return True
            
        except Exception as e:
            raise Exception(f"Failed to connect to PostgreSQL: {str(e)}")
    
    @classmethod
    def get_metadata(cls, connection_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get metadata about PostgreSQL database
        
        Args:
            connection_params: Parameters for the connection
            
        Returns:
            Dict[str, Any]: Database metadata
            
        Raises:
            Exception: If metadata retrieval fails
        """
        cls.validate_connection_params(connection_params)
        
        try:
            connection_string = connection_params["connection_string"]
            engine = create_engine(connection_string)
            
            with engine.connect() as conn:
                # Get PostgreSQL version
                version_result = conn.execute(text("SELECT version()")).scalar()
                
                # Get database name
                db_name_result = conn.execute(text("SELECT current_database()")).scalar()
                
                # Get table count
                inspector = inspect(engine)
                schemas = inspector.get_schema_names()
                table_count = 0
                
                for schema in schemas:
                    if schema not in ('information_schema', 'pg_catalog'):
                        table_count += len(inspector.get_table_names(schema=schema))
            
            return {
                "type": "postgresql",
                "version": version_result,
                "database": db_name_result,
                "table_count": table_count
            }
            
        except Exception as e:
            raise Exception(f"Failed to get PostgreSQL metadata: {str(e)}")
    
    @classmethod
    def get_schema(cls, connection_params: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        """
        Get schema information from PostgreSQL
        
        Args:
            connection_params: Parameters for the connection
            **kwargs: Additional parameters:
                - schema_name: Filter by schema name
                - table_name: Filter by table name
            
        Returns:
            List[Dict[str, Any]]: Schema information
            
        Raises:
            Exception: If schema retrieval fails
        """
        cls.validate_connection_params(connection_params)
        
        try:
            schema_name = kwargs.get("schema_name")
            table_name = kwargs.get("table_name")
            connection_string = connection_params["connection_string"]
            
            engine = create_engine(connection_string)
            inspector = inspect(engine)
            
            result = []
            
            # Get schema names
            schemas = [schema_name] if schema_name else inspector.get_schema_names()
            
            for schema in schemas:
                if schema in ('information_schema', 'pg_catalog'):
                    continue  # Skip system schemas
                
                # Get table names
                tables = [table_name] if table_name else inspector.get_table_names(schema=schema)
                
                for table in tables:
                    # Skip if specific table_name was provided but doesn't match
                    if table_name and table != table_name:
                        continue
                    
                    # Get columns
                    columns = []
                    for column in inspector.get_columns(table, schema=schema):
                        columns.append({
                            "name": column["name"],
                            "type": str(column["type"]),
                            "nullable": column.get("nullable", True)
                        })
                    
                    # Get primary keys
                    pk_constraint = inspector.get_pk_constraint(table, schema=schema)
                    primary_keys = pk_constraint.get("constrained_columns", []) if pk_constraint else []
                    
                    # Get foreign keys
                    foreign_keys = []
                    for fk in inspector.get_foreign_keys(table, schema=schema):
                        foreign_keys.append({
                            "name": fk.get("name"),
                            "referred_schema": fk.get("referred_schema"),
                            "referred_table": fk.get("referred_table"),
                            "referred_columns": fk.get("referred_columns"),
                            "constrained_columns": fk.get("constrained_columns")
                        })
                    
                    result.append({
                        "schema": schema,
                        "table": table,
                        "columns": columns,
                        "primary_keys": primary_keys,
                        "foreign_keys": foreign_keys
                    })
            
            return result
            
        except Exception as e:
            raise Exception(f"Failed to get PostgreSQL schema: {str(e)}")


# Register the connector
ConnectorRegistry.register(PostgreSQLConnector)