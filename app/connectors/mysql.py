"""
MySQL connector implementation
"""
from typing import Dict, Any, List, Optional
from sqlalchemy import create_engine, inspect, text

from app.connectors.base import BaseConnector
from app.connectors.registry import ConnectorRegistry


class MySQLConnector(BaseConnector):
    """MySQL connector implementation"""
    
    @classmethod
    def connector_type(cls) -> str:
        return "mysql"
    
    @classmethod
    def connector_name(cls) -> str:
        return "MySQL"
    
    @classmethod
    def connector_description(cls) -> str:
        return "Connect to MySQL/MariaDB databases"
    
    @classmethod
    def validate_connection_params(cls, connection_params: Dict[str, Any]) -> bool:
        """
        Validate connection parameters for MySQL
        
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
        
        # Ensure connection string starts with mysql://
        conn_str = connection_params["connection_string"]
        if not conn_str.startswith("mysql://") and not conn_str.startswith("mysql+pymysql://"):
            raise ValueError("MySQL connection string must start with mysql:// or mysql+pymysql://")
        
        return True
    
    @classmethod
    def test_connection(cls, connection_params: Dict[str, Any]) -> bool:
        """
        Test connection to MySQL
        
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
            raise Exception(f"Failed to connect to MySQL: {str(e)}")
    
    @classmethod
    def get_metadata(cls, connection_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get metadata about MySQL database
        
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
                # Get MySQL version
                version_result = conn.execute(text("SELECT VERSION()")).scalar()
                
                # Get database name
                db_name_result = conn.execute(text("SELECT DATABASE()")).scalar()
                
                # Get table count
                table_count_result = conn.execute(text(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()"
                )).scalar()
            
            return {
                "type": "mysql",
                "version": version_result,
                "database": db_name_result,
                "table_count": table_count_result
            }
            
        except Exception as e:
            raise Exception(f"Failed to get MySQL metadata: {str(e)}")
    
    @classmethod
    def get_schema(cls, connection_params: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        """
        Get schema information from MySQL
        
        Args:
            connection_params: Parameters for the connection
            **kwargs: Additional parameters:
                - schema_name: Filter by schema name (database name in MySQL)
                - table_name: Filter by table name
            
        Returns:
            List[Dict[str, Any]]: Schema information
            
        Raises:
            Exception: If schema retrieval fails
        """
        cls.validate_connection_params(connection_params)
        
        try:
            table_name = kwargs.get("table_name")
            connection_string = connection_params["connection_string"]
            
            engine = create_engine(connection_string)
            inspector = inspect(engine)
            
            # In MySQL, schema_name is the database name
            # Get the current database name if not specified
            schema_name = kwargs.get("schema_name")
            if not schema_name:
                with engine.connect() as conn:
                    schema_name = conn.execute(text("SELECT DATABASE()")).scalar()
            
            result = []
            
            # Get table names
            tables = [table_name] if table_name else inspector.get_table_names(schema=schema_name)
            
            for table in tables:
                # Get columns
                columns = []
                for column in inspector.get_columns(table, schema=schema_name):
                    columns.append({
                        "name": column["name"],
                        "type": str(column["type"]),
                        "nullable": column.get("nullable", True)
                    })
                
                # Get primary keys
                pk_constraint = inspector.get_pk_constraint(table, schema=schema_name)
                primary_keys = pk_constraint.get("constrained_columns", []) if pk_constraint else []
                
                # Get foreign keys
                foreign_keys = []
                for fk in inspector.get_foreign_keys(table, schema=schema_name):
                    foreign_keys.append({
                        "name": fk.get("name"),
                        "referred_schema": fk.get("referred_schema"),
                        "referred_table": fk.get("referred_table"),
                        "referred_columns": fk.get("referred_columns"),
                        "constrained_columns": fk.get("constrained_columns")
                    })
                
                # Get indexes
                indexes = []
                for idx in inspector.get_indexes(table, schema=schema_name):
                    indexes.append({
                        "name": idx.get("name"),
                        "unique": idx.get("unique", False),
                        "columns": idx.get("column_names", [])
                    })
                
                result.append({
                    "schema": schema_name,
                    "table": table,
                    "columns": columns,
                    "primary_keys": primary_keys,
                    "foreign_keys": foreign_keys,
                    "indexes": indexes
                })
            
            return result
            
        except Exception as e:
            raise Exception(f"Failed to get MySQL schema: {str(e)}")


# Register the connector
ConnectorRegistry.register(MySQLConnector)