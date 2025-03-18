"""
Database service for connecting and interacting with databases
"""
import pandas as pd
from typing import Dict, List, Any, Optional
from sqlalchemy import create_engine, inspect, text


class DatabaseService:
    """Service for database operations"""
    
    @staticmethod
    def test_connection(connection_string: str) -> bool:
        """
        Test connection to a database
        
        Args:
            connection_string: Database connection string
            
        Returns:
            bool: True if connection successful
            
        Raises:
            Exception: If connection test fails
        """
        try:
            # Create engine
            engine = create_engine(connection_string)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            return True
            
        except Exception as e:
            raise Exception(f"Failed to connect to database: {str(e)}")
    
    @staticmethod
    def get_tables(connection_string: str) -> List[Dict[str, str]]:
        """
        Get list of tables in a database
        
        Args:
            connection_string: Database connection string
            
        Returns:
            List[Dict]: List of tables with schema and name
            
        Raises:
            Exception: If table listing fails
        """
        try:
            # Create engine
            engine = create_engine(connection_string)
            
            # Get inspector
            inspector = inspect(engine)
            
            # Get schema names
            schemas = inspector.get_schema_names()
            
            # Get tables for each schema
            tables = []
            
            # Determine database type from URL
            is_mysql = 'mysql' in connection_string.lower()
            is_postgresql = 'postgresql' in connection_string.lower() or 'postgres' in connection_string.lower()
            
            # Handle different database types
            if is_mysql:
                # MySQL stores tables differently
                for schema in schemas:
                    if schema in ('information_schema', 'performance_schema', 'mysql', 'sys'):
                        continue  # Skip system schemas
                    
                    for table_name in inspector.get_table_names(schema=schema):
                        tables.append({
                            'schema': schema,
                            'name': table_name
                        })
            elif is_postgresql:
                # PostgreSQL handling
                for schema in schemas:
                    if schema in ('information_schema', 'pg_catalog'):
                        continue  # Skip system schemas
                    
                    for table_name in inspector.get_table_names(schema=schema):
                        tables.append({
                            'schema': schema,
                            'name': table_name
                        })
            else:
                # Generic handling for other databases
                for schema in schemas:
                    for table_name in inspector.get_table_names(schema=schema):
                        tables.append({
                            'schema': schema,
                            'name': table_name
                        })
            
            return tables
            
        except Exception as e:
            raise Exception(f"Failed to get tables: {str(e)}")
    
    @staticmethod
    def get_table_columns(connection_string: str, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get columns for a table
        
        Args:
            connection_string: Database connection string
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            List[Dict]: List of columns with name, type, and nullable
            
        Raises:
            Exception: If column retrieval fails
        """
        try:
            # Create engine
            engine = create_engine(connection_string)
            
            # Get inspector
            inspector = inspect(engine)
            
            # Get columns
            columns = inspector.get_columns(table_name, schema=schema)
            
            # Format columns
            result = []
            for col in columns:
                result.append({
                    'name': col['name'],
                    'type': str(col['type']),
                    'nullable': col.get('nullable', True)
                })
            
            return result
            
        except Exception as e:
            raise Exception(f"Failed to get table columns: {str(e)}")