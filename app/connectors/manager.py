import importlib
from typing import Dict, Any, Optional, List
import asyncio
import logging

from app.utils.encryption import decrypt_data

# Initialize logger
logger = logging.getLogger(__name__)

class ConnectorManager:
    """
    Manager for handling connector operations like testing connections and
    retrieving metadata from different data sources
    """

    @staticmethod
    async def test_connection(connector_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test a connection using the provided configuration

        Args:
            connector_type: Type of connector (mysql, postgresql, etc.)
            config: Connection configuration (already decrypted)

        Returns:
            Dict with success status and additional information
        """
        try:
            # Determine which test method to use based on connector type
            if connector_type.lower() == "mysql":
                return await ConnectorManager._test_mysql_connection(config)
            elif connector_type.lower() == "postgresql":
                return await ConnectorManager._test_postgresql_connection(config)
            elif connector_type.lower() == "mongodb":
                return await ConnectorManager._test_mongodb_connection(config)
            elif connector_type.lower() == "redis":
                return await ConnectorManager._test_redis_connection(config)
            elif connector_type.lower() == "googlesheets":
                return await ConnectorManager._test_googlesheets_connection(config)
            else:
                return {
                    "success": False,
                    "message": f"Testing not implemented for connector type: {connector_type}"
                }
        except Exception as e:
            logger.error(f"Error testing {connector_type} connection: {str(e)}")
            return {
                "success": False,
                "message": f"Connection test failed: {str(e)}"
            }

    @staticmethod
    async def get_metadata(connector_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Retrieve metadata from a data source

        Args:
            connector_type: Type of connector (mysql, postgresql, etc.)
            config: Connection configuration (already decrypted)

        Returns:
            Dict with metadata information (tables, columns, etc.)
        """
        try:
            # Determine which metadata method to use based on connector type
            if connector_type.lower() == "mysql":
                return await ConnectorManager._get_mysql_metadata(config)
            elif connector_type.lower() == "postgresql":
                return await ConnectorManager._get_postgresql_metadata(config)
            elif connector_type.lower() == "mongodb":
                return await ConnectorManager._get_mongodb_metadata(config)
            elif connector_type.lower() == "googlesheets":
                return await ConnectorManager._get_googlesheets_metadata(config)
            else:
                return {
                    "success": False,
                    "message": f"Metadata retrieval not implemented for connector type: {connector_type}"
                }
        except Exception as e:
            logger.error(f"Error getting metadata for {connector_type}: {str(e)}")
            return {
                "success": False,
                "message": f"Metadata retrieval failed: {str(e)}"
            }

    # Implementation for specific connector types
    @staticmethod
    async def _test_mysql_connection(config: Dict[str, Any]) -> Dict[str, Any]:
        """Test MySQL connection"""
        try:
            # Check if the required package is installed
            import aiomysql
        except ImportError:
            return {
                "success": False,
                "message": "MySQL support requires aiomysql package. Install with: pip install aiomysql"
            }

        try:
            conn = await aiomysql.connect(
                host=config["host"],
                port=config.get("port", 3306),
                user=config["user"],
                password=config["password"],
                db=config["database"],
                charset=config.get("charset", "utf8mb4")
            )

            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                result = await cur.fetchone()

            conn.close()

            return {
                "success": True,
                "message": "Successfully connected to MySQL database",
                "details": {
                    "host": config["host"],
                    "database": config["database"]
                }
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"MySQL connection failed: {str(e)}"
            }

    @staticmethod
    async def _test_postgresql_connection(config: Dict[str, Any]) -> Dict[str, Any]:
        """Test PostgreSQL connection"""
        try:
            # Check if the required package is installed
            import asyncpg
        except ImportError:
            return {
                "success": False,
                "message": "PostgreSQL support requires asyncpg package. Install with: pip install asyncpg"
            }

        try:
            conn = await asyncpg.connect(
                host=config["host"],
                port=config.get("port", 5432),
                user=config["user"],
                password=config["password"],
                database=config["database"],
                ssl=config.get("sslmode", "prefer") != "disable"
            )

            # Execute a simple query
            result = await conn.fetch("SELECT 1")

            await conn.close()

            return {
                "success": True,
                "message": "Successfully connected to PostgreSQL database",
                "details": {
                    "host": config["host"],
                    "database": config["database"]
                }
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"PostgreSQL connection failed: {str(e)}"
            }

    @staticmethod
    async def _test_mongodb_connection(config: Dict[str, Any]) -> Dict[str, Any]:
        """Test MongoDB connection"""
        try:
            # Check if the required package is installed
            import motor.motor_asyncio
        except ImportError:
            return {
                "success": False,
                "message": "MongoDB support requires motor package. Install with: pip install motor"
            }

        try:
            client = motor.motor_asyncio.AsyncIOMotorClient(
                config["uri"],
                authSource=config.get("auth_source"),
                authMechanism=config.get("auth_mechanism")
            )

            # Force connection to verify credentials
            await client.admin.command("ping")

            return {
                "success": True,
                "message": "Successfully connected to MongoDB",
                "details": {
                    "database": config["database"]
                }
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"MongoDB connection failed: {str(e)}"
            }

    @staticmethod
    async def _test_redis_connection(config: Dict[str, Any]) -> Dict[str, Any]:
        """Test Redis connection"""
        try:
            # Check if the required package is installed
            import redis.asyncio as redis
        except ImportError:
            return {
                "success": False,
                "message": "Redis support requires redis package. Install with: pip install redis"
            }

        try:
            client = redis.Redis(
                host=config["host"],
                port=config.get("port", 6379),
                password=config.get("password"),
                db=config.get("db", 0),
                ssl=config.get("ssl", False)
            )

            # Test connection
            await client.ping()

            # Close connection
            await client.close()

            return {
                "success": True,
                "message": "Successfully connected to Redis",
                "details": {
                    "host": config["host"],
                    "db": config.get("db", 0)
                }
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Redis connection failed: {str(e)}"
            }

    @staticmethod
    async def _test_googlesheets_connection(config: Dict[str, Any]) -> Dict[str, Any]:
        """Test Google Sheets connection"""
        try:
            # Check if the required package is installed
            from googleapiclient.discovery import build
            from google.oauth2.credentials import Credentials
        except ImportError:
            return {
                "success": False,
                "message": "Google Sheets support requires google-api-python-client. Install with: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib"
            }

        try:
            # Create credentials from the token
            credentials = Credentials(
                token=config.get("access_token"),
                refresh_token=config.get("refresh_token"),
                client_id=config.get("client_id"),
                client_secret=config.get("client_secret"),
                token_uri="https://oauth2.googleapis.com/token"
            )

            # Create the service
            service = build('sheets', 'v4', credentials=credentials)

            # Test by getting spreadsheet metadata
            if "spreadsheet_id" in config:
                # Get specific spreadsheet info if ID is provided
                result = service.spreadsheets().get(spreadsheetId=config["spreadsheet_id"]).execute()
                title = result.get("properties", {}).get("title", "Untitled")
                return {
                    "success": True,
                    "message": f"Successfully connected to Google Sheets: {title}",
                    "details": {
                        "spreadsheet_id": config["spreadsheet_id"],
                        "title": title
                    }
                }
            else:
                # Just verify the API connection works
                return {
                    "success": True,
                    "message": "Successfully authenticated with Google Sheets API",
                    "details": {}
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"Google Sheets connection failed: {str(e)}"
            }

    # Metadata retrieval methods
    @staticmethod
    async def _get_mysql_metadata(config: Dict[str, Any]) -> Dict[str, Any]:
        """Get MySQL metadata"""
        try:
            import aiomysql
        except ImportError:
            return {
                "success": False,
                "message": "MySQL support requires aiomysql package"
            }

        try:
            conn = await aiomysql.connect(
                host=config["host"],
                port=config.get("port", 3306),
                user=config["user"],
                password=config["password"],
                db=config["database"],
                charset=config.get("charset", "utf8mb4")
            )

            tables = []

            async with conn.cursor() as cur:
                # Get list of tables
                await cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_name
                """, (config["database"],))

                table_rows = await cur.fetchall()

                # For each table, get columns
                for (table_name,) in table_rows:
                    await cur.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                    """, (config["database"], table_name))

                    column_rows = await cur.fetchall()
                    columns = [{"name": col_name, "type": col_type} for col_name, col_type in column_rows]

                    tables.append({
                        "name": table_name,
                        "columns": columns
                    })

            conn.close()

            return {
                "success": True,
                "database": config["database"],
                "tables": tables
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to retrieve MySQL metadata: {str(e)}"
            }

    @staticmethod
    async def _get_postgresql_metadata(config: Dict[str, Any]) -> Dict[str, Any]:
        """Get PostgreSQL metadata"""
        try:
            import asyncpg
        except ImportError:
            return {
                "success": False,
                "message": "PostgreSQL support requires asyncpg package"
            }

        try:
            conn = await asyncpg.connect(
                host=config["host"],
                port=config.get("port", 5432),
                user=config["user"],
                password=config["password"],
                database=config["database"],
                ssl=config.get("sslmode", "prefer") != "disable"
            )

            # Get list of tables in the public schema
            tables_query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """

            table_records = await conn.fetch(tables_query)

            tables = []
            for record in table_records:
                table_name = record["table_name"]

                # Get column info for this table
                columns_query = """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = $1
                    ORDER BY ordinal_position
                """

                column_records = await conn.fetch(columns_query, table_name)
                columns = [{"name": rec["column_name"], "type": rec["data_type"]} for rec in column_records]

                tables.append({
                    "name": table_name,
                    "columns": columns
                })

            await conn.close()

            return {
                "success": True,
                "database": config["database"],
                "tables": tables
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to retrieve PostgreSQL metadata: {str(e)}"
            }

    @staticmethod
    async def _get_mongodb_metadata(config: Dict[str, Any]) -> Dict[str, Any]:
        """Get MongoDB metadata"""
        try:
            import motor.motor_asyncio
        except ImportError:
            return {
                "success": False,
                "message": "MongoDB support requires motor package"
            }

        try:
            client = motor.motor_asyncio.AsyncIOMotorClient(
                config["uri"],
                authSource=config.get("auth_source"),
                authMechanism=config.get("auth_mechanism")
            )

            db = client[config["database"]]

            # Get list of collections
            collection_names = await db.list_collection_names()

            collections = []
            for name in sorted(collection_names):
                # Get sample document to infer fields
                collection = db[name]
                sample = await collection.find_one()

                fields = []
                if sample:
                    for field_name, value in sample.items():
                        fields.append({
                            "name": field_name,
                            "type": type(value).__name__
                        })

                collections.append({
                    "name": name,
                    "fields": fields
                })

            return {
                "success": True,
                "database": config["database"],
                "collections": collections
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to retrieve MongoDB metadata: {str(e)}"
            }

    @staticmethod
    async def _get_googlesheets_metadata(config: Dict[str, Any]) -> Dict[str, Any]:
        """Get Google Sheets metadata"""
        try:
            from googleapiclient.discovery import build
            from google.oauth2.credentials import Credentials
        except ImportError:
            return {
                "success": False,
                "message": "Google Sheets support requires google-api-python-client"
            }

        try:
            # Create credentials from the token
            credentials = Credentials(
                token=config.get("access_token"),
                refresh_token=config.get("refresh_token"),
                client_id=config.get("client_id"),
                client_secret=config.get("client_secret"),
                token_uri="https://oauth2.googleapis.com/token"
            )

            # Create the service
            service = build('sheets', 'v4', credentials=credentials)

            if "spreadsheet_id" not in config:
                return {
                    "success": False,
                    "message": "Spreadsheet ID is required for metadata retrieval"
                }

            # Get the spreadsheet metadata
            spreadsheet = service.spreadsheets().get(
                spreadsheetId=config["spreadsheet_id"]
            ).execute()

            sheets = []
            for sheet in spreadsheet.get("sheets", []):
                props = sheet.get("properties", {})

                # Try to get header row if possible
                sheet_name = props.get("title", "")
                range_name = f"'{sheet_name}'!A1:Z1"

                headers = []
                try:
                    result = service.spreadsheets().values().get(
                        spreadsheetId=config["spreadsheet_id"],
                        range=range_name
                    ).execute()

                    values = result.get("values", [])
                    if values and len(values) > 0:
                        headers = values[0]
                except Exception:
                    # Just continue if we can't get headers
                    pass

                sheets.append({
                    "name": sheet_name,
                    "id": props.get("sheetId"),
                    "headers": headers,
                    "grid_properties": props.get("gridProperties", {})
                })

            return {
                "success": True,
                "spreadsheet": {
                    "id": config["spreadsheet_id"],
                    "title": spreadsheet.get("properties", {}).get("title", ""),
                    "sheets": sheets
                }
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to retrieve Google Sheets metadata: {str(e)}"
            }