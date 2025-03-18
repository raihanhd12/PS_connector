"""
Google Sheets connector implementation
"""
import json
import tempfile
import os
from typing import Dict, Any, List, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.connectors.base import BaseConnector
from app.connectors.registry import ConnectorRegistry
from app.config import settings


class GoogleSheetsConnector(BaseConnector):
    """Google Sheets connector implementation"""
    
    @classmethod
    def connector_type(cls) -> str:
        return "google_sheets"
    
    @classmethod
    def connector_name(cls) -> str:
        return "Google Sheets"
    
    @classmethod
    def connector_description(cls) -> str:
        return "Connect to Google Sheets spreadsheets"
    
    @classmethod
    def validate_connection_params(cls, connection_params: Dict[str, Any]) -> bool:
        """
        Validate connection parameters for Google Sheets
        
        Args:
            connection_params: Parameters to validate
            
        Returns:
            bool: True if parameters are valid
            
        Raises:
            ValueError: If parameters are invalid
        """
        required_params = ["credentials"]
        
        for param in required_params:
            if param not in connection_params:
                raise ValueError(f"Missing required parameter: {param}")
        
        # Check credentials structure
        credentials = connection_params["credentials"]
        required_credential_fields = [
            "type", "project_id", "private_key_id", "private_key",
            "client_email", "client_id", "auth_uri", "token_uri"
        ]
        
        for field in required_credential_fields:
            if field not in credentials:
                raise ValueError(f"Missing required credential field: {field}")
        
        # Validate spreadsheet_id if provided
        if "spreadsheet_id" in connection_params and not connection_params["spreadsheet_id"]:
            raise ValueError("spreadsheet_id cannot be empty if provided")
        
        return True
    
    @classmethod
    def test_connection(cls, connection_params: Dict[str, Any]) -> bool:
        """
        Test connection to Google Sheets
        
        Args:
            connection_params: Parameters for the connection
            
        Returns:
            bool: True if connection successful
            
        Raises:
            Exception: If connection fails
        """
        cls.validate_connection_params(connection_params)
        
        # Create temp file for credentials
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp:
            temp.write(json.dumps(connection_params["credentials"]).encode())
            credentials_path = temp.name
            
        try:
            # Create credentials
            creds = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            # Build service
            service = build('sheets', 'v4', credentials=creds)
            
            # Test connection
            if "spreadsheet_id" in connection_params and connection_params["spreadsheet_id"]:
                service.spreadsheets().get(spreadsheetId=connection_params["spreadsheet_id"]).execute()
            else:
                # Just test authentication
                service.spreadsheets().get(spreadsheetId="invalid_just_testing_auth").execute()
            
            return True
            
        except HttpError as e:
            # 404 is expected for the invalid ID test
            if e.resp.status == 404:
                return True
            raise Exception(f"Google Sheets API error: {str(e)}")
            
        except Exception as e:
            raise Exception(f"Failed to connect to Google Sheets: {str(e)}")
            
        finally:
            # Clean up temp file
            if os.path.exists(credentials_path):
                os.unlink(credentials_path)
    
    @classmethod
    def get_metadata(cls, connection_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get metadata about Google Sheets spreadsheet
        
        Args:
            connection_params: Parameters for the connection
            
        Returns:
            Dict[str, Any]: Spreadsheet metadata
            
        Raises:
            Exception: If metadata retrieval fails
        """
        cls.validate_connection_params(connection_params)
        
        if "spreadsheet_id" not in connection_params or not connection_params["spreadsheet_id"]:
            raise ValueError("spreadsheet_id is required to get metadata")
        
        # Create temp file for credentials
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp:
            temp.write(json.dumps(connection_params["credentials"]).encode())
            credentials_path = temp.name
            
        try:
            # Create credentials
            creds = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            # Build service
            service = build('sheets', 'v4', credentials=creds)
            
            # Get spreadsheet info
            spreadsheet_id = connection_params["spreadsheet_id"]
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            
            # Extract sheet names
            sheets = []
            for sheet in spreadsheet.get('sheets', []):
                sheet_props = sheet.get('properties', {})
                sheets.append({
                    'title': sheet_props.get('title', ''),
                    'sheetId': sheet_props.get('sheetId', 0),
                    'index': sheet_props.get('index', 0)
                })
            
            return {
                'type': 'google_sheets',
                'id': spreadsheet_id,
                'title': spreadsheet.get('properties', {}).get('title', ''),
                'locale': spreadsheet.get('properties', {}).get('locale', ''),
                'timeZone': spreadsheet.get('properties', {}).get('timeZone', ''),
                'sheets': sheets
            }
            
        except Exception as e:
            raise Exception(f"Failed to get Google Sheets metadata: {str(e)}")
            
        finally:
            # Clean up temp file
            if os.path.exists(credentials_path):
                os.unlink(credentials_path)
    
    @classmethod
    def get_schema(cls, connection_params: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        """
        Get schema information from Google Sheets
        
        Args:
            connection_params: Parameters for the connection
            **kwargs: Additional parameters:
                - sheet_name: Filter by sheet name (optional)
            
        Returns:
            List[Dict[str, Any]]: Schema information
            
        Raises:
            Exception: If schema retrieval fails
        """
        cls.validate_connection_params(connection_params)
        
        if "spreadsheet_id" not in connection_params or not connection_params["spreadsheet_id"]:
            raise ValueError("spreadsheet_id is required to get schema")
        
        sheet_name = kwargs.get("sheet_name", settings.DEFAULT_SHEET_NAME)
        
        # Create temp file for credentials
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp:
            temp.write(json.dumps(connection_params["credentials"]).encode())
            credentials_path = temp.name
            
        try:
            # Create credentials
            creds = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            # Build service
            service = build('sheets', 'v4', credentials=creds)
            
            # Get metadata first to validate sheet name
            spreadsheet_id = connection_params["spreadsheet_id"]
            metadata = cls.get_metadata(connection_params)
            
            # Check if sheet exists
            sheet_exists = False
            for sheet in metadata.get('sheets', []):
                if sheet.get('title') == sheet_name:
                    sheet_exists = True
                    break
            
            if not sheet_exists:
                # If sheet_name not found, use the first sheet
                if metadata.get('sheets'):
                    sheet_name = metadata.get('sheets')[0].get('title')
                else:
                    raise ValueError(f"Sheet '{sheet_name}' not found and no sheets exist in the spreadsheet")
            
            # Get header row
            result = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!1:1",
                valueRenderOption='UNFORMATTED_VALUE'
            ).execute()
            
            headers = result.get('values', [[]])[0]
            
            # Get a few rows to infer data types
            data_result = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!1:6",  # Get first 5 rows (including header)
                valueRenderOption='UNFORMATTED_VALUE'
            ).execute()
            
            values = data_result.get('values', [])
            
            if len(values) <= 1:  # Only header or empty
                column_types = ["string"] * len(headers)
            else:
                # Infer types from data
                column_types = []
                for col_idx in range(len(headers)):
                    col_type = "string"  # Default type
                    
                    # Check each row (starting from row 1, after header)
                    for row_idx in range(1, len(values)):
                        if row_idx < len(values) and col_idx < len(values[row_idx]):
                            value = values[row_idx][col_idx]
                            
                            if isinstance(value, (int, float)):
                                col_type = "number"
                            elif isinstance(value, bool):
                                col_type = "boolean"
                            # Could add more type checks if needed
                    
                    column_types.append(col_type)
            
            # Get row count
            sheet_data_result = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                valueRenderOption='UNFORMATTED_VALUE'
            ).execute()
            
            row_count = len(sheet_data_result.get('values', [])) - 1  # Subtract header row
            
            # Build schema info
            columns = []
            for i, header in enumerate(headers):
                columns.append({
                    "name": header,
                    "type": column_types[i] if i < len(column_types) else "string",
                    "nullable": True  # Google Sheets columns are always nullable
                })
            
            return [{
                "sheet": sheet_name,
                "columns": columns,
                "row_count": row_count
            }]
            
        except Exception as e:
            raise Exception(f"Failed to get Google Sheets schema: {str(e)}")
            
        finally:
            # Clean up temp file
            if os.path.exists(credentials_path):
                os.unlink(credentials_path)


# Register the connector
ConnectorRegistry.register(GoogleSheetsConnector)