# Database Connector Service

A modular service for managing connections to various data sources, with extensible support for different connector types.

## Features

- Extensible connector architecture
- Secure storage of connection parameters with encryption
- API endpoints for managing connections
- Testing connections to verify they work
- Retrieving metadata about data sources
- Support for:
  - PostgreSQL
  - MySQL
  - Google Sheets
  - Easily extensible for additional connectors

## Architecture

This service is focused solely on managing connections and providing basic metadata about those connections. It follows a plugin-based architecture that makes it easy to add new connector types without modifying existing code.

## API Endpoints

### Health Endpoints

- `GET /` - Root endpoint, returns
