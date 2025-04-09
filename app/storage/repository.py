import uuid
from datetime import datetime
from typing import List, Optional

from sqlalchemy import delete, insert, select, update

from app.models.connector import Connector, ConnectorCreate, ConnectorUpdate
from app.storage.database import connectors, database
from app.utils.encryption import decrypt_data, encrypt_data


class ConnectorRepository:
    """Repository for database connectors"""

    @staticmethod
    async def get_all() -> List[Connector]:
        """Get all connectors"""
        query = select(connectors)
        result = await database.fetch_all(query)
        connectors_list = []

        for row in result:
            connector_dict = dict(row)
            # Decrypt config data
            if connector_dict.get("config"):
                connector_dict["config"] = decrypt_data(connector_dict["config"])
            connectors_list.append(Connector.model_validate(connector_dict))

        return connectors_list

    @staticmethod
    async def get_by_id(connector_id: str) -> Optional[Connector]:
        """Get connector by ID"""
        query = select(connectors).where(connectors.c.id == connector_id)
        result = await database.fetch_one(query)
        if result:
            connector_dict = dict(result)
            # Decrypt config data
            if connector_dict.get("config"):
                connector_dict["config"] = decrypt_data(connector_dict["config"])
            return Connector.model_validate(connector_dict)
        return None

    @staticmethod
    async def get_by_name(name: str) -> Optional[Connector]:
        """Get connector by name"""
        query = select(connectors).where(connectors.c.name == name)
        result = await database.fetch_one(query)
        if result:
            connector_dict = dict(result)
            # Decrypt config data
            if connector_dict.get("config"):
                connector_dict["config"] = decrypt_data(connector_dict["config"])
            return Connector.model_validate(connector_dict)
        return None

    @staticmethod
    async def get_by_type(connector_type: str) -> List[Connector]:
        """Get connectors by type"""
        query = select(connectors).where(connectors.c.type == connector_type)
        result = await database.fetch_all(query)
        connectors_list = []

        for row in result:
            connector_dict = dict(row)
            # Decrypt config data
            if connector_dict.get("config"):
                connector_dict["config"] = decrypt_data(connector_dict["config"])
            connectors_list.append(Connector.model_validate(connector_dict))

        return connectors_list

    @staticmethod
    async def create(connector: ConnectorCreate) -> Connector:
        """Create a new connector"""
        connector_id = str(uuid.uuid4())
        now = datetime.now()

        # Encrypt sensitive config data
        encrypted_config = encrypt_data(connector.config)

        query = insert(connectors).values(
            id=connector_id,
            name=connector.name,
            type=connector.type,
            config=encrypted_config,
            description=connector.description,
            created_at=now,
            updated_at=now
        )

        await database.execute(query)

        # Get the created connector
        return await ConnectorRepository.get_by_id(connector_id)

    @staticmethod
    async def update(connector_id: str, connector: ConnectorUpdate) -> Optional[Connector]:
        """Update an existing connector"""
        # Get current connector
        current_connector = await ConnectorRepository.get_by_id(connector_id)
        if not current_connector:
            return None

        # Build update values, only including fields that are set
        update_values = {"updated_at": datetime.now()}
        if connector.name is not None:
            update_values["name"] = connector.name
        if connector.description is not None:
            update_values["description"] = connector.description
        if connector.config is not None:
            # Encrypt config data
            update_values["config"] = encrypt_data(connector.config)

        query = update(connectors).where(connectors.c.id == connector_id).values(**update_values)
        await database.execute(query)

        # Get the updated connector
        return await ConnectorRepository.get_by_id(connector_id)

    @staticmethod
    async def delete(connector_id: str) -> bool:
        """Delete a connector"""
        query = delete(connectors).where(connectors.c.id == connector_id).returning(connectors.c.id)
        result = await database.fetch_one(query)
        return result is not None