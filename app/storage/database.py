import sys

import databases
import sqlalchemy
from sqlalchemy import MetaData

import app.config as config

# Database instance
try:
    database = databases.Database(config.DATABASE_URL)
    print("Database instance created successfully")
except Exception as e:
    print(f"Error creating database instance: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# SQLAlchemy MetaData object
metadata = MetaData()

# Define connector table
connectors = sqlalchemy.Table(
    "connectors",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.String(36), primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String(100), nullable=False, index=True),
    sqlalchemy.Column("type", sqlalchemy.String(50), nullable=False, index=True),
    sqlalchemy.Column("config", sqlalchemy.JSON, nullable=False),
    sqlalchemy.Column("description", sqlalchemy.Text, nullable=True),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, default=sqlalchemy.sql.func.now()), # pylint: disable=not-callable
    sqlalchemy.Column("updated_at", sqlalchemy.DateTime,
                     default=sqlalchemy.sql.func.now(), # pylint: disable=not-callable
                     onupdate=sqlalchemy.sql.func.now()), # pylint: disable=not-callable
)

# Create engine
engine = sqlalchemy.create_engine(
    config.DATABASE_URL,
    connect_args={"check_same_thread": False} if config.DATABASE_URL and config.DATABASE_URL.startswith("sqlite") else {}
)

async def init_db():
    """Initialize the database"""
    # Create tables
    metadata.create_all(engine)
    # Connect to database
    await database.connect()

async def close_db():
    """Close the database connection"""
    await database.disconnect()