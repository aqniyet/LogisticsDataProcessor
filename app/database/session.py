from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import logging
import os

from app.core.config import get_config_value
from app.database.models import Base

logger = logging.getLogger(__name__)

def get_database_url():
    """Get the database URL from configuration."""
    db_path = get_config_value("database_path", "logistics_processor.db")
    # Convert to absolute path if relative
    if not os.path.isabs(db_path):
        db_path = os.path.abspath(db_path)
    return f'sqlite:///{db_path}'

def create_database_engine():
    """Create and configure the database engine."""
    database_url = get_database_url()
    logger.info(f"Creating database engine with URL: {database_url}")
    
    # Create the engine with SQLite optimizations
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},  # Allow multiple threads for SQLite
        pool_pre_ping=True,  # Enable connection health checks
        echo=False  # Set to True for SQL query logging
    )
    
    # Create all tables if they don't exist
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully")
    
    return engine

# Create the engine
engine = create_database_engine()

# Create a scoped session factory
Session = scoped_session(sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False
))

def get_session():
    """Get a new database session."""
    return Session()

def cleanup_session():
    """Remove the current session."""
    Session.remove()

# Register cleanup on module unload
import atexit
atexit.register(cleanup_session) 