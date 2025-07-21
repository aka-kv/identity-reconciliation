"""
Database connection and session management for Identity Reconciliation API
This module sets up SQLAlchemy database connections with proper session management
for both synchronous and asynchronous operations. Supports local PostgreSQL
and AWS RDS deployments with connection pooling and error handling.
"""

from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
from contextlib import contextmanager
from config import settings

# Configure logging
logger = logging.getLogger(__name__)

# SQLAlchemy base class for models
Base = declarative_base()

# Database metadata for table operations
metadata = MetaData()

class DatabaseManager:
    """
    Database connection manager that handles SQLAlchemy engine,
    session creation, and connection lifecycle management
    """
    
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database engine and session factory"""
        try:
            database_url = settings.get_active_database_url()
            logger.info(f"Initializing database connection to: {database_url.split('@')[0]}@[HIDDEN]")
            
            # Create SQLAlchemy engine with connection pooling
            self.engine = create_engine(
                database_url,
                pool_size=settings.DB_POOL_SIZE,
                max_overflow=settings.DB_MAX_OVERFLOW,
                pool_timeout=settings.DB_POOL_TIMEOUT,
                pool_pre_ping=True,  # Validate connections before use
                echo=settings.DEBUG  # Log SQL queries in debug mode
            )
            
            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            logger.info("Database connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise
    
    def create_tables(self):
        """Create all database tables defined in models"""
        try:
            logger.info("Creating database tables...")
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise
    
    def test_connection(self):
        """Test database connection"""
        try:
            with self.engine.connect() as connection:
                connection.execute("SELECT 1")
                logger.info("Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    @contextmanager
    def get_session(self):
        """
        Context manager for database sessions with automatic cleanup
        Usage: 
            with db_manager.get_session() as session:
                # database operations
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def get_db_session(self):
        """
        Generator function for FastAPI dependency injection
        Provides database session for API endpoints
        """
        session = self.SessionLocal()
        try:
            yield session
        except Exception as e:
            session.rollback()
            logger.error(f"FastAPI database session error: {e}")
            raise
        finally:
            session.close()

# Global database manager instance
db_manager = DatabaseManager() 