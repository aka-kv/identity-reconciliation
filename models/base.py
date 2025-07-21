"""
SQLAlchemy base configuration for Identity Reconciliation System
This module sets up the SQLAlchemy declarative base and async engine
"""

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.ext.asyncio import async_sessionmaker

from config import settings

# Global variables for lazy initialization
engine = None
AsyncSessionFactory = None

def create_database_engine():
    """Create database engine with appropriate settings for environment"""
    database_url = settings.get_active_database_url()
    
    if settings.is_lambda_environment():
        # Lambda-optimized settings for RDS Proxy
        return create_async_engine(
            database_url,
            echo=settings.DEBUG,
            pool_pre_ping=True,  # Essential for Lambda - check connections
            pool_size=1,  # Small pool for Lambda (single concurrent execution)
            max_overflow=0,  # No overflow in Lambda
            pool_recycle=3600,  # Recycle connections after 1 hour
            pool_timeout=10,  # Quick timeout for Lambda
            connect_args={
                "command_timeout": 10,  # Quick command timeout
                "server_settings": {
                    "application_name": "identity-reconciliation-lambda",
                }
            }
        )
    else:
        # Local development settings
        return create_async_engine(
            database_url,
            echo=settings.DEBUG,
            pool_pre_ping=True,
            pool_size=5,  # Larger pool for local development
            max_overflow=10,
            connect_args={
                "server_settings": {
                    "application_name": "identity-reconciliation-local",
                }
            }
        )

def get_engine():
    """Get database engine, creating it if necessary"""
    global engine
    if engine is None:
        engine = create_database_engine()
    return engine

def get_session_factory():
    """Get session factory, creating it if necessary"""
    global AsyncSessionFactory
    if AsyncSessionFactory is None:
        AsyncSessionFactory = async_sessionmaker(
            get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,  # Don't expire objects after commit
            autoflush=False  # Don't auto-flush - better control over operations
        )
    return AsyncSessionFactory

class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models"""
    pass 