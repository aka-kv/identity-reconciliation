"""
Configuration settings for Identity Reconciliation System
Manages database connections, API settings, and environment variables
Compatible with Python 3.7+ and handles both local and AWS deployment
"""

import os
from typing import Optional

class Settings:
    """
    Application configuration class that loads settings from environment variables
    with fallback defaults for local development
    """
    
    # Database Configuration
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        "postgresql+asyncpg://postgres:kv2k04@localhost:5432/identity_reconciliation"
    )
    
    # RDS Configuration (for AWS deployment)
    RDS_HOSTNAME: str = os.getenv("RDS_HOSTNAME", "localhost")
    RDS_PORT: str = os.getenv("RDS_PORT", "5432")
    RDS_DB_NAME: str = os.getenv("RDS_DB_NAME", "identity_reconciliation")
    RDS_USERNAME: str = os.getenv("RDS_USERNAME", "postgres")
    RDS_PASSWORD: str = os.getenv("RDS_PASSWORD", "")
    
    # SSL Configuration for RDS
    DB_SSL_MODE: str = os.getenv("DB_SSL_MODE", "prefer")  # require, prefer, disable
    
    # API Configuration
    API_TITLE: str = "Identity Reconciliation API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "Backend service for tracking and linking customer identities"
    
    # Server Configuration
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    
    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "True").lower() == "true"
    
    # AWS Configuration (for future deployment)
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # CORS Configuration
    CORS_ORIGINS: list = ["*"]  # In production, this should be more restrictive
    
    @classmethod
    def get_database_url(cls) -> str:
        """
        Get the database URL for the current environment
        Ensures proper async driver is used
        """
        # If a DATABASE_URL is provided in environment, ensure it uses asyncpg
        if "postgresql://" in cls.DATABASE_URL and "asyncpg" not in cls.DATABASE_URL:
            return cls.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
        return cls.DATABASE_URL
    
    @classmethod
    def get_rds_database_url(cls) -> str:
        """
        Build RDS database URL from individual components
        Used when deploying to AWS with RDS
        """
        if cls.RDS_PASSWORD:
            auth = f"{cls.RDS_USERNAME}:{cls.RDS_PASSWORD}"
        else:
            auth = cls.RDS_USERNAME
            
        base_url = f"postgresql+asyncpg://{auth}@{cls.RDS_HOSTNAME}:{cls.RDS_PORT}/{cls.RDS_DB_NAME}"
        
        # Add SSL configuration for RDS (asyncpg uses 'ssl' parameter, not 'sslmode')
        if cls.DB_SSL_MODE != "disable":
            if cls.DB_SSL_MODE == "require":
                base_url += "?ssl=require"
            elif cls.DB_SSL_MODE == "prefer":
                base_url += "?ssl=prefer"
            else:
                base_url += "?ssl=true"
            
        return base_url
    
    @classmethod
    def get_active_database_url(cls) -> str:
        """
        Get the appropriate database URL based on environment
        Uses RDS configuration if RDS_HOSTNAME is set and not localhost, otherwise uses DATABASE_URL
        """
        # If we have RDS configuration (hostname is not localhost and password is set), use RDS
        if cls.RDS_HOSTNAME and cls.RDS_HOSTNAME != "localhost" and cls.RDS_PASSWORD:
            return cls.get_rds_database_url()
        # Otherwise use the DATABASE_URL
        return cls.get_database_url()
    
    @classmethod
    def is_production(cls) -> bool:
        """
        Check if we're running in production environment
        """
        return cls.ENVIRONMENT.lower() == "production"
    
    @classmethod
    def is_lambda_environment(cls) -> bool:
        """
        Check if we're running in AWS Lambda
        """
        return os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None

# Create a global settings instance
settings = Settings() 