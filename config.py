"""
Configuration settings for Identity Reconciliation API
This module handles all environment variables and application settings
for database connections, API configuration, and deployment environments.
Supports both local development and AWS Lambda deployment.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class Settings:
    """
    Application settings class that manages all configuration
    from environment variables with sensible defaults
    """
    
    # API Configuration
    API_TITLE = "Identity Reconciliation API"
    API_DESCRIPTION = "Customer identity linking and reconciliation service"
    API_VERSION = "1.0.0"
    
    # Server Configuration
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", 8000))
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    
    # Database Configuration
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/identity_db")
    
    # AWS RDS Configuration (for production)
    RDS_HOSTNAME = os.getenv("RDS_HOSTNAME", "localhost")
    RDS_PORT = int(os.getenv("RDS_PORT", 5432))
    RDS_DB_NAME = os.getenv("RDS_DB_NAME", "identity_db")
    RDS_USERNAME = os.getenv("RDS_USERNAME", "user")
    RDS_PASSWORD = os.getenv("RDS_PASSWORD", "password")
    
    # Database Connection Settings
    DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", 5))
    DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", 10))
    DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", 30))
    DB_SSL_MODE = os.getenv("DB_SSL_MODE", "prefer")
    
    # CORS Configuration
    CORS_ORIGINS = [
        "http://localhost:3000",
        "http://localhost:8000",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8000"
    ]
    
    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    def get_active_database_url(self):
        """
        Returns the appropriate database URL based on environment
        Uses RDS configuration if available, otherwise falls back to DATABASE_URL
        """
        if self.is_lambda_environment() or self.RDS_HOSTNAME != "localhost":
            return (
                f"postgresql://{self.RDS_USERNAME}:{self.RDS_PASSWORD}"
                f"@{self.RDS_HOSTNAME}:{self.RDS_PORT}/{self.RDS_DB_NAME}"
                f"?sslmode={self.DB_SSL_MODE}"
            )
        return self.DATABASE_URL
    
    def is_lambda_environment(self):
        """Check if running in AWS Lambda environment"""
        return os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None

# Global settings instance
settings = Settings() 