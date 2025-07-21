"""
Database table creation script for Identity Reconciliation API
This script creates all database tables and tests the database connection.
Run this script after setting up your database to initialize the schema.
"""

import logging
from database import db_manager
from models import Base, Contact

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tables():
    """
    Create all database tables defined in the models
    This function ensures all tables exist and are up to date
    """
    try:
        logger.info("Starting database table creation...")
        
        # Test database connection first
        if not db_manager.test_connection():
            logger.error("Database connection failed - cannot create tables")
            return False
        
        # Create all tables
        db_manager.create_tables()
        
        logger.info("Database tables created successfully!")
        
        # Test basic operations
        logger.info("Testing basic database operations...")
        
        with db_manager.get_session() as session:
            # Test that we can query the contacts table (even if empty)
            result = session.query(Contact).count()
            logger.info(f"Contacts table accessible - current count: {result}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create database tables: {e}")
        return False

def main():
    """Main function to run the table creation"""
    logger.info("Identity Reconciliation API - Database Setup")
    logger.info("=" * 50)
    
    success = create_tables()
    
    if success:
        logger.info("Database setup completed successfully!")
        logger.info("You can now start the API server with: python main.py")
    else:
        logger.error("Database setup failed!")
        logger.error("Please check your database configuration and try again")
    
    return success

if __name__ == "__main__":
    main() 