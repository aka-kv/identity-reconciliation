"""
Database initialization and management for Identity Reconciliation System
Handles database connection, table creation, and basic operations
"""

import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from models.base import get_engine, get_session_factory, Base
from models.contact import Contact
from config import settings


class DatabaseManager:
    """Manages database operations and connections"""
    
    def __init__(self):
        self._engine = None
        self._session_factory = None
    
    @property
    def engine(self):
        """Get database engine with lazy initialization"""
        if self._engine is None:
            self._engine = get_engine()
        return self._engine
    
    @property
    def session_factory(self):
        """Get session factory with lazy initialization"""
        if self._session_factory is None:
            self._session_factory = get_session_factory()
        return self._session_factory
    
    async def create_tables(self):
        """Create all database tables"""
        async with self.engine.begin() as conn:
            # Drop tables if they exist (for development)
            await conn.run_sync(Base.metadata.drop_all)
            # Create all tables
            await conn.run_sync(Base.metadata.create_all)
            print("✅ Database tables created successfully")
    
    async def create_indexes(self):
        """Create additional database indexes for performance"""
        async with self.engine.begin() as conn:
            # Create composite index for email and phone lookup
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_contacts_email_phone 
                ON contacts(email, phone_number) 
                WHERE email IS NOT NULL OR phone_number IS NOT NULL
            """))
            
            # Create partial indexes for better performance
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_contacts_email_not_null 
                ON contacts(email) 
                WHERE email IS NOT NULL
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_contacts_phone_not_null 
                ON contacts(phone_number) 
                WHERE phone_number IS NOT NULL
            """))
            
            print("✅ Database indexes created successfully")
    
    async def test_connection(self):
        """Test database connection"""
        try:
            # Get the active database URL to see what we're trying to connect to
            active_url = settings.get_active_database_url()
            print(f"🔗 Attempting to connect to: {active_url[:50]}...")
            
            # Check if we have proper database configuration
            if settings.RDS_HOSTNAME and settings.RDS_HOSTNAME != "localhost":
                if not settings.RDS_PASSWORD:
                    print("❌ RDS hostname provided but no password set")
                    return False
                print(f"🌐 Using RDS configuration: {settings.RDS_HOSTNAME}")
            else:
                print("🏠 Using local database configuration")
            
            async with self.session_factory() as session:
                result = await session.execute(text("SELECT 1"))
                row = result.fetchone()
                if row and row[0] == 1:
                    print("✅ Database connection successful")
                    return True
                else:
                    print("❌ Database connection test failed")
                    return False
        except Exception as e:
            print(f"❌ Database connection error: {e}")
            return False
    
    def get_session(self) -> AsyncSession:
        """Get an async database session"""
        return self.session_factory()


# Global database manager instance
db_manager = DatabaseManager()


async def init_database():
    """Initialize database with tables and indexes"""
    print("🔄 Initializing database...")
    
    # Test connection first
    if not await db_manager.test_connection():
        print("❌ Cannot connect to database. Please check your DATABASE_URL")
        return False
    
    # Create tables
    await db_manager.create_tables()
    
    # Create indexes
    await db_manager.create_indexes()
    
    print("✅ Database initialization complete!")
    return True


if __name__ == "__main__":
    """Run database initialization"""
    print(f"Database URL: {settings.DATABASE_URL}")
    asyncio.run(init_database())