"""
Base model class for Identity Reconciliation API
This module provides the base model class with common fields
and functionality that all database models inherit from.
Includes timestamp fields and soft delete functionality.
"""

from sqlalchemy import Column, Integer, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

# Base class for all database models
Base = declarative_base()

class BaseModel(Base):
    """
    Base model class that provides common fields and functionality
    for all database models. Includes automatic timestamp management
    and soft delete capability.
    """
    __abstract__ = True
    
    # Primary key field - auto-incrementing integer
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    
    # Timestamp fields with automatic management
    created_at = Column(
        DateTime(timezone=True), 
        server_default=func.now(), 
        nullable=False,
        comment="Timestamp when record was created"
    )
    
    updated_at = Column(
        DateTime(timezone=True), 
        server_default=func.now(), 
        onupdate=func.now(), 
        nullable=False,
        comment="Timestamp when record was last updated"
    )
    
    deleted_at = Column(
        DateTime(timezone=True), 
        nullable=True,
        comment="Timestamp when record was soft deleted (NULL if not deleted)"
    )
    
    def __repr__(self):
        """String representation of the model"""
        return f"<{self.__class__.__name__}(id={self.id})>"
    
    def to_dict(self):
        """Convert model instance to dictionary"""
        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
        } 