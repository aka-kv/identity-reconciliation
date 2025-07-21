"""
Database models for Identity Reconciliation API
Contains SQLAlchemy model definitions for customer contact data
and identity linking relationships.
"""

from .base import BaseModel, Base
from .contact import Contact

# Export all models for easy importing
__all__ = [
    "BaseModel",
    "Base", 
    "Contact"
] 