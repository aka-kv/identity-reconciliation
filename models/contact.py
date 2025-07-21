"""
Contact model for Identity Reconciliation System
Represents customer contact information with linking relationships
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import String, Integer, ForeignKey, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base


class Contact(Base):
    """
    Contact model representing customer contact information
    
    Relationships:
    - Primary contacts have linked_id = None, link_precedence = 'primary'
    - Secondary contacts have linked_id pointing to primary, link_precedence = 'secondary'
    """
    __tablename__ = "contacts"
    
    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Contact information (at least one must be provided)
    phone_number: Mapped[Optional[str]] = mapped_column(String(20), index=True, nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    
    # Linking information
    linked_id: Mapped[Optional[int]] = mapped_column(
        Integer, 
        ForeignKey("contacts.id"), 
        nullable=True,
        index=True
    )
    link_precedence: Mapped[str] = mapped_column(
        String(10), 
        nullable=False,
        default="primary"
    )
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False,
        server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), 
        nullable=True
    )
    
    # Relationships
    # Self-referential relationship for linked contacts
    primary_contact: Mapped[Optional["Contact"]] = relationship(
        "Contact",
        remote_side=[id],
        back_populates="secondary_contacts"
    )
    
    secondary_contacts: Mapped[list["Contact"]] = relationship(
        "Contact",
        back_populates="primary_contact"
    )
    
    def __repr__(self) -> str:
        return f"<Contact(id={self.id}, email={self.email}, phone={self.phone_number}, precedence={self.link_precedence})>"
    
    def is_primary(self) -> bool:
        """Check if this contact is a primary contact"""
        return self.link_precedence == "primary" and self.linked_id is None
    
    def is_secondary(self) -> bool:
        """Check if this contact is a secondary contact"""
        return self.link_precedence == "secondary" and self.linked_id is not None