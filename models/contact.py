"""
Contact model for Identity Reconciliation API
This module defines the Contact database model for storing customer
contact information and managing identity linking relationships.
Supports primary/secondary contact hierarchy and soft delete functionality.
"""

from sqlalchemy import Column, String, Integer, ForeignKey, Index, CheckConstraint
from sqlalchemy.orm import relationship
from .base import BaseModel

class Contact(BaseModel):
    """
    Contact model representing customer contact information
    
    Stores email and phone number data with linking relationships
    to support identity reconciliation. Each contact can be either
    'primary' (independent) or 'secondary' (linked to a primary contact).
    
    Database Table: contacts
    """
    __tablename__ = "contacts"
    
    # Contact information fields - at least one must be provided
    phone_number = Column(
        String(20), 
        nullable=True, 
        index=True,
        comment="Customer phone number in international format"
    )
    
    email = Column(
        String(255), 
        nullable=True, 
        index=True,
        comment="Customer email address"
    )
    
    # Identity linking fields
    linked_id = Column(
        Integer, 
        ForeignKey("contacts.id", ondelete="CASCADE"), 
        nullable=True,
        index=True,
        comment="ID of the primary contact this secondary contact links to"
    )
    
    link_precedence = Column(
        String(10), 
        nullable=False, 
        default="primary",
        comment="Either 'primary' (independent contact) or 'secondary' (linked contact)"
    )
    
    # Relationships
    linked_contact = relationship(
        "Contact", 
        remote_side=[BaseModel.id], 
        backref="secondary_contacts",
        comment="The primary contact this contact links to"
    )
    
    # Database constraints
    __table_args__ = (
        # Ensure link_precedence is either 'primary' or 'secondary'
        CheckConstraint(
            link_precedence.in_(["primary", "secondary"]),
            name="valid_link_precedence"
        ),
        
        # Ensure at least one contact method is provided
        CheckConstraint(
            "(phone_number IS NOT NULL) OR (email IS NOT NULL)",
            name="contact_info_required"
        ),
        
        # Ensure secondary contacts have a linked_id
        CheckConstraint(
            "(link_precedence = 'primary' AND linked_id IS NULL) OR "
            "(link_precedence = 'secondary' AND linked_id IS NOT NULL)",
            name="secondary_must_have_linked_id"
        ),
        
        # Composite indexes for efficient querying
        Index("ix_contact_email_phone", email, phone_number),
        Index("ix_contact_precedence_linked", link_precedence, linked_id),
    )
    
    def __repr__(self):
        """String representation showing key contact information"""
        contact_info = []
        if self.email:
            contact_info.append(f"email={self.email}")
        if self.phone_number:
            contact_info.append(f"phone={self.phone_number}")
        
        return (
            f"<Contact(id={self.id}, "
            f"{', '.join(contact_info)}, "
            f"precedence={self.link_precedence})>"
        )
    
    def is_primary(self):
        """Check if this is a primary contact"""
        return self.link_precedence == "primary"
    
    def is_secondary(self):
        """Check if this is a secondary contact"""
        return self.link_precedence == "secondary"
    
    def get_primary_contact(self):
        """
        Get the primary contact for this contact
        Returns self if this is already primary, or the linked primary contact
        """
        if self.is_primary():
            return self
        return self.linked_contact
    
    def to_dict(self):
        """Convert contact to dictionary with formatted timestamps"""
        data = super().to_dict()
        
        # Format datetime fields as ISO strings
        if data.get("created_at"):
            data["created_at"] = data["created_at"].isoformat()
        if data.get("updated_at"):
            data["updated_at"] = data["updated_at"].isoformat()
        if data.get("deleted_at"):
            data["deleted_at"] = data["deleted_at"].isoformat()
            
        return data 