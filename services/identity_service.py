"""
Identity Service for contact reconciliation and linking
This service handles the core business logic for identifying and linking
customer contacts based on email and phone number information.
Implements the complete identity reconciliation algorithm.
"""

from typing import List, Optional, Set
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
import logging
from datetime import datetime

from models import Contact
from schemas import IdentifyRequest, IdentifyResponse, ContactResponse
from database import db_manager

# Configure logging
logger = logging.getLogger(__name__)

class IdentityService:
    """
    Core service for customer identity reconciliation
    
    Handles the complete workflow of finding, linking, and consolidating
    customer contact information based on email and phone number matches.
    """
    
    def __init__(self):
        """Initialize the identity service"""
        pass
    
    async def identify_contact(self, request: IdentifyRequest) -> IdentifyResponse:
        """
        Main method to identify and reconcile customer contacts
        
        Algorithm:
        1. Find existing contacts matching email or phone
        2. If no matches → create new primary contact
        3. If matches found → determine linking strategy:
           - New information → create secondary contact
           - Link two primaries → convert newer to secondary
        4. Return consolidated contact information
        """
        logger.info(f"Processing identity request: email={request.email}, phone={request.phoneNumber}")
        
        with db_manager.get_session() as session:
            # Step 1: Find existing contacts
            existing_contacts = self._find_existing_contacts(session, request.email, request.phoneNumber)
            
            if not existing_contacts:
                # Step 2: No matches - create new primary contact
                logger.info("No existing contacts found - creating new primary contact")
                primary_contact = self._create_primary_contact(session, request.email, request.phoneNumber)
                return self._build_response(session, primary_contact)
            
            # Step 3: Handle existing contacts
            primary_contacts = [c for c in existing_contacts if c.is_primary()]
            
            if len(primary_contacts) == 0:
                # All existing contacts are secondary - get their primary
                logger.info("Found secondary contacts - using existing primary")
                primary_contact = existing_contacts[0].get_primary_contact()
                
                # Check if we need to add new info to existing primary
                if self._needs_new_secondary(primary_contact, request.email, request.phoneNumber):
                    logger.info("Adding new secondary contact with additional info")
                    self._create_secondary_contact(session, primary_contact.id, request.email, request.phoneNumber)
                
                return self._build_response(session, primary_contact)
            
            elif len(primary_contacts) == 1:
                # One primary contact found
                primary_contact = primary_contacts[0]
                logger.info(f"Found existing primary contact: {primary_contact.id}")
                
                # Check if we need to add new info as secondary
                if self._needs_new_secondary(primary_contact, request.email, request.phoneNumber):
                    logger.info("Adding new secondary contact with additional info")
                    self._create_secondary_contact(session, primary_contact.id, request.email, request.phoneNumber)
                
                return self._build_response(session, primary_contact)
            
            else:
                # Multiple primary contacts found - need to link them
                logger.info(f"Found {len(primary_contacts)} primary contacts - linking them")
                primary_contact = self._link_primary_contacts(session, primary_contacts, request.email, request.phoneNumber)
                return self._build_response(session, primary_contact)
    
    def _find_existing_contacts(self, session: Session, email: Optional[str], phone: Optional[str]) -> List[Contact]:
        """
        Find all existing contacts that match the given email or phone number
        """
        conditions = []
        
        if email:
            conditions.append(Contact.email == email)
        if phone:
            conditions.append(Contact.phone_number == phone)
        
        if not conditions:
            return []
        
        # Find contacts matching any of the conditions
        contacts = session.query(Contact).filter(
            or_(*conditions),
            Contact.deleted_at.is_(None)  # Exclude soft-deleted contacts
        ).all()
        
        logger.info(f"Found {len(contacts)} existing contacts")
        return contacts
    
    def _create_primary_contact(self, session: Session, email: Optional[str], phone: Optional[str]) -> Contact:
        """
        Create a new primary contact with the given email and phone
        """
        contact = Contact(
            email=email,
            phone_number=phone,
            link_precedence="primary",
            linked_id=None
        )
        
        session.add(contact)
        session.flush()  # Get the ID without committing
        
        logger.info(f"Created new primary contact: {contact.id}")
        return contact
    
    def _create_secondary_contact(self, session: Session, primary_id: int, email: Optional[str], phone: Optional[str]) -> Contact:
        """
        Create a new secondary contact linked to the given primary contact
        """
        contact = Contact(
            email=email,
            phone_number=phone,
            link_precedence="secondary",
            linked_id=primary_id
        )
        
        session.add(contact)
        session.flush()  # Get the ID without committing
        
        logger.info(f"Created new secondary contact: {contact.id} linked to primary: {primary_id}")
        return contact
    
    def _needs_new_secondary(self, primary_contact: Contact, email: Optional[str], phone: Optional[str]) -> bool:
        """
        Check if we need to create a new secondary contact for additional information
        Returns True if the request contains new email or phone not already associated with the primary
        """
        # Get all contacts linked to this primary (including the primary itself)
        with db_manager.get_session() as session:
            all_linked_contacts = session.query(Contact).filter(
                or_(
                    Contact.id == primary_contact.id,
                    Contact.linked_id == primary_contact.id
                ),
                Contact.deleted_at.is_(None)
            ).all()
        
        # Collect all existing emails and phones
        existing_emails = {c.email for c in all_linked_contacts if c.email}
        existing_phones = {c.phone_number for c in all_linked_contacts if c.phone_number}
        
        # Check if request has new information
        has_new_email = email and email not in existing_emails
        has_new_phone = phone and phone not in existing_phones
        
        return has_new_email or has_new_phone
    
    def _link_primary_contacts(self, session: Session, primary_contacts: List[Contact], email: Optional[str], phone: Optional[str]) -> Contact:
        """
        Link multiple primary contacts by converting the newer ones to secondary
        Returns the primary contact that remains as primary
        """
        # Sort by creation date - oldest becomes the primary
        primary_contacts.sort(key=lambda c: c.created_at)
        primary_contact = primary_contacts[0]
        
        logger.info(f"Keeping contact {primary_contact.id} as primary (oldest: {primary_contact.created_at})")
        
        # Convert newer primaries to secondary
        for contact in primary_contacts[1:]:
            logger.info(f"Converting contact {contact.id} from primary to secondary")
            contact.link_precedence = "secondary"
            contact.linked_id = primary_contact.id
            contact.updated_at = datetime.utcnow()
        
        # Check if we need to add new information as another secondary
        if self._needs_new_secondary(primary_contact, email, phone):
            logger.info("Adding new secondary contact with additional info")
            self._create_secondary_contact(session, primary_contact.id, email, phone)
        
        return primary_contact
    
    def _build_response(self, session: Session, primary_contact: Contact) -> IdentifyResponse:
        """
        Build the consolidated response with all linked contact information
        """
        # Get all contacts linked to this primary
        all_linked_contacts = session.query(Contact).filter(
            or_(
                Contact.id == primary_contact.id,
                Contact.linked_id == primary_contact.id
            ),
            Contact.deleted_at.is_(None)
        ).order_by(Contact.created_at).all()
        
        # Separate primary and secondary contacts
        primary = primary_contact
        secondaries = [c for c in all_linked_contacts if c.id != primary.id]
        
        # Collect all emails and phone numbers (primary first)
        emails = []
        phone_numbers = []
        
        # Add primary contact info first
        if primary.email:
            emails.append(primary.email)
        if primary.phone_number:
            phone_numbers.append(primary.phone_number)
        
        # Add secondary contact info
        for contact in secondaries:
            if contact.email and contact.email not in emails:
                emails.append(contact.email)
            if contact.phone_number and contact.phone_number not in phone_numbers:
                phone_numbers.append(contact.phone_number)
        
        # Get secondary contact IDs
        secondary_ids = [c.id for c in secondaries]
        
        # Build response
        contact_response = ContactResponse(
            primaryContatId=primary.id,
            emails=emails,
            phoneNumbers=phone_numbers,
            secondaryContactIds=secondary_ids
        )
        
        logger.info(f"Built response for primary {primary.id} with {len(emails)} emails, {len(phone_numbers)} phones, {len(secondary_ids)} secondaries")
        
        return IdentifyResponse(contact=contact_response)

# Global service instance
identity_service = IdentityService() 