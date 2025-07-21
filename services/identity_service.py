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
            
            # Step 2.5: Analyze the type of matches we found
            match_analysis = self._analyze_contact_matches(existing_contacts, request.email, request.phoneNumber)
            
            # Step 3: Handle existing contacts based on secondary contact logic
            primary_contacts = [c for c in existing_contacts if c.is_primary()]
            
            if len(primary_contacts) == 0:
                # All existing contacts are secondary - handle partial match scenario
                logger.info("Found only secondary contacts - handling partial match")
                primary_contact = self._handle_partial_match_scenario(
                    session, existing_contacts, request.email, request.phoneNumber
                )
                return self._build_response(session, primary_contact)
            
            elif len(primary_contacts) == 1:
                # Single primary contact found - handle secondary contact creation
                logger.info("Found single primary contact - checking for secondary contact needs")
                primary_contact = self._handle_partial_match_scenario(
                    session, existing_contacts, request.email, request.phoneNumber
                )
                return self._build_response(session, primary_contact)
            
            else:
                # Multiple primary contacts found - need to link them
                logger.info(f"Found {len(primary_contacts)} primary contacts - initiating linking process")
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
    
    def _create_secondary_contact(self, session: Session, primary_id: int, email: Optional[str], phone: Optional[str]) -> Optional[Contact]:
        """
        Create a new secondary contact linked to the given primary contact
        Includes validation to prevent duplicate secondary contacts
        """
        # Get primary contact for validation
        primary_contact = session.query(Contact).filter(Contact.id == primary_id).first()
        if not primary_contact:
            logger.error(f"Primary contact {primary_id} not found")
            return None
        
        # Validate secondary contact data
        if not self._validate_secondary_contact_data(email, phone, primary_contact):
            logger.info("Secondary contact validation failed - skipping creation")
            return None
        
        # Check if secondary contact with same data already exists
        existing_secondary = self._find_existing_secondary_with_data(session, primary_id, email, phone)
        if existing_secondary:
            logger.info("Secondary contact with same data already exists - returning existing")
            return existing_secondary
        
        # Create new secondary contact
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
        
        logger.info(f"Checking for new secondary contact need:")
        logger.info(f"  Request email: {email}, existing emails: {existing_emails}")
        logger.info(f"  Request phone: {phone}, existing phones: {existing_phones}")
        logger.info(f"  Has new email: {has_new_email}, has new phone: {has_new_phone}")
        
        return has_new_email or has_new_phone
    
    def _get_all_linked_contacts(self, session: Session, primary_contact: Contact) -> List[Contact]:
        """
        Get all contacts linked to a primary contact (including the primary itself)
        Returns contacts sorted by creation date for consistent ordering
        """
        all_linked_contacts = session.query(Contact).filter(
            or_(
                Contact.id == primary_contact.id,
                Contact.linked_id == primary_contact.id
            ),
            Contact.deleted_at.is_(None)
        ).order_by(Contact.created_at).all()
        
        logger.info(f"Found {len(all_linked_contacts)} contacts linked to primary {primary_contact.id}")
        return all_linked_contacts
    
    def _handle_partial_match_scenario(self, session: Session, existing_contacts: List[Contact], 
                                     email: Optional[str], phone: Optional[str]) -> Contact:
        """
        Handle scenarios where we have partial matches (email OR phone, not both)
        This is a key part of secondary contact logic
        """
        logger.info("Handling partial match scenario")
        
        # Find the primary contact from existing matches
        primary_contacts = [c for c in existing_contacts if c.is_primary()]
        
        if primary_contacts:
            # Use the first primary contact found
            primary_contact = primary_contacts[0]
            logger.info(f"Using existing primary contact: {primary_contact.id}")
        else:
            # All existing contacts are secondary, get their primary
            primary_contact = existing_contacts[0].get_primary_contact()
            logger.info(f"All matches are secondary, using their primary: {primary_contact.id}")
        
        # Check if we need to create a new secondary with additional info
        if self._needs_new_secondary(primary_contact, email, phone):
            logger.info("Creating new secondary contact for partial match")
            self._create_secondary_contact(session, primary_contact.id, email, phone)
        else:
            logger.info("No new information to add for partial match")
        
        return primary_contact
    
    def _validate_secondary_contact_data(self, email: Optional[str], phone: Optional[str], primary_contact: Contact) -> bool:
        """
        Validate that secondary contact data is different from primary contact
        Ensures we don't create duplicate secondary contacts with same info as primary
        """
        if not email and not phone:
            logger.warning("Cannot create secondary contact without email or phone")
            return False
        
        # Check if this would be identical to primary contact
        if (email == primary_contact.email and phone == primary_contact.phone_number):
            logger.info("Secondary contact data identical to primary - skipping creation")
            return False
        
        return True
    
    def _find_existing_secondary_with_data(self, session: Session, primary_id: int, 
                                         email: Optional[str], phone: Optional[str]) -> Optional[Contact]:
        """
        Find if a secondary contact with the exact same data already exists
        Prevents duplicate secondary contacts
        """
        query = session.query(Contact).filter(
            Contact.linked_id == primary_id,
            Contact.link_precedence == "secondary",
            Contact.deleted_at.is_(None)
        )
        
        # Add conditions based on provided data
        if email and phone:
            query = query.filter(Contact.email == email, Contact.phone_number == phone)
        elif email:
            query = query.filter(Contact.email == email, Contact.phone_number.is_(None))
        elif phone:
            query = query.filter(Contact.phone_number == phone, Contact.email.is_(None))
        
        existing = query.first()
        if existing:
            logger.info(f"Found existing secondary contact {existing.id} with same data")
        
        return existing
    
    def _analyze_contact_matches(self, existing_contacts: List[Contact], email: Optional[str], phone: Optional[str]) -> Dict[str, Any]:
        """
        Analyze what type of matches we have (email only, phone only, or both)
        This helps determine the best secondary contact strategy
        """
        email_matches = [c for c in existing_contacts if c.email == email] if email else []
        phone_matches = [c for c in existing_contacts if c.phone_number == phone] if phone else []
        
        analysis = {
            "email_matches": email_matches,
            "phone_matches": phone_matches,
            "email_only_matches": [c for c in email_matches if c not in phone_matches],
            "phone_only_matches": [c for c in phone_matches if c not in email_matches],
            "exact_matches": [c for c in email_matches if c in phone_matches],
            "total_unique_contacts": len(set([c.id for c in existing_contacts]))
        }
        
        logger.info(f"Contact match analysis: {len(email_matches)} email, {len(phone_matches)} phone, "
                   f"{len(analysis['exact_matches'])} exact matches")
        
        return analysis
    
    def _get_secondary_contact_summary(self, session: Session, primary_contact: Contact) -> Dict[str, int]:
        """
        Get a summary of secondary contacts linked to a primary contact
        Useful for logging and debugging secondary contact logic
        """
        secondaries = session.query(Contact).filter(
            Contact.linked_id == primary_contact.id,
            Contact.link_precedence == "secondary",
            Contact.deleted_at.is_(None)
        ).all()
        
        summary = {
            "total_secondaries": len(secondaries),
            "with_email_only": len([c for c in secondaries if c.email and not c.phone_number]),
            "with_phone_only": len([c for c in secondaries if c.phone_number and not c.email]),
            "with_both": len([c for c in secondaries if c.email and c.phone_number])
        }
        
        logger.info(f"Secondary contact summary for primary {primary_contact.id}: {summary}")
        return summary
    
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
        # Get all contacts linked to this primary using helper method
        all_linked_contacts = self._get_all_linked_contacts(session, primary_contact)
        
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
        
        # Log secondary contact summary for debugging
        self._get_secondary_contact_summary(session, primary_contact)
        
        logger.info(f"Built response for primary {primary.id} with {len(emails)} emails, {len(phone_numbers)} phones, {len(secondary_ids)} secondaries")
        
        return IdentifyResponse(contact=contact_response)

# Global service instance
identity_service = IdentityService() 