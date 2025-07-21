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
import traceback
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
    
    def _validate_identify_request(self, request: IdentifyRequest):
        """
        Validate identify request data before processing
        """
        if not request.email and not request.phoneNumber:
            raise ValueError("At least one of email or phoneNumber must be provided")
        
        if request.email and len(request.email.strip()) == 0:
            raise ValueError("Email cannot be empty string")
        
        if request.phoneNumber and len(request.phoneNumber.strip()) == 0:
            raise ValueError("Phone number cannot be empty string")
        
        logger.debug(f"Request validation passed for email={request.email}, phone={request.phoneNumber}")
    
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
        
        try:
            # Validate input data
            self._validate_identify_request(request)
            
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
                    # Multiple primary contacts found - need to link them with complex scenario handling
                    logger.info(f"Found {len(primary_contacts)} primary contacts - initiating complex linking process")
                    primary_contact = self._handle_complex_linking_scenario(session, primary_contacts, request.email, request.phoneNumber)
                    
                    # Ensure data integrity after complex conversion
                    self._ensure_data_integrity_after_conversion(session, primary_contact)
                    
                    return self._build_response(session, primary_contact)
        
        except ValueError as e:
            logger.error(f"Validation error in identify_contact: {e}")
            raise
        except ConnectionError as e:
            logger.error(f"Database connection error in identify_contact: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in identify_contact: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
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
        Link multiple primary contacts by converting newer ones to secondary
        Handles complex scenarios including existing secondary contacts
        """
        if len(primary_contacts) < 2:
            logger.warning("_link_primary_contacts called with less than 2 primary contacts")
            return primary_contacts[0] if primary_contacts else None
        
        # Sort by creation date - oldest becomes the primary
        primary_contacts.sort(key=lambda c: c.created_at)
        primary_contact = primary_contacts[0]
        contacts_to_convert = primary_contacts[1:]
        
        logger.info(f"Linking {len(primary_contacts)} primary contacts - keeping {primary_contact.id} as primary (oldest: {primary_contact.created_at})")
        
        # Convert newer primaries to secondary and handle their existing secondaries
        for contact in contacts_to_convert:
            logger.info(f"Converting primary contact {contact.id} to secondary (created: {contact.created_at})")
            
            # First, handle any existing secondary contacts linked to this primary
            self._relocate_secondary_contacts(session, contact.id, primary_contact.id)
            
            # Now convert this primary to secondary
            contact.link_precedence = "secondary"
            contact.linked_id = primary_contact.id
            contact.updated_at = datetime.utcnow()
            
            logger.info(f"Successfully converted contact {contact.id} from primary to secondary")
        
        # Check if we need to add new information as another secondary
        if self._needs_new_secondary(primary_contact, email, phone):
            logger.info("Adding new secondary contact with additional info")
            self._create_secondary_contact(session, primary_contact.id, email, phone)
        
        return primary_contact
    
    def _relocate_secondary_contacts(self, session: Session, old_primary_id: int, new_primary_id: int):
        """
        Move all secondary contacts from old primary to new primary
        This ensures no secondary contacts are orphaned during primary-to-secondary conversion
        """
        secondary_contacts = session.query(Contact).filter(
            Contact.linked_id == old_primary_id,
            Contact.link_precedence == "secondary",
            Contact.deleted_at.is_(None)
        ).all()
        
        if secondary_contacts:
            logger.info(f"Relocating {len(secondary_contacts)} secondary contacts from primary {old_primary_id} to {new_primary_id}")
            
            for secondary in secondary_contacts:
                logger.info(f"Moving secondary contact {secondary.id} from primary {old_primary_id} to {new_primary_id}")
                secondary.linked_id = new_primary_id
                secondary.updated_at = datetime.utcnow()
        else:
            logger.info(f"No secondary contacts to relocate from primary {old_primary_id}")
    
    def _validate_primary_to_secondary_conversion(self, session: Session, contact: Contact, new_primary_id: int) -> bool:
        """
        Validate that primary-to-secondary conversion is safe and correct
        """
        if not contact.is_primary():
            logger.error(f"Cannot convert non-primary contact {contact.id} to secondary")
            return False
        
        if contact.id == new_primary_id:
            logger.error(f"Cannot link contact {contact.id} to itself")
            return False
        
        # Check that new primary exists and is actually primary
        new_primary = session.query(Contact).filter(Contact.id == new_primary_id).first()
        if not new_primary:
            logger.error(f"New primary contact {new_primary_id} not found")
            return False
        
        if not new_primary.is_primary():
            logger.error(f"Target contact {new_primary_id} is not a primary contact")
            return False
        
        return True
    
    def _perform_primary_to_secondary_conversion(self, session: Session, contact: Contact, new_primary_id: int):
        """
        Perform the actual primary-to-secondary conversion with validation
        """
        if not self._validate_primary_to_secondary_conversion(session, contact, new_primary_id):
            logger.error(f"Primary-to-secondary conversion validation failed for contact {contact.id}")
            return False
        
        old_id = contact.id
        logger.info(f"Converting primary contact {old_id} to secondary linked to {new_primary_id}")
        
        # Move any existing secondary contacts first
        self._relocate_secondary_contacts(session, old_id, new_primary_id)
        
        # Convert the primary to secondary
        contact.link_precedence = "secondary"
        contact.linked_id = new_primary_id
        contact.updated_at = datetime.utcnow()
        
        logger.info(f"Successfully converted primary {old_id} to secondary linked to {new_primary_id}")
        return True
    
    def _handle_complex_linking_scenario(self, session: Session, primary_contacts: List[Contact], 
                                       email: Optional[str], phone: Optional[str]) -> Contact:
        """
        Handle complex scenarios where multiple primaries need to be linked
        Ensures proper hierarchy and data integrity during conversion
        """
        if len(primary_contacts) < 2:
            return primary_contacts[0] if primary_contacts else None
        
        # Analyze each primary contact's existing secondary network
        primary_analysis = []
        for primary in primary_contacts:
            secondaries = session.query(Contact).filter(
                Contact.linked_id == primary.id,
                Contact.link_precedence == "secondary",
                Contact.deleted_at.is_(None)
            ).count()
            
            primary_analysis.append({
                "contact": primary,
                "secondary_count": secondaries,
                "created_at": primary.created_at
            })
            
            logger.info(f"Primary {primary.id} has {secondaries} secondary contacts (created: {primary.created_at})")
        
        # Sort by creation date (oldest first, then by secondary count if needed)
        primary_analysis.sort(key=lambda x: (x["created_at"], -x["secondary_count"]))
        
        # The oldest primary becomes the main primary
        main_primary = primary_analysis[0]["contact"]
        primaries_to_convert = [p["contact"] for p in primary_analysis[1:]]
        
        logger.info(f"Complex linking: {main_primary.id} remains primary, converting {len(primaries_to_convert)} primaries")
        
        # Convert each primary to secondary, handling their secondary networks
        for primary_to_convert in primaries_to_convert:
            success = self._perform_primary_to_secondary_conversion(session, primary_to_convert, main_primary.id)
            if not success:
                logger.error(f"Failed to convert primary {primary_to_convert.id} to secondary")
        
        return main_primary
    
    def _ensure_data_integrity_after_conversion(self, session: Session, primary_contact: Contact):
        """
        Verify data integrity after primary-to-secondary conversion
        Ensures no orphaned contacts or circular references
        """
        # Check for orphaned secondary contacts
        orphaned = session.query(Contact).filter(
            Contact.link_precedence == "secondary",
            Contact.linked_id.notin_(
                session.query(Contact.id).filter(
                    Contact.link_precedence == "primary",
                    Contact.deleted_at.is_(None)
                )
            ),
            Contact.deleted_at.is_(None)
        ).all()
        
        if orphaned:
            logger.warning(f"Found {len(orphaned)} orphaned secondary contacts")
            for contact in orphaned:
                logger.warning(f"Orphaned secondary: {contact.id} linked to non-existent primary {contact.linked_id}")
        
        # Check for circular references (though this shouldn't happen)
        all_linked = self._get_all_linked_contacts(session, primary_contact)
        contact_ids = {c.id for c in all_linked}
        
        for contact in all_linked:
            if contact.is_secondary() and contact.linked_id not in contact_ids:
                logger.error(f"Data integrity error: secondary {contact.id} linked to {contact.linked_id} outside contact group")
        
        logger.info(f"Data integrity check completed for primary {primary_contact.id}")
    
    def _build_response(self, session: Session, primary_contact: Contact) -> IdentifyResponse:
        """
        Build the consolidated response with all linked contact information
        Ensures primary contact data appears first and proper ordering per specification
        """
        # Get all contacts linked to this primary using helper method
        all_linked_contacts = self._get_all_linked_contacts(session, primary_contact)
        
        # Ensure primary contact is first, then secondaries by creation date
        primary = primary_contact
        secondaries = [c for c in all_linked_contacts if c.id != primary.id]
        secondaries.sort(key=lambda c: c.created_at)  # Consistent ordering
        
        # Collect all emails and phone numbers with proper consolidation
        emails = self._consolidate_emails(primary, secondaries)
        phone_numbers = self._consolidate_phone_numbers(primary, secondaries)
        
        # Get secondary contact IDs in creation order
        secondary_ids = [c.id for c in secondaries]
        
        # Build response with consolidated data
        contact_response = ContactResponse(
            primaryContatId=primary.id,
            emails=emails,
            phoneNumbers=phone_numbers,
            secondaryContactIds=secondary_ids
        )
        
        # Validate response format matches specification
        self._validate_response_format(contact_response, primary, secondaries)
        
        # Log consolidated response details
        self._log_response_details(primary, secondaries, emails, phone_numbers)
        
        return IdentifyResponse(contact=contact_response)
    
    def _consolidate_emails(self, primary: Contact, secondaries: List[Contact]) -> List[str]:
        """
        Consolidate emails ensuring primary contact email appears first
        Removes duplicates while preserving order
        """
        emails = []
        
        # Add primary contact email first (if exists)
        if primary.has_email():
            emails.append(primary.email)
            logger.debug(f"Added primary email: {primary.email}")
        
        # Add secondary contact emails (avoiding duplicates)
        for contact in secondaries:
            if contact.has_email() and contact.email not in emails:
                emails.append(contact.email)
                logger.debug(f"Added secondary email: {contact.email} from contact {contact.id}")
        
        logger.info(f"Consolidated {len(emails)} unique emails: {emails}")
        return emails
    
    def _consolidate_phone_numbers(self, primary: Contact, secondaries: List[Contact]) -> List[str]:
        """
        Consolidate phone numbers ensuring primary contact phone appears first
        Removes duplicates while preserving order
        """
        phone_numbers = []
        
        # Add primary contact phone first (if exists)
        if primary.has_phone():
            phone_numbers.append(primary.phone_number)
            logger.debug(f"Added primary phone: {primary.phone_number}")
        
        # Add secondary contact phones (avoiding duplicates)
        for contact in secondaries:
            if contact.has_phone() and contact.phone_number not in phone_numbers:
                phone_numbers.append(contact.phone_number)
                logger.debug(f"Added secondary phone: {contact.phone_number} from contact {contact.id}")
        
        logger.info(f"Consolidated {len(phone_numbers)} unique phone numbers: {phone_numbers}")
        return phone_numbers
    
    def _validate_response_format(self, response: ContactResponse, primary: Contact, secondaries: List[Contact]):
        """
        Validate that response format matches specification exactly
        Ensures data integrity and correct consolidation
        """
        # Validate primary contact ID
        if response.primaryContatId != primary.id:
            logger.error(f"Response primary ID mismatch: expected {primary.id}, got {response.primaryContatId}")
        
        # Validate that primary contact info appears first if present
        if primary.has_email() and response.emails and response.emails[0] != primary.email:
            logger.warning(f"Primary email not first in response: expected {primary.email}, got {response.emails[0]}")
        
        if primary.has_phone() and response.phoneNumbers and response.phoneNumbers[0] != primary.phone_number:
            logger.warning(f"Primary phone not first in response: expected {primary.phone_number}, got {response.phoneNumbers[0]}")
        
        # Validate secondary contact IDs are correct
        expected_secondary_ids = {c.id for c in secondaries}
        actual_secondary_ids = set(response.secondaryContactIds)
        
        if expected_secondary_ids != actual_secondary_ids:
            logger.error(f"Secondary contact IDs mismatch: expected {expected_secondary_ids}, got {actual_secondary_ids}")
        
        # Validate no duplicates in arrays
        if len(response.emails) != len(set(response.emails)):
            logger.error(f"Duplicate emails in response: {response.emails}")
        
        if len(response.phoneNumbers) != len(set(response.phoneNumbers)):
            logger.error(f"Duplicate phone numbers in response: {response.phoneNumbers}")
        
        logger.debug("Response format validation completed")
    
    def _log_response_details(self, primary: Contact, secondaries: List[Contact], 
                            emails: List[str], phone_numbers: List[str]):
        """
        Log detailed response information for debugging and monitoring
        """
        logger.info(f"Response built for primary contact {primary.id}:")
        logger.info(f"  Primary info: {primary.get_contact_info_summary()}")
        logger.info(f"  Secondary contacts: {len(secondaries)} ({[c.id for c in secondaries]})")
        logger.info(f"  Consolidated emails: {len(emails)} ({emails})")
        logger.info(f"  Consolidated phones: {len(phone_numbers)} ({phone_numbers})")
        
        # Log secondary contact details
        for secondary in secondaries:
            logger.debug(f"  Secondary {secondary.id}: {secondary.get_contact_info_summary()}")
        
        # Verify response completeness
        total_contacts = 1 + len(secondaries)  # primary + secondaries
        logger.info(f"Response consolidates data from {total_contacts} contacts into unified identity")

# Global service instance
identity_service = IdentityService() 