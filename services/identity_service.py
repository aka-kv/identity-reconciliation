"""
Identity Service - Core business logic for identity reconciliation
Handles contact linking, primary/secondary relationships, and response building
Fixed to ensure primary contact info appears first in response arrays
"""

from typing import Optional, List, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, and_
from sqlalchemy.orm import selectinload
from datetime import datetime

from models.contact import Contact
from schemas.identify import IdentifyRequest, IdentifyResponse, ContactResponse
from database import db_manager


class IdentityService:
    """
    Core service for identity reconciliation logic
    Handles all business rules for linking customer contacts
    """
    
    def __init__(self):
        self.db_manager = db_manager
    
    async def _ensure_relationships_loaded(self, session: AsyncSession, contact: Contact) -> Contact:
        """
        Ensure a contact has all its relationships properly loaded to avoid lazy loading issues
        """
        if contact.secondary_contacts is None or contact.primary_contact is None:
            # Refresh the contact with all relationships
            query = select(Contact).where(Contact.id == contact.id).options(
                selectinload(Contact.secondary_contacts),
                selectinload(Contact.primary_contact).selectinload(Contact.secondary_contacts)
            )
            result = await session.execute(query)
            return result.scalar_one()
        return contact
    
    async def identify_contact(self, request: IdentifyRequest) -> IdentifyResponse:
        """
        Main orchestration method for identity reconciliation
        
        Algorithm:
        1. Find existing contacts matching email or phone
        2. If no matches -> create new primary contact
        3. If matches found -> determine linking strategy
        4. Return consolidated contact information
        """
        async with self.db_manager.get_session() as session:
            # Step 1: Find related contacts
            existing_contacts = await self._find_related_contacts(
                session, request.email, request.phoneNumber
            )
            
            if not existing_contacts:
                # Step 2: No matches - create new primary contact
                new_contact = await self._create_primary_contact(
                    session, request.email, request.phoneNumber
                )
                await session.commit()
                return await self._build_consolidated_response(session, new_contact)
            
            # Step 3: Matches found - determine linking strategy
            primary_contact = await self._handle_contact_linking(
                session, existing_contacts, request.email, request.phoneNumber
            )
            await session.commit()
            
            # Step 4: Return consolidated response
            return await self._build_consolidated_response(session, primary_contact)
    
    async def _find_related_contacts(
        self, 
        session: AsyncSession, 
        email: Optional[str], 
        phone: Optional[str]
    ) -> List[Contact]:
        """
        Find all contacts that match the provided email or phone number
        Returns both primary and secondary contacts
        """
        conditions = []
        
        if email:
            conditions.append(Contact.email == email)
        if phone:
            conditions.append(Contact.phone_number == phone)
        
        if not conditions:
            return []
        
        # Find contacts matching email OR phone
        query = select(Contact).where(
            and_(
                or_(*conditions),
                Contact.deleted_at.is_(None)  # Only active contacts
            )
        ).options(
            selectinload(Contact.secondary_contacts),
            selectinload(Contact.primary_contact).selectinload(Contact.secondary_contacts)
        )
        
        result = await session.execute(query)
        contacts = result.scalars().all()
        
        # Also find all related contacts (primary and their secondaries)
        all_related = set(contacts)
        for contact in contacts:
            if contact.is_primary():
                # Add all secondary contacts
                all_related.update(contact.secondary_contacts)
            else:
                # Add primary contact and its other secondaries
                if contact.primary_contact:
                    all_related.add(contact.primary_contact)
                    all_related.update(contact.primary_contact.secondary_contacts)
        
        return list(all_related)
    
    async def _create_primary_contact(
        self, 
        session: AsyncSession, 
        email: Optional[str], 
        phone: Optional[str]
    ) -> Contact:
        """
        Create a new primary contact with the provided information
        """
        contact = Contact(
            email=email,
            phone_number=phone,
            linked_id=None,
            link_precedence="primary",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        session.add(contact)
        await session.flush()  # Get the ID
        return contact
    
    async def _handle_contact_linking(
        self,
        session: AsyncSession,
        existing_contacts: List[Contact],
        email: Optional[str],
        phone: Optional[str]
    ) -> Contact:
        """
        Handle linking logic when existing contacts are found
        
        Cases:
        1. Exact match -> return existing primary
        2. New information -> create secondary contact  
        3. Link two primaries -> convert newer to secondary
        """
        # Separate primary and secondary contacts
        primaries = [c for c in existing_contacts if c.is_primary()]
        secondaries = [c for c in existing_contacts if c.is_secondary()]
        
        # Check if this exact combination already exists
        exact_match = self._find_exact_match(existing_contacts, email, phone)
        if exact_match:
            # Return the primary contact for this match
            return exact_match if exact_match.is_primary() else exact_match.primary_contact
        
        if len(primaries) == 1:
            # Case 1: One primary found - create secondary with new info if needed
            primary = primaries[0]
            
            # Check if we need to create a secondary contact
            if self._has_new_information(primary, email, phone):
                await self._create_secondary_contact(session, primary, email, phone)
            
            return primary
        
        elif len(primaries) > 1:
            # Case 2: Multiple primaries - link them (convert newer to secondary)
            return await self._link_primary_contacts(session, primaries, email, phone)
        
        else:
            # Only secondaries found - return the primary of the first secondary
            # This shouldn't normally happen with proper data integrity
            return secondaries[0].primary_contact
    
    def _find_exact_match(
        self, 
        contacts: List[Contact], 
        email: Optional[str], 
        phone: Optional[str]
    ) -> Optional[Contact]:
        """
        Check if exact email+phone combination already exists
        """
        for contact in contacts:
            if contact.email == email and contact.phone_number == phone:
                return contact
        return None
    
    def _has_new_information(
        self, 
        primary: Contact, 
        email: Optional[str], 
        phone: Optional[str]
    ) -> bool:
        """
        Check if the request contains new information not in the primary contact
        """
        # Get all information from primary and its secondaries
        all_emails = {primary.email} if primary.email else set()
        all_phones = {primary.phone_number} if primary.phone_number else set()
        
        # Only iterate through secondary_contacts if the relationship is loaded
        if primary.secondary_contacts is not None:
            for secondary in primary.secondary_contacts:
                if secondary.email:
                    all_emails.add(secondary.email)
                if secondary.phone_number:
                    all_phones.add(secondary.phone_number)
        
        # Check if we have new information
        has_new_email = email and email not in all_emails
        has_new_phone = phone and phone not in all_phones
        
        return has_new_email or has_new_phone
    
    async def _create_secondary_contact(
        self,
        session: AsyncSession,
        primary: Contact,
        email: Optional[str],
        phone: Optional[str]
    ) -> Contact:
        """
        Create a secondary contact linked to the primary
        """
        secondary = Contact(
            email=email,
            phone_number=phone,
            linked_id=primary.id,
            link_precedence="secondary",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        session.add(secondary)
        await session.flush()
        return secondary
    
    async def _link_primary_contacts(
        self,
        session: AsyncSession,
        primaries: List[Contact],
        email: Optional[str],
        phone: Optional[str]
    ) -> Contact:
        """
        Link multiple primary contacts by converting newer ones to secondary
        The oldest primary remains primary
        """
        # Sort by created_at to find the oldest
        primaries.sort(key=lambda c: c.created_at)
        oldest_primary = primaries[0]
        newer_primaries = primaries[1:]
        
        # Convert newer primaries to secondaries
        for primary in newer_primaries:
            primary.linked_id = oldest_primary.id
            primary.link_precedence = "secondary"
            primary.updated_at = datetime.utcnow()
            
            # Ensure secondary_contacts relationship is loaded before accessing
            if primary.secondary_contacts is not None:
                # Also update any secondaries that were linked to this primary
                for secondary in primary.secondary_contacts:
                    secondary.linked_id = oldest_primary.id
                    secondary.updated_at = datetime.utcnow()
        
        # Create new secondary if we have new information
        if self._has_new_information(oldest_primary, email, phone):
            await self._create_secondary_contact(session, oldest_primary, email, phone)
        
        return oldest_primary
    
    async def _build_consolidated_response(
        self, 
        session: AsyncSession, 
        primary_contact: Contact
    ) -> IdentifyResponse:
        """
        Build the consolidated response with all contact information
        FIXED: Ensures primary contact info appears first in arrays as per requirements
        """
        # Refresh the primary contact to get updated relationships
        await session.refresh(primary_contact, ['secondary_contacts'])
        
        # Initialize arrays - primary contact info goes first
        emails = []
        phone_numbers = []
        secondary_ids = []
        
        # Add primary contact info FIRST (as required by API spec)
        if primary_contact.email:
            emails.append(primary_contact.email)
        if primary_contact.phone_number:
            phone_numbers.append(primary_contact.phone_number)
        
        # Collect secondary contact info (excluding duplicates)
        secondary_emails = []
        secondary_phones = []
        
        for secondary in primary_contact.secondary_contacts:
            # Add secondary contact ID
            secondary_ids.append(secondary.id)
            
            # Collect unique emails and phones from secondaries
            if secondary.email and secondary.email not in emails:
                secondary_emails.append(secondary.email)
            if secondary.phone_number and secondary.phone_number not in phone_numbers:
                secondary_phones.append(secondary.phone_number)
        
        # Sort only the secondary info for consistency, then append to primary info
        secondary_emails.sort()
        secondary_phones.sort()
        secondary_ids.sort()
        
        # Final arrays: primary info first, then sorted secondary info
        emails.extend(secondary_emails)
        phone_numbers.extend(secondary_phones)
        
        contact_response = ContactResponse(
            primaryContatId=primary_contact.id,
            emails=emails,
            phoneNumbers=phone_numbers,
            secondaryContactIds=secondary_ids
        )
        
        return IdentifyResponse(contact=contact_response)


# Global service instance
identity_service = IdentityService()