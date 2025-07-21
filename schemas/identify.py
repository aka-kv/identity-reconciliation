"""
Pydantic schemas for the /identify endpoint
Handles request validation and response serialization
Fixed to properly handle "null" strings as null values
"""

import re
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, EmailStr, field_validator, model_validator


class IdentifyRequest(BaseModel):
    """
    Request schema for the /identify endpoint
    Validates that at least one of email or phoneNumber is provided
    Handles "null" strings by converting them to None
    """
    email: Optional[Union[EmailStr, str]] = Field(
        None,
        description="Customer email address",
        examples=["customer@example.com", None]
    )
    phoneNumber: Optional[str] = Field(
        None,
        alias="phoneNumber",  # API uses camelCase
        description="Customer phone number",
        examples=["+1234567890", "123-456-7890", None]
    )
    
    @field_validator('email', mode='before')
    @classmethod
    def validate_email(cls, v) -> Optional[str]:
        """
        Validate and clean email input
        Converts "null" strings to None and validates proper emails
        """
        # Handle null/None cases
        if v is None:
            return None
        
        # Convert "null" string to None (case insensitive)
        if isinstance(v, str) and v.lower().strip() in ['null', '']:
            return None
        
        # If it's a string, validate it as an email
        if isinstance(v, str):
            # Basic email validation (Pydantic will do the heavy lifting)
            v = v.strip()
            if '@' not in v:
                raise ValueError('Invalid email format: email must contain @')
            return v
        
        return v
    
    @field_validator('phoneNumber', mode='before')
    @classmethod
    def validate_phone_number(cls, v) -> Optional[str]:
        """
        Validate phone number format
        Converts "null" strings to None and validates phone numbers
        """
        # Handle null/None cases
        if v is None:
            return None
        
        # Convert "null" string to None (case insensitive)
        if isinstance(v, str) and v.lower().strip() in ['null', '']:
            return None
        
        # Convert to string if it's a number
        if isinstance(v, (int, float)):
            v = str(int(v))  # Remove decimal point if it's a float
        
        if not isinstance(v, str):
            raise ValueError('Phone number must be a string or number')
        
        # Clean the phone number
        v = v.strip()
        if not v:  # Empty string after stripping
            return None
        
        # Remove all non-digit characters except +
        cleaned = re.sub(r'[^\d+]', '', v)
        
        # Basic validation: should have at least 3 digits (flexible for testing)
        digits_only = re.sub(r'[^\d]', '', cleaned)
        if len(digits_only) < 3:
            raise ValueError('Phone number must contain at least 3 digits')
        
        # Return the original format (we'll store it as provided)
        return v
    
    @model_validator(mode='after')
    def validate_at_least_one_field(self):
        """
        Ensure at least one of email or phoneNumber is provided
        """
        if not self.email and not self.phoneNumber:
            raise ValueError('Either email or phoneNumber must be provided')
        return self
    
    class Config:
        # Allow both camelCase (API) and snake_case (Python) field names
        populate_by_name = True
        json_schema_extra = {
            "examples": [
                {
                    "email": "customer@example.com",
                    "phoneNumber": "+1234567890"
                },
                {
                    "email": "customer@example.com",
                    "phoneNumber": None
                },
                {
                    "email": None,
                    "phoneNumber": "123-456-7890"
                },
                {
                    "email": "null",
                    "phoneNumber": "123456"
                }
            ]
        }


class ContactResponse(BaseModel):
    """
    Contact information in the API response
    Contains consolidated contact data for a customer
    """
    primaryContatId: int = Field(
        alias="primaryContatId",  # Note: This matches the API spec typo
        description="ID of the primary contact"
    )
    emails: List[str] = Field(
        description="All email addresses associated with this contact",
        examples=[["customer@example.com", "customer2@example.com"]]
    )
    phoneNumbers: List[str] = Field(
        description="All phone numbers associated with this contact",
        examples=[["+1234567890", "123-456-7890"]]
    )
    secondaryContactIds: List[int] = Field(
        description="IDs of all secondary contacts linked to the primary",
        examples=[[2, 3, 4]]
    )
    
    class Config:
        populate_by_name = True


class IdentifyResponse(BaseModel):
    """
    Response schema for the /identify endpoint
    Contains the consolidated contact information
    """
    contact: ContactResponse = Field(
        description="Consolidated contact information"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "contact": {
                    "primaryContatId": 1,
                    "emails": ["customer@example.com", "customer2@example.com"],
                    "phoneNumbers": ["+1234567890", "123-456-7890"],
                    "secondaryContactIds": [2, 3]
                }
            }
        }


class ErrorResponse(BaseModel):
    """
    Error response schema for API errors
    """
    error: str = Field(
        description="Error type or category"
    )
    message: str = Field(
        description="Human-readable error message"
    )
    details: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional error details"
    )
    
    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "error": "ValidationError",
                    "message": "Either email or phoneNumber must be provided",
                    "details": {"field": "root"}
                },
                {
                    "error": "DatabaseError", 
                    "message": "Unable to connect to database"
                }
            ]
        }