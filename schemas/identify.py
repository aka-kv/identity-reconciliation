"""
Pydantic schemas for Identity Reconciliation API
This module defines request and response schemas for the /identify endpoint
with comprehensive validation for email and phone number formats.
Compatible with Python 3.7 and includes detailed error responses.
"""

from pydantic import BaseModel, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import re

class IdentifyRequest(BaseModel):
    """
    Request schema for the /identify endpoint
    
    Validates customer contact information for identity reconciliation.
    At least one of email or phoneNumber must be provided.
    """
    
    email: Optional[str] = None
    phoneNumber: Optional[str] = None
    
    @validator('email')
    def validate_email(cls, value):
        """
        Validate email format and normalize it
        Uses basic email validation compatible with most systems
        """
        if value is None:
            return None
            
        # Basic email validation regex (more permissive than EmailStr)
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        # Normalize email to lowercase
        value = value.strip().lower()
        
        if not re.match(email_pattern, value):
            raise ValueError('Invalid email format')
            
        # Check reasonable length limits
        if len(value) > 255:
            raise ValueError('Email address too long (max 255 characters)')
            
        return value
    
    @validator('phoneNumber')
    def validate_phone_number(cls, value):
        """
        Validate and normalize phone number format
        Accepts various formats and normalizes to international format
        """
        if value is None:
            return None
            
        # Remove all non-digit characters
        digits_only = re.sub(r'[^\d]', '', value)
        
        # Check for reasonable length (7-15 digits as per ITU-T E.164)
        if len(digits_only) < 7:
            raise ValueError('Phone number too short (minimum 7 digits)')
        if len(digits_only) > 15:
            raise ValueError('Phone number too long (maximum 15 digits)')
            
        # Add '+' prefix if not present for international format
        if not value.startswith('+'):
            normalized = '+' + digits_only
        else:
            normalized = '+' + digits_only
            
        return normalized
    
    @validator('phoneNumber', 'email', always=True)
    def validate_at_least_one_contact(cls, value, values):
        """
        Ensure at least one contact method is provided
        This validator runs after individual field validation
        """
        # Only run this validation if we're validating the second field
        if len(values) >= 1:  # Both fields have been processed
            email = values.get('email')
            phone = value if 'phoneNumber' in values else values.get('phoneNumber')
            
            if not email and not phone:
                raise ValueError('At least one of email or phoneNumber must be provided')
                
        return value
    
    class Config:
        """Pydantic configuration"""
        # Generate example for API documentation
        schema_extra = {
            "example": {
                "email": "customer@example.com",
                "phoneNumber": "+1234567890"
            }
        }

class ContactResponse(BaseModel):
    """
    Response schema for individual contact information
    Represents a single contact with all associated data
    """
    
    primaryContatId: int  # Note: keeping typo from original spec for compatibility
    emails: List[str] = []
    phoneNumbers: List[str] = []
    secondaryContactIds: List[int] = []
    
    class Config:
        """Pydantic configuration"""
        schema_extra = {
            "example": {
                "primaryContatId": 1,
                "emails": ["customer@example.com"],
                "phoneNumbers": ["+1234567890"],
                "secondaryContactIds": [2, 3]
            }
        }

class IdentifyResponse(BaseModel):
    """
    Main response schema for the /identify endpoint
    Contains the consolidated contact information after reconciliation
    """
    
    contact: ContactResponse
    
    class Config:
        """Pydantic configuration"""
        schema_extra = {
            "example": {
                "contact": {
                    "primaryContatId": 1,
                    "emails": ["customer@example.com", "customer.alt@example.com"],
                    "phoneNumbers": ["+1234567890", "+0987654321"],
                    "secondaryContactIds": [2, 3]
                }
            }
        }

class ErrorDetail(BaseModel):
    """
    Schema for individual error details
    Used in validation and other error responses
    """
    
    field: Optional[str] = None
    message: str
    type: Optional[str] = None
    
class ErrorResponse(BaseModel):
    """
    Standard error response schema
    Provides consistent error format across all endpoints
    """
    
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = None
    
    def __init__(self, **data):
        """Initialize with current timestamp if not provided"""
        if 'timestamp' not in data:
            data['timestamp'] = datetime.utcnow()
        super().__init__(**data)
    
    class Config:
        """Pydantic configuration"""
        schema_extra = {
            "example": {
                "error": "ValidationError",
                "message": "Request validation failed",
                "details": {
                    "errors": [
                        {
                            "field": "email",
                            "message": "Invalid email format",
                            "type": "value_error"
                        }
                    ]
                },
                "timestamp": "2024-01-15T10:30:00.000Z"
            }
        } 