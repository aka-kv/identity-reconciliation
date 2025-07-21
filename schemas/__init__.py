"""
Pydantic schemas for Identity Reconciliation API
Contains request/response models and data validation schemas
for API endpoints and data transfer objects.
"""

from .identify import (
    IdentifyRequest,
    ContactResponse, 
    IdentifyResponse,
    ErrorDetail,
    ErrorResponse
)

# Export all schemas for easy importing
__all__ = [
    "IdentifyRequest",
    "ContactResponse",
    "IdentifyResponse", 
    "ErrorDetail",
    "ErrorResponse"
] 