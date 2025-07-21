"""
Business logic services for Identity Reconciliation API
Contains core identity reconciliation algorithms and data processing
services for customer contact management and linking.
"""

from .identity_service import IdentityService, identity_service

# Export all services for easy importing
__all__ = [
    "IdentityService",
    "identity_service"
] 