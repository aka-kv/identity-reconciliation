"""
Schema validation test script for Identity Reconciliation API
This script tests the Pydantic schemas to ensure proper validation
and error handling before implementing the full business logic.
"""

from schemas import IdentifyRequest, ContactResponse, IdentifyResponse, ErrorResponse
from pydantic import ValidationError
import json

def test_identify_request():
    """Test IdentifyRequest validation scenarios"""
    print("Testing IdentifyRequest validation...")
    
    # Valid requests
    valid_cases = [
        {"email": "test@example.com", "phoneNumber": "+1234567890"},
        {"email": "user@domain.org"},
        {"phoneNumber": "+91-987-654-3210"},
        {"phoneNumber": "1234567890"},  # Should normalize to +1234567890
    ]
    
    for case in valid_cases:
        try:
            request = IdentifyRequest(**case)
            print(f"✓ Valid: {case} -> {request.dict()}")
        except ValidationError as e:
            print(f"✗ Unexpected validation error for {case}: {e}")
    
    # Invalid requests
    invalid_cases = [
        {},  # No contact info
        {"email": "invalid-email"},
        {"email": "toolong" + "x" * 250 + "@example.com"},
        {"phoneNumber": "123"},  # Too short
        {"phoneNumber": "12345678901234567890"},  # Too long
    ]
    
    for case in invalid_cases:
        try:
            request = IdentifyRequest(**case)
            print(f"✗ Should have failed: {case}")
        except ValidationError as e:
            print(f"✓ Correctly rejected: {case}")
    
    print()

def test_response_schemas():
    """Test response schema creation"""
    print("Testing response schemas...")
    
    try:
        # Test ContactResponse
        contact = ContactResponse(
            primaryContatId=1,
            emails=["primary@example.com", "secondary@example.com"],
            phoneNumbers=["+1234567890", "+0987654321"],
            secondaryContactIds=[2, 3, 4]
        )
        print(f"✓ ContactResponse: {contact.dict()}")
        
        # Test IdentifyResponse
        response = IdentifyResponse(contact=contact)
        print(f"✓ IdentifyResponse: {response.dict()}")
        
        # Test ErrorResponse
        error = ErrorResponse(
            error="ValidationError",
            message="Test error message",
            details={"field": "email", "issue": "invalid format"}
        )
        print(f"✓ ErrorResponse: {error.dict()}")
        
    except Exception as e:
        print(f"✗ Error creating response schemas: {e}")
    
    print()

def test_json_serialization():
    """Test JSON serialization of schemas"""
    print("Testing JSON serialization...")
    
    try:
        # Create sample data
        request = IdentifyRequest(email="test@example.com", phoneNumber="+1234567890")
        contact = ContactResponse(
            primaryContatId=1,
            emails=["test@example.com"],
            phoneNumbers=["+1234567890"],
            secondaryContactIds=[]
        )
        response = IdentifyResponse(contact=contact)
        
        # Test serialization
        request_json = request.json()
        response_json = response.json()
        
        print(f"✓ Request JSON: {request_json}")
        print(f"✓ Response JSON: {response_json}")
        
        # Test deserialization
        request_back = IdentifyRequest.parse_raw(request_json)
        response_back = IdentifyResponse.parse_raw(response_json)
        
        print("✓ JSON round-trip successful")
        
    except Exception as e:
        print(f"✗ JSON serialization error: {e}")
    
    print()

def main():
    """Run all schema tests"""
    print("Identity Reconciliation API - Schema Validation Tests")
    print("=" * 60)
    
    test_identify_request()
    test_response_schemas()
    test_json_serialization()
    
    print("Schema testing completed!")

if __name__ == "__main__":
    main() 