"""
Main FastAPI application entry point for Identity Reconciliation API
This file serves as the primary entry point for the FastAPI application
with the core /identify endpoint for customer identity reconciliation.
Includes error handling, middleware, and business logic integration.
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
import logging
import traceback

from config import settings
from schemas import IdentifyRequest, IdentifyResponse, ErrorResponse
from services import identity_service

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI application instance
app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION,
    debug=settings.DEBUG
)

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Exception handlers
@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    """Handle Pydantic validation errors with detailed field information"""
    logger.warning(f"Validation error for {request.url}: {exc}")
    
    error_details = []
    for error in exc.errors():
        error_details.append({
            "field": " -> ".join(str(x) for x in error["loc"]),
            "message": error["msg"],
            "type": error["type"],
            "input": error.get("input")
        })
    
    error_response = ErrorResponse(
        error="ValidationError",
        message="Request validation failed",
        details={"errors": error_details, "total_errors": len(error_details)}
    )
    
    return JSONResponse(
        status_code=400,
        content=error_response.dict(),
        headers={"X-Error-Type": "ValidationError"}
    )

@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    """Handle ValueError exceptions from business logic"""
    logger.warning(f"Value error for {request.url}: {exc}")
    
    error_response = ErrorResponse(
        error="BusinessLogicError",
        message=str(exc),
        details={"error_type": "ValueError"}
    )
    
    return JSONResponse(
        status_code=422,
        content=error_response.dict(),
        headers={"X-Error-Type": "BusinessLogicError"}
    )

@app.exception_handler(ConnectionError)
async def connection_error_handler(request: Request, exc: ConnectionError):
    """Handle database connection errors"""
    logger.error(f"Database connection error for {request.url}: {exc}")
    
    error_response = ErrorResponse(
        error="DatabaseConnectionError",
        message="Database is currently unavailable. Please try again later.",
        details={"error_type": "ConnectionError"}
    )
    
    return JSONResponse(
        status_code=503,
        content=error_response.dict(),
        headers={"X-Error-Type": "DatabaseConnectionError", "Retry-After": "30"}
    )

@app.exception_handler(TimeoutError)
async def timeout_error_handler(request: Request, exc: TimeoutError):
    """Handle request timeout errors"""
    logger.error(f"Timeout error for {request.url}: {exc}")
    
    error_response = ErrorResponse(
        error="RequestTimeoutError",
        message="Request timed out. Please try again.",
        details={"error_type": "TimeoutError"}
    )
    
    return JSONResponse(
        status_code=408,
        content=error_response.dict(),
        headers={"X-Error-Type": "RequestTimeoutError"}
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle FastAPI HTTP exceptions with consistent error format"""
    logger.warning(f"HTTP exception for {request.url}: {exc.status_code} - {exc.detail}")
    
    # If detail is already an ErrorResponse dict, use it as is
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.detail,
            headers=getattr(exc, "headers", {})
        )
    
    # Otherwise, wrap in ErrorResponse format
    error_response = ErrorResponse(
        error="HTTPException",
        message=str(exc.detail),
        details={"status_code": exc.status_code}
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=error_response.dict(),
        headers=getattr(exc, "headers", {})
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors with detailed logging"""
    error_id = f"error_{hash(str(exc))}"
    logger.error(f"Unexpected error [{error_id}] for {request.url}: {exc}")
    logger.error(f"Traceback [{error_id}]: {traceback.format_exc()}")
    
    # Check for specific error types in the exception message
    error_str = str(exc).lower()
    if any(db_error in error_str for db_error in ['connect', 'timeout', 'connection', 'database']):
        error_response = ErrorResponse(
            error="DatabaseError",
            message="Database operation failed. Please try again later.",
            details={"error_id": error_id, "error_type": type(exc).__name__}
        )
        status_code = 503
    else:
        error_response = ErrorResponse(
            error="InternalServerError",
            message="An unexpected error occurred. Please contact support if the issue persists.",
            details={"error_id": error_id, "error_type": type(exc).__name__}
        )
        status_code = 500
    
    return JSONResponse(
        status_code=status_code,
        content=error_response.dict(),
        headers={"X-Error-ID": error_id, "X-Error-Type": "InternalServerError"}
    )

@app.get("/")
async def root():
    """
    Root endpoint that returns basic API information
    """
    return {
        "message": "Identity Reconciliation API",
        "version": settings.API_VERSION,
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """
    Health check endpoint for monitoring and load balancer health checks
    """
    try:
        from database import db_manager
        db_status = "connected" if db_manager.test_connection() else "disconnected"
    except Exception as e:
        db_status = f"error: {str(e)[:50]}"
    
    return {
        "status": "healthy",
        "environment": settings.ENVIRONMENT,
        "database": db_status,
        "version": settings.API_VERSION,
        "endpoints": {
            "identify": "/identify",
            "health": "/health", 
            "docs": "/docs"
        }
    }

@app.get("/debug/contacts")
async def debug_contacts():
    """
    Debug endpoint to check database contents and recent contacts
    """
    try:
        from database import db_manager
        
        with db_manager.get_session() as session:
            from models import Contact
            
            # Count total contacts
            total_contacts = session.query(Contact).filter(Contact.deleted_at.is_(None)).count()
            primary_count = session.query(Contact).filter(
                Contact.link_precedence == "primary",
                Contact.deleted_at.is_(None)
            ).count()
            secondary_count = session.query(Contact).filter(
                Contact.link_precedence == "secondary", 
                Contact.deleted_at.is_(None)
            ).count()
            
            # Get recent contacts
            recent_contacts = session.query(Contact).filter(
                Contact.deleted_at.is_(None)
            ).order_by(Contact.created_at.desc()).limit(5).all()
            
            return {
                "total_contacts": total_contacts,
                "primary_contacts": primary_count,
                "secondary_contacts": secondary_count,
                "recent_contacts": [
                    {
                        "id": c.id,
                        "email": c.email,
                        "phone": c.phone_number,
                        "precedence": c.link_precedence,
                        "linked_id": c.linked_id,
                        "created_at": c.created_at.isoformat() if c.created_at else None
                    } for c in recent_contacts
                ],
                "database_status": "connected"
            }
            
    except Exception as e:
        logger.error(f"Debug endpoint error: {e}")
        return {
            "error": str(e),
            "total_contacts": 0,
            "recent_contacts": [],
            "database_status": "error"
        }

@app.post("/identify", response_model=IdentifyResponse)
async def identify_endpoint(request: IdentifyRequest):
    """
    Main identity reconciliation endpoint
    
    Links customer identities based on email and/or phone number.
    Returns consolidated contact information including all linked emails,
    phone numbers, and secondary contact IDs.
    
    **Algorithm:**
    1. Find existing contacts matching email or phone
    2. If no matches → create new primary contact
    3. If matches found → determine linking strategy:
       - New information → create secondary contact
       - Link two primaries → convert newer to secondary
    4. Return consolidated contact information
    
    **Examples:**
    - New customer: Creates primary contact
    - Existing email + new phone: Creates secondary contact
    - Two existing primaries with shared info: Links them (older remains primary)
    """
    try:
        logger.info(f"Processing identify request: email={request.email}, phone={request.phoneNumber}")
        
        # Validate request data one more time for endpoint-specific checks
        await _validate_identify_request(request)
        
        # Process the request through our identity service
        response = await identity_service.identify_contact(request)
        
        # Validate response before returning
        _validate_identify_response(response)
        
        logger.info(f"Successfully processed request. Primary contact ID: {response.contact.primaryContatId}")
        
        return response
        
    except ValidationError as e:
        # This should be caught by the exception handler, but just in case
        logger.warning(f"Validation error in identify endpoint: {e}")
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                error="ValidationError",
                message="Invalid request data",
                details={"validation_errors": e.errors()}
            ).dict()
        )
    
    except Exception as e:
        # Check if this is a database connection error
        error_str = str(e).lower()
        if any(db_error in error_str for db_error in ['connect', 'timeout', 'connection', 'database']):
            logger.error(f"Database connection error in identify endpoint: {e}")
            raise HTTPException(
                status_code=503,
                detail=ErrorResponse(
                    error="DatabaseConnectionError",
                    message="Database is currently unavailable. Please try again later."
                ).dict()
            )
        
        # Log the full error for debugging
        logger.error(f"Error in identify endpoint: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Return generic error to client
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                error="InternalServerError",
                message="Unable to process identity reconciliation request"
            ).dict()
        )

@app.post("/identify/test", response_model=IdentifyResponse)
async def identify_test_endpoint(request: IdentifyRequest):
    """
    Test version of identify endpoint for local testing and debugging
    Includes additional logging and validation steps
    """
    logger.info(f"TEST MODE: Processing identify request: email={request.email}, phone={request.phoneNumber}")
    
    try:
        # Enhanced logging for test mode
        from database import db_manager
        with db_manager.get_session() as session:
            from models import Contact
            existing_count = session.query(Contact).filter(Contact.deleted_at.is_(None)).count()
            logger.info(f"TEST MODE: Current database has {existing_count} active contacts")
        
        # Process request
        response = await identity_service.identify_contact(request)
        
        # Additional test mode logging
        logger.info(f"TEST MODE: Response generated with primary ID {response.contact.primaryContatId}")
        logger.info(f"TEST MODE: Response emails: {response.contact.emails}")
        logger.info(f"TEST MODE: Response phones: {response.contact.phoneNumbers}")
        logger.info(f"TEST MODE: Secondary IDs: {response.contact.secondaryContactIds}")
        
        return response
        
    except Exception as e:
        logger.error(f"TEST MODE: Error processing request: {e}")
        raise

async def _validate_identify_request(request: IdentifyRequest):
    """
    Additional validation for identify endpoint requests
    """
    # Check that at least one contact method is provided (already in schema, but double-check)
    if not request.email and not request.phoneNumber:
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                error="ValidationError",
                message="At least one of email or phoneNumber must be provided"
            ).dict()
        )
    
    # Additional business logic validation can be added here
    logger.debug(f"Request validation passed for email={request.email}, phone={request.phoneNumber}")

def _validate_identify_response(response: IdentifyResponse):
    """
    Validate identify response before returning to client
    """
    # Basic response structure validation
    if not response.contact:
        raise ValueError("Response missing contact information")
    
    if not response.contact.primaryContatId:
        raise ValueError("Response missing primary contact ID")
    
    # Ensure arrays are not None
    if response.contact.emails is None:
        response.contact.emails = []
    if response.contact.phoneNumbers is None:
        response.contact.phoneNumbers = []
    if response.contact.secondaryContactIds is None:
        response.contact.secondaryContactIds = []
    
    logger.debug(f"Response validation passed for primary ID {response.contact.primaryContatId}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    ) 