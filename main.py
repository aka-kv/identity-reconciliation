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
    """Handle Pydantic validation errors"""
    logger.warning(f"Validation error for {request.url}: {exc}")
    
    error_details = []
    for error in exc.errors():
        error_details.append({
            "field": " -> ".join(str(x) for x in error["loc"]),
            "message": error["msg"],
            "type": error["type"]
        })
    
    error_response = ErrorResponse(
        error="ValidationError",
        message="Request validation failed",
        details={"errors": error_details}
    )
    
    return JSONResponse(
        status_code=400,
        content=error_response.dict()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors"""
    logger.error(f"Unexpected error for {request.url}: {exc}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    
    error_response = ErrorResponse(
        error="InternalServerError",
        message="An unexpected error occurred"
    )
    
    return JSONResponse(
        status_code=500,
        content=error_response.dict()
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
    Health check endpoint for monitoring
    """
    try:
        from database import db_manager
        db_status = "connected" if db_manager.test_connection() else "disconnected"
    except Exception as e:
        db_status = f"error: {str(e)[:50]}"
    
    return {
        "status": "healthy",
        "environment": settings.ENVIRONMENT,
        "database": db_status
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
    """
    try:
        logger.info(f"Processing identify request: email={request.email}, phone={request.phoneNumber}")
        
        # Process the request through our identity service
        response = await identity_service.identify_contact(request)
        
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    ) 