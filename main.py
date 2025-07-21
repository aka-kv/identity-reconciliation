"""
Main FastAPI application entry point for Identity Reconciliation System
This file sets up the FastAPI application with proper configuration,
middleware, and basic health check endpoints. It serves as the main
entry point for both local development and AWS Lambda deployment.
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
import logging
import traceback
from datetime import datetime

from schemas.identify import IdentifyRequest, IdentifyResponse, ErrorResponse
from services.identity_service import identity_service
from config import settings

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
        content=error_response.model_dump()
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
        content=error_response.model_dump()
    )

@app.get("/")
async def root():
    """
    Root endpoint that returns basic API information
    """
    return {
        "message": "Identity Reconciliation API is running",
        "version": settings.API_VERSION,
        "environment": settings.ENVIRONMENT
    }

@app.get("/health")
async def health_check():
    """
    Health check endpoint for monitoring and load balancer health checks
    """
    # Test database connection
    db_status = "unknown"
    db_error = None
    try:
        from database import db_manager
        if await db_manager.test_connection():
            db_status = "connected"
        else:
            db_status = "disconnected"
    except Exception as e:
        db_status = "error"
        db_error = str(e)[:100]
    
    response = {
        "status": "healthy",
        "environment": settings.ENVIRONMENT,
        "version": settings.API_VERSION,
        "timestamp": datetime.utcnow().isoformat(),
        "lambda": settings.is_lambda_environment(),
        "database": {
            "status": db_status,
            "rds_configured": bool(settings.RDS_HOSTNAME and settings.RDS_HOSTNAME != "localhost"),
            "hostname": settings.RDS_HOSTNAME,
            "ssl_mode": settings.DB_SSL_MODE
        }
    }
    
    if db_error:
        response["database"]["error"] = db_error
    
    return response

@app.get("/test")
async def test_endpoint():
    """
    Simple test endpoint that doesn't require database connection
    """
    return {
        "message": "Test endpoint working",
        "environment": settings.ENVIRONMENT,
        "lambda": settings.is_lambda_environment(),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/db-config")
async def database_config():
    """
    Database configuration endpoint for debugging
    """
    return {
        "rds_hostname": settings.RDS_HOSTNAME,
        "rds_port": settings.RDS_PORT,
        "rds_db_name": settings.RDS_DB_NAME,
        "rds_username": settings.RDS_USERNAME,
        "rds_password_set": bool(settings.RDS_PASSWORD),
        "database_url_set": bool(settings.DATABASE_URL),
        "database_url_contains_localhost": "localhost" in settings.DATABASE_URL if settings.DATABASE_URL else False,
        "active_database_url": settings.get_active_database_url()[:50] + "..." if len(settings.get_active_database_url()) > 50 else settings.get_active_database_url(),
        "lambda_environment": settings.is_lambda_environment(),
        "environment": settings.ENVIRONMENT
    }

@app.get("/debug/contacts")
async def debug_contacts():
    """
    Debug endpoint to check database contents
    """
    try:
        from database import db_manager
        
        # Get recent contacts
        async with db_manager.get_session() as session:
            from sqlalchemy import text
            
            # Count total contacts
            result = await session.execute(text("SELECT COUNT(*) FROM contacts"))
            total_count = result.scalar()
            
            # Get recent contacts
            result = await session.execute(text("""
                SELECT id, email, phone_number, link_precedence, linked_id, 
                       created_at, updated_at
                FROM contacts 
                ORDER BY created_at DESC 
                LIMIT 10
            """))
            
            contacts = []
            for row in result.fetchall():
                contacts.append({
                    "id": row[0],
                    "email": row[1],
                    "phone_number": row[2],
                    "link_precedence": row[3],
                    "linked_id": row[4],
                    "created_at": row[5].isoformat() if row[5] else None,
                    "updated_at": row[6].isoformat() if row[6] else None
                })
            
            return {
                "total_contacts": total_count,
                "recent_contacts": contacts,
                "message": "Database query successful"
            }
            
    except Exception as e:
        logger.error(f"Debug contacts error: {e}")
        return {
            "error": str(e),
            "total_contacts": 0,
            "recent_contacts": [],
            "message": "Database query failed"
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
        
        # For production deployment, we expect the database to be available
        # If database is not available, we should return an error, not a mock response
        
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
            ).model_dump()
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
                ).model_dump()
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
            ).model_dump()
        )

# This is the proper way to run the application using uvicorn
if __name__ == "__main__":
    import uvicorn
    # Use the module:app_instance format for proper reloading
    uvicorn.run(
        "main:app",  # This tells uvicorn to import 'app' from 'main.py'
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,  # Enable auto-reload in debug mode
        workers=1  # Single worker for development
    )