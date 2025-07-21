"""
Main FastAPI application entry point for Identity Reconciliation API
This file serves as the primary entry point for the FastAPI application.
Currently contains basic setup - will be expanded in later tasks to include
endpoints, middleware, error handling, and business logic integration.
"""

from fastapi import FastAPI
from config import settings

# Create FastAPI application instance
app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION,
    debug=settings.DEBUG
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
    Basic health check endpoint
    """
    return {
        "status": "healthy",
        "environment": settings.ENVIRONMENT
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    ) 