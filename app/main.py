"""
Main FastAPI application
"""
from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.storage.database import init_db
from app.api import connector_routes


# Initialize the FastAPI app
app = FastAPI(
    title="Database Connector Service",
    description="API for managing connections to various data sources",
    version="1.0.0"
)

# Include connector routes
app.include_router(connector_routes.router)

# Initialize database on startup
@app.on_event("startup")
def startup_event():
    """Initialize the application on startup"""
    init_db()


# Health check endpoint
@app.get("/", tags=["Health"])
def root():
    """Root endpoint to check if API is running"""
    return {
        "message": "Database Connector Service",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": "1.0.0"
    }