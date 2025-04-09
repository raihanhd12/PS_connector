import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import app.config as config
from app.api.connector_routes import router as connector_router
from app.storage.database import close_db, init_db

app = FastAPI(
    title="Connector Service",
    description="Database Connector Storage Service",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(connector_router, prefix="/api")

@app.on_event("startup")
async def startup_event():
    # Initialize database
    await init_db()

@app.on_event("shutdown")
async def shutdown_event():
    # Close database connection
    await close_db()

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Welcome to the Connector Service API!"}

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)