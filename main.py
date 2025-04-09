from dotenv import dotenv_values
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.connector_routes import router as connector_router
from app.storage.database import close_db, init_db

# Membaca isi file .env
config = dotenv_values(".env")
CORS_ORIGINS = config.get("CORS_ORIGINS", "*").split(",")

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
    allow_origins=CORS_ORIGINS,
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

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)