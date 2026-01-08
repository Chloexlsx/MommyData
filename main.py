"""Main FastAPI application."""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from app.database import init_db
from app.routes import api, web

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan."""
    await init_db()  # Startup
    yield
    # Shutdown


app = FastAPI(
    title="MommyData - NSW Mothers and Babies Data Explorer",
    description="Interactive data explorer for NSW Mothers and Babies 2023 data",
    version="1.0.0",
    lifespan=lifespan,
)

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(api.router)
app.include_router(web.router)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}

