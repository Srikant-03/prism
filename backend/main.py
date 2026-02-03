"""
Data Intelligence Platform — FastAPI Application Entry Point.
"""

from dotenv import load_dotenv
load_dotenv()  # Load .env before anything else reads os.getenv

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import AppConfig
from api.upload import router as upload_router
from api.profiling import router as profiling_router
from api.cleaning import router as cleaning_router
from api.sql import router as sql_router
from api.reporting import router as reporting_router
from api.chat import router as chat_router
from api.grid import router as grid_router
from api.watchlist import router as watchlist_router
from api.graph import router as graph_router
from api.story import router as story_router
from api.recipe import router as recipe_router
from api.metadata import router as metadata_router
from api.collab import router as collab_router
from api.simulate import router as simulate_router
from api.stats import router as stats_router
from api.explain import router as explain_router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Data Intelligence Platform",
        description=(
            "Autonomous, production-grade data intelligence engine. "
            "Upload any file — the system handles everything from format detection "
            "to data profiling without any user configuration."
        ),
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # CORS — allow frontend dev server
    app.add_middleware(
        CORSMiddleware,
        allow_origins=AppConfig.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register API routes
    app.include_router(upload_router)
    app.include_router(profiling_router)
    app.include_router(cleaning_router)
    app.include_router(sql_router)
    app.include_router(reporting_router)
    app.include_router(chat_router)
    app.include_router(grid_router)
    app.include_router(watchlist_router)
    app.include_router(graph_router)
    app.include_router(story_router)
    app.include_router(recipe_router)
    app.include_router(metadata_router)
    app.include_router(collab_router)
    app.include_router(simulate_router)
    app.include_router(stats_router)
    app.include_router(explain_router)

    @app.get("/")
    async def root():
        return {
            "name": "Data Intelligence Platform",
            "version": "1.0.0",
            "status": "operational",
            "endpoints": {
                "upload": "/api/upload",
                "profile": "/api/profile/{file_id}",
                "docs": "/docs",
            },
        }

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=AppConfig.HOST,
        port=AppConfig.PORT,
        reload=AppConfig.DEBUG,
    )
