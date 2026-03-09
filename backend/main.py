"""
Data Intelligence Platform — FastAPI Application Entry Point.

NOTE: This application uses in-memory state dicts (_ingestion_store, _profile_store,
_cleaning_store) which are NOT shared across worker processes. When deploying with
uvicorn, use a single worker (--workers 1) or migrate state to Redis/a shared store.
"""

from dotenv import load_dotenv
load_dotenv()  # Load .env before anything else reads os.getenv

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from config import AppConfig, LLMConfig

logger = logging.getLogger(__name__)
from api.dependencies import verify_api_key
from llm.api_manager import key_manager
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
from api.hypotheses import router as hypotheses_router
from api.dashboard import router as dashboard_router
from api.drift import router as drift_router
from api.hypothesis_testing import router as hypothesis_testing_router
from api.joins import router as joins_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle: validate config and log warnings."""
    # ── Startup validation ────────────────────────────────────────
    warnings_list = []

    if not LLMConfig.GEMINI_API_KEY:
        warnings_list.append("GEMINI_API_KEY is not set — NL Query and chat features will fail.")

    if AppConfig.API_KEY == "dev-secret-key-123":
        warnings_list.append("DATA_INTEL_API_KEY is using the default dev key — set a secure key for production.")

    if AppConfig.DEBUG:
        warnings_list.append("DEBUG mode is ON — stack traces will be exposed. Disable for production.")

    for w in warnings_list:
        logger.warning("[STARTUP] %s", w)

    logger.info(
        "[STARTUP] Data Intelligence Platform v%s | host=%s port=%d debug=%s",
        AppConfig.VERSION, AppConfig.HOST, AppConfig.PORT, AppConfig.DEBUG,
    )

    yield  # App is running

    # ── Shutdown ──────────────────────────────────────────────────
    logger.info("[SHUTDOWN] Data Intelligence Platform shutting down.")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Data Intelligence Platform",
        description=(
            "Autonomous, production-grade data intelligence engine. "
            "Upload any file — the system handles everything from format detection "
            "to data profiling without any user configuration."
        ),
        version=AppConfig.VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # CORS — allow frontend dev server
    app.add_middleware(
        CORSMiddleware,
        allow_origins=AppConfig.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-API-Key"],
    )

    # Rate limiting
    limiter = Limiter(
        key_func=get_remote_address,
        default_limits=[AppConfig.RATE_LIMIT],
    )
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    # Register API routes with API Key requirement
    app.include_router(upload_router, dependencies=[Depends(verify_api_key)])
    app.include_router(profiling_router, dependencies=[Depends(verify_api_key)])
    app.include_router(cleaning_router, dependencies=[Depends(verify_api_key)])
    app.include_router(sql_router, dependencies=[Depends(verify_api_key)])
    app.include_router(reporting_router, dependencies=[Depends(verify_api_key)])
    app.include_router(chat_router, dependencies=[Depends(verify_api_key)])
    app.include_router(grid_router, dependencies=[Depends(verify_api_key)])
    app.include_router(watchlist_router, dependencies=[Depends(verify_api_key)])
    app.include_router(graph_router, dependencies=[Depends(verify_api_key)])
    app.include_router(story_router, dependencies=[Depends(verify_api_key)])
    app.include_router(recipe_router, dependencies=[Depends(verify_api_key)])
    app.include_router(metadata_router, dependencies=[Depends(verify_api_key)])
    app.include_router(collab_router, dependencies=[Depends(verify_api_key)])
    app.include_router(simulate_router, dependencies=[Depends(verify_api_key)])
    app.include_router(stats_router, dependencies=[Depends(verify_api_key)])
    app.include_router(explain_router, dependencies=[Depends(verify_api_key)])
    app.include_router(hypotheses_router, dependencies=[Depends(verify_api_key)])
    app.include_router(dashboard_router, dependencies=[Depends(verify_api_key)])
    app.include_router(drift_router, dependencies=[Depends(verify_api_key)])
    app.include_router(hypothesis_testing_router, dependencies=[Depends(verify_api_key)])
    app.include_router(joins_router, dependencies=[Depends(verify_api_key)])

    @app.get("/")
    @limiter.exempt
    async def root():
        return {
            "name": "Data Intelligence Platform",
            "version": AppConfig.VERSION,
            "status": "operational",
            "endpoints": {
                "upload": "/api/upload",
                "profile": "/api/profile/{file_id}",
                "docs": "/docs",
            },
        }

    @app.get("/health")
    @limiter.exempt
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
