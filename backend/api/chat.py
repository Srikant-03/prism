"""
Chat API — FastAPI endpoints for AI conversation layer.
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from chat.engine import get_chat_engine

router = APIRouter(prefix="/api/chat", tags=["chat"])


class ChatRequest(BaseModel):
    message: str
    conversation_history: Optional[list[dict]] = None


class ContextUpdate(BaseModel):
    schema: Optional[dict] = None
    profile_summary: Optional[dict] = None
    cleaning_step: Optional[dict] = None
    query: Optional[str] = None


@router.post("/message")
async def chat_message(request: ChatRequest):
    """Send a message and get AI response with actions."""
    engine = get_chat_engine()
    result = await engine.chat(
        message=request.message,
        conversation_history=request.conversation_history,
    )
    return result


@router.post("/context")
async def update_context(request: ContextUpdate):
    """Update the AI's awareness of the current dataset state."""
    ctx = get_chat_engine().context
    if request.schema:
        ctx.set_schema(request.schema)
    if request.profile_summary:
        ctx.set_profile(request.profile_summary)
    if request.cleaning_step:
        ctx.add_cleaning_step(request.cleaning_step)
    if request.query:
        ctx.add_query(request.query)
    return {"status": "context_updated"}


@router.get("/context")
async def get_context():
    """Get current AI context state for debugging."""
    ctx = get_chat_engine().context
    return {
        "has_schema": ctx._schema is not None,
        "has_profile": ctx._profile_summary is not None,
        "cleaning_steps": len(ctx._cleaning_log),
        "recent_queries": len(ctx._recent_queries),
    }
