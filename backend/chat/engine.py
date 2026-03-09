"""
Chat Engine — Builds context for AI conversations and parses actions.
Provides full dataset awareness across all pillars.
"""

from __future__ import annotations

import json
import logging
import asyncio # Added for _execute_chat_prompt
from typing import Any, Optional

import google.generativeai as genai
from config import LLMConfig
from llm.api_manager import with_llm_failover

logger = logging.getLogger(__name__)

@with_llm_failover(tier_rpm=10)
async def _execute_chat_prompt(model, contents) -> Any:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: model.generate_content(contents))


# ── Context builder ──────────────────────────────────────────────────

class ChatContextBuilder:
    """Assembles full dataset context for the AI conversation.

    Instance-based so state is isolated per-engine and testable.
    """

    def __init__(self) -> None:
        self._schema: Optional[dict] = None
        self._profile_summary: Optional[dict] = None
        self._cleaning_log: list[dict] = []
        self._recent_queries: list[str] = []

    def set_schema(self, schema: dict) -> None:
        self._schema = schema

    def set_profile(self, profile: dict) -> None:
        self._profile_summary = profile

    def add_cleaning_step(self, step: dict) -> None:
        self._cleaning_log.append(step)

    def add_query(self, sql: str) -> None:
        self._recent_queries = ([sql] + self._recent_queries)[:10]

    def build_system_prompt(self) -> str:
        parts = [
            "You are a data analysis assistant embedded in a Data Intelligence Platform.",
            "You have full awareness of the user's dataset and all analysis performed.",
            "Always be specific about column names, data types, and values.",
            "When suggesting actions, format them as JSON action blocks.",
            "",
            "## Response Format",
            "For actionable suggestions, include a JSON block like:",
            '```json',
            '{"actions": [{"label": "Run this query", "type": "sql", "payload": "SELECT ..."}]}',
            '```',
            "Action types: sql (run query), grid (show in grid), fix (apply preprocessing), navigate (go to section)",
            "",
        ]

        if self._schema:
            parts.append("## Current Dataset Schema")
            for table_name, cols in self._schema.items():
                parts.append(f"### Table: {table_name}")
                for col in cols:
                    parts.append(f"  - {col['name']} ({col['type']})")
            parts.append("")

        if self._profile_summary:
            parts.append("## Profiling Summary")
            p = self._profile_summary
            parts.append(f"  - Rows: {p.get('row_count', '?')}, Columns: {p.get('column_count', '?')}")
            parts.append(f"  - Memory: {p.get('memory_mb', '?')} MB")
            if p.get('missing_summary'):
                parts.append(f"  - Missing values: {p['missing_summary']}")
            if p.get('quality_score'):
                parts.append(f"  - Data quality score: {p['quality_score']}/100")
            parts.append("")

        if self._cleaning_log:
            parts.append("## Preprocessing Steps Applied")
            for i, step in enumerate(self._cleaning_log[-10:], 1):
                parts.append(f"  {i}. {step.get('action', '?')} on {step.get('column', 'multiple columns')}")
            parts.append("")

        if self._recent_queries:
            parts.append("## Recent Queries")
            for q in self._recent_queries[:5]:
                parts.append(f"  - {q[:200]}")
            parts.append("")

        return "\n".join(parts)


# ── Chat Engine ──────────────────────────────────────────────────────

class ChatEngine:
    """Multi-turn AI conversation engine using Gemini."""

    def __init__(self, context: ChatContextBuilder | None = None):
        self.context = context or ChatContextBuilder()
        try:
            self.model = genai.GenerativeModel(LLMConfig.MODEL_WORKHORSE)
        except Exception:
            self.model = None

    async def chat(
        self,
        message: str,
        conversation_history: list[dict] | None = None,
    ) -> dict:
        """Process a chat message and return AI response with actions."""
        if not self.model:
            return {
                "response": "⚠️ AI service not configured. Please set your Gemini API key.",
                "actions": [],
            }

        system_prompt = self.context.build_system_prompt()

        # Build messages
        contents = [{"role": "user", "parts": [{"text": system_prompt + "\n\nNow respond to the user's messages."}]}]
        contents.append({"role": "model", "parts": [{"text": "Understood. I'm ready to help with your data analysis. I have full context of your dataset schema, profiling results, and preprocessing decisions."}]})

        if conversation_history:
            for msg in conversation_history[-20:]:
                role = "user" if msg.get("role") == "user" else "model"
                contents.append({"role": role, "parts": [{"text": msg.get("content", "")}]})

        contents.append({"role": "user", "parts": [{"text": message}]})

        try:
            response = await _execute_chat_prompt(self.model, contents)
            text = response.text

            # Parse action blocks from response
            actions = []
            if "```json" in text:
                import re
                json_blocks = re.findall(r'```json\s*(.*?)\s*```', text, re.DOTALL)
                for block in json_blocks:
                    try:
                        parsed = json.loads(block)
                        if "actions" in parsed:
                            actions.extend(parsed["actions"])
                    except json.JSONDecodeError:
                        pass
                # Remove JSON blocks from display text
                clean_text = re.sub(r'```json\s*\{[^}]*"actions"[^}]*\}\s*```', '', text).strip()
            else:
                clean_text = text

            return {
                "response": clean_text,
                "actions": actions,
            }
        except Exception as e:
            return {
                "response": f"⚠️ AI error: {str(e)}",
                "actions": [],
            }


# ── Singleton accessor ───────────────────────────────────────────────
_engine: Optional[ChatEngine] = None


def get_chat_engine() -> ChatEngine:
    """Return the singleton ChatEngine (creates one on first call)."""
    global _engine
    if _engine is None:
        _engine = ChatEngine()
    return _engine
