"""
Chat Engine — Builds context for AI conversations and parses actions.
Provides full dataset awareness across all pillars.
"""

from __future__ import annotations

import json
from typing import Any, Optional

import google.generativeai as genai
from config import settings


# ── Context builder ──────────────────────────────────────────────────

class ChatContextBuilder:
    """Assembles full dataset context for the AI conversation."""

    _schema: Optional[dict] = None
    _profile_summary: Optional[dict] = None
    _cleaning_log: list[dict] = []
    _recent_queries: list[str] = []

    @classmethod
    def set_schema(cls, schema: dict):
        cls._schema = schema

    @classmethod
    def set_profile(cls, profile: dict):
        cls._profile_summary = profile

    @classmethod
    def add_cleaning_step(cls, step: dict):
        cls._cleaning_log.append(step)

    @classmethod
    def add_query(cls, sql: str):
        cls._recent_queries = ([sql] + cls._recent_queries)[:10]

    @classmethod
    def build_system_prompt(cls) -> str:
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

        if cls._schema:
            parts.append("## Current Dataset Schema")
            for table_name, cols in cls._schema.items():
                parts.append(f"### Table: {table_name}")
                for col in cols:
                    parts.append(f"  - {col['name']} ({col['type']})")
            parts.append("")

        if cls._profile_summary:
            parts.append("## Profiling Summary")
            p = cls._profile_summary
            parts.append(f"  - Rows: {p.get('row_count', '?')}, Columns: {p.get('column_count', '?')}")
            parts.append(f"  - Memory: {p.get('memory_mb', '?')} MB")
            if p.get('missing_summary'):
                parts.append(f"  - Missing values: {p['missing_summary']}")
            if p.get('quality_score'):
                parts.append(f"  - Data quality score: {p['quality_score']}/100")
            parts.append("")

        if cls._cleaning_log:
            parts.append("## Preprocessing Steps Applied")
            for i, step in enumerate(cls._cleaning_log[-10:], 1):
                parts.append(f"  {i}. {step.get('action', '?')} on {step.get('column', 'multiple columns')}")
            parts.append("")

        if cls._recent_queries:
            parts.append("## Recent Queries")
            for q in cls._recent_queries[:5]:
                parts.append(f"  - {q[:200]}")
            parts.append("")

        return "\n".join(parts)


# ── Chat Engine ──────────────────────────────────────────────────────

class ChatEngine:
    """Multi-turn AI conversation engine using Gemini."""

    def __init__(self):
        try:
            genai.configure(api_key=settings.GEMINI_API_KEY)
            self.model = genai.GenerativeModel("gemini-2.0-flash")
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

        system_prompt = ChatContextBuilder.build_system_prompt()

        # Build messages
        contents = [{"role": "user", "parts": [{"text": system_prompt + "\n\nNow respond to the user's messages."}]}]
        contents.append({"role": "model", "parts": [{"text": "Understood. I'm ready to help with your data analysis. I have full context of your dataset schema, profiling results, and preprocessing decisions."}]})

        if conversation_history:
            for msg in conversation_history[-20:]:
                role = "user" if msg.get("role") == "user" else "model"
                contents.append({"role": role, "parts": [{"text": msg.get("content", "")}]})

        contents.append({"role": "user", "parts": [{"text": message}]})

        try:
            response = self.model.generate_content(contents)
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


# Singleton
_engine: Optional[ChatEngine] = None

def get_chat_engine() -> ChatEngine:
    global _engine
    if _engine is None:
        _engine = ChatEngine()
    return _engine
