"""
NL Query — Natural Language to SQL translator using Google Gemini.
Builds full schema context from live tables and generates SQL with explanations.
"""

from __future__ import annotations

import os
import json
import re
from typing import Any, Optional

try:
    import google.generativeai as genai
    from llm.api_manager import with_llm_failover
    from config import LLMConfig
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False


# ── Schema context builder ────────────────────────────────────────────

def build_schema_context(engine) -> str:
    """
    Build a rich schema context string from all registered tables.
    Includes table names, columns, types, sample values, cardinality.
    """
    tables = engine.list_tables()
    if not tables:
        return "No tables are currently loaded."

    parts = []
    for table in tables:
        cols = engine.get_columns(table["name"])
        col_descriptions = []
        for c in cols:
            samples = c.get("sample_values", [])
            sample_str = ", ".join(str(s) for s in samples[:5]) if samples else "N/A"
            col_descriptions.append(
                f"    - {c['name']} ({c['ui_type']}, dtype={c['dtype']}, "
                f"unique={c['unique_count']}, nulls={c['null_pct']}%, "
                f"samples: [{sample_str}])"
            )

        parts.append(
            f"Table: \"{table['name']}\" "
            f"(source={table['source']}, rows={table['n_rows']}, cols={table['n_cols']})\n"
            + "\n".join(col_descriptions)
        )

    return "\n\n".join(parts)


# ── System prompt ─────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert SQL analyst. You translate natural language questions into DuckDB-compatible SQL queries.

RULES:
1. Use ONLY the tables and columns described in the SCHEMA below. NEVER invent column or table names.
2. Always quote table and column names with double quotes (e.g., "table_name"."column_name").
3. Write efficient, correct DuckDB SQL.
4. If the query is ambiguous, make your best interpretation and note assumptions.
5. If you CANNOT confidently map the user's request to the schema, explain what you could not resolve and ask a targeted clarifying question.
6. Support complex queries: JOINs, window functions, CTEs, subqueries, CASE WHEN, date functions, aggregations.
7. For date operations, use DuckDB functions (DATE_TRUNC, DATE_DIFF, INTERVAL, etc.).
8. Always include LIMIT 1000 unless the user asks for all results or specifies a different limit.

RESPONSE FORMAT — respond with ONLY a JSON object, no markdown fencing:
{
    "sql": "SELECT ...",
    "explanation": "This query does...",
    "assumptions": ["Assumed X means Y"],
    "confidence": "high|medium|low",
    "clarification_needed": null or "What do you mean by...?"
}
"""


# ── NL Query translator ──────────────────────────────────────────────

class NLQueryTranslator:
    """Translates natural language queries to SQL using Google Gemini."""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY", "")
        self.model_name = model or LLMConfig.MODEL_HEAVY
        self._model = None

    def _get_model(self):
        """Lazy-init the Gemini model."""
        if self._model is None:
            if not HAS_GENAI:
                raise RuntimeError("google-generativeai package is not installed.")
            self._model = genai.GenerativeModel(
                model_name=self.model_name,
                system_instruction=SYSTEM_PROMPT,
            )
        return self._model

    async def translate(
        self,
        question: str,
        engine,
        conversation_history: Optional[list[dict]] = None,
    ) -> dict:
        """
        Translate a natural language question to SQL.

        Returns:
            {
                "sql": str,
                "explanation": str,
                "assumptions": list[str],
                "confidence": str,
                "clarification_needed": str | None,
                "schema_context": str,
                "success": bool,
                "error": str | None,
            }
        """
        try:
            schema_ctx = build_schema_context(engine)

            # Build the prompt
            user_prompt = f"""SCHEMA:
{schema_ctx}

USER QUESTION:
{question}"""

            # Include conversation history for refinement
            contents = []
            if conversation_history:
                for msg in conversation_history[-6:]:  # Last 3 exchanges
                    contents.append({"role": msg["role"], "parts": [msg["content"]]})

            contents.append({"role": "user", "parts": [user_prompt]})

            @with_llm_failover(tier_rpm=2)
            def do_generate():
                model = self._get_model()
                return model.generate_content(contents)
            
            response = await do_generate()

            # Parse the JSON response
            text = response.text.strip()
            # Remove markdown code fencing if present
            text = re.sub(r'^```(?:json)?\s*', '', text)
            text = re.sub(r'\s*```$', '', text)

            try:
                result = json.loads(text)
            except json.JSONDecodeError:
                # Try to extract JSON from the response
                json_match = re.search(r'\{[\s\S]*\}', text)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    return {
                        "sql": "",
                        "explanation": text,
                        "assumptions": [],
                        "confidence": "low",
                        "clarification_needed": None,
                        "schema_context": schema_ctx,
                        "success": False,
                        "error": "Could not parse LLM response as JSON.",
                    }

            return {
                "sql": result.get("sql", ""),
                "explanation": result.get("explanation", ""),
                "assumptions": result.get("assumptions", []),
                "confidence": result.get("confidence", "medium"),
                "clarification_needed": result.get("clarification_needed"),
                "schema_context": schema_ctx,
                "success": True,
                "error": None,
            }

        except Exception as e:
            return {
                "sql": "",
                "explanation": "",
                "assumptions": [],
                "confidence": "low",
                "clarification_needed": None,
                "schema_context": "",
                "success": False,
                "error": str(e),
            }

    async def refine(
        self,
        original_question: str,
        original_sql: str,
        refinement: str,
        engine,
    ) -> dict:
        """
        Refine a previously generated SQL query based on user feedback.
        """
        conversation_history = [
            {"role": "user", "content": original_question},
            {"role": "model", "content": json.dumps({
                "sql": original_sql,
                "explanation": "Previous query",
                "assumptions": [],
                "confidence": "high",
            })},
        ]

        refined_question = (
            f"The previous SQL was:\n{original_sql}\n\n"
            f"The user wants to modify it: {refinement}\n\n"
            f"Generate the updated SQL."
        )

        return await self.translate(
            refined_question,
            engine,
            conversation_history=conversation_history,
        )


# ── Singleton ─────────────────────────────────────────────────────────

_translator: Optional[NLQueryTranslator] = None


def get_translator() -> NLQueryTranslator:
    """Get or create the global NL translator instance."""
    global _translator
    if _translator is None:
        _translator = NLQueryTranslator()
    return _translator
