"""
Live Gemini model smoke test.
Safely skipped when GEMINI_API_KEY is not configured.
"""
import os
import pytest
from dotenv import load_dotenv

load_dotenv()

@pytest.mark.skipif(not os.getenv("GEMINI_API_KEY"), reason="No GEMINI_API_KEY configured in environment")
def test_gemini_models_smoke():
    from google import genai
    key = os.environ.get("GEMINI_API_KEY", "")
    client = genai.Client(api_key=key)
    resp = client.models.generate_content(model="gemini-2.5-flash", contents="Say hello in one word.")
    assert resp is not None
    assert resp.text and len(resp.text.strip()) > 0
