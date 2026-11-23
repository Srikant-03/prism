"""
Developer benchmark utility: test live Gemini models using GEMINI_API_KEY.
Usage:
    python backend/scripts/benchmark_models.py
"""
import os
from dotenv import load_dotenv
load_dotenv()

from google import genai

key = os.environ.get("GEMINI_API_KEY", "")
print(f"Testing Key: ...{key[-4:] if len(key) >= 4 else 'NONE'}")

if not key:
    print("No GEMINI_API_KEY found in environment or .env. Exiting.")
    exit(1)

client = genai.Client(api_key=key)

for model_name in ["gemini-2.0-flash", "gemini-2.5-flash"]:
    try:
        print(f"\n--- Testing {model_name} ---")
        resp = client.models.generate_content(model=model_name, contents="Say hello in one word.")
        print(f"SUCCESS ({model_name}): {resp.text.strip()}")
    except Exception as e:
        print(f"FAILED ({model_name}): {type(e).__name__} - {e}")
