"""
Developer utility: verify all keys in GEMINI_API_KEYS against Gemini API.
Usage:
    python backend/scripts/verify_keys.py
"""
import sys
import os
from dotenv import load_dotenv

load_dotenv()

keys_str = os.getenv("GEMINI_API_KEYS", "")
print(f"Loaded {len(keys_str.split(','))} keys from .env")

keys = [k.strip() for k in keys_str.split(",") if k.strip()]
if not keys:
    print("No GEMINI_API_KEYS found.")
    sys.exit(0)

try:
    from google import genai
except ImportError:
    print("google-genai package not installed.")
    sys.exit(1)

print("Testing Keys...")
working_keys = []
for i, key in enumerate(keys):
    try:
        client = genai.Client(api_key=key.strip())
        resp = client.models.generate_content(model="gemini-2.5-flash", contents="Say hello")
        print(f"Key {i} (...{key[-4:]}) SUCCESS: {resp.text.strip()}")
        working_keys.append(key)
        break
    except Exception as e:
        print(f"Key {i} (...{key[-4:]}) FAILED: {type(e).__name__}")
        
if working_keys:
    print(f"\nSUCCESS! Found a working API key: ...{working_keys[-1][-4:]}")
else:
    print("\nALL KEYS FAILED.")
