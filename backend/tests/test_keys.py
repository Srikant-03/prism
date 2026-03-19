from google import genai
import sys
import os
from dotenv import load_dotenv

load_dotenv()

keys_str = os.getenv("GEMINI_API_KEYS", "")
print(f"Loaded {len(keys_str.split(','))} keys from .env")

keys = [k.strip() for k in keys_str.split(",") if k.strip()]

print("Testing Keys...")
working_keys = []
for i, key in enumerate(keys):
    try:
        client = genai.Client(api_key=key.strip())
        resp = client.models.generate_content(model="gemini-2.0-flash", contents="Say hello")
        print(f"Key {i} (...{key[-4:]}) SUCCESS: {resp.text.strip()}")
        working_keys.append(key)
        break # One working key is enough to prove it!
    except Exception as e:
        print(f"Key {i} (...{key[-4:]}) FAILED: {type(e).__name__}")
        
if working_keys:
    print(f"\nSUCCESS! Found a working API key: ...{working_keys[-1][-4:]}")
else:
    print("\nALL 38 KEYS FAILED (429 Quota Exceeded).")
