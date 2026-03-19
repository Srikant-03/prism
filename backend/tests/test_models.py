from google import genai
import os

# Test the first key which previously failed
key = os.environ.get("GEMINI_API_KEY", "")
print(f"Testing Key: ...{key[-4:] if len(key) >= 4 else 'NONE'}")

client = genai.Client(api_key=key)

try:
    print("\n--- Testing gemini-1.5-flash ---")
    resp_15 = client.models.generate_content(model="gemini-1.5-flash", contents="Say hello in one word.")
    print(f"SUCCESS (1.5-flash): {resp_15.text.strip()}")
except Exception as e:
    print(f"FAILED (1.5-flash): {type(e).__name__} - {e}")

try:
    print("\n--- Testing gemini-2.5-flash ---")
    resp_25 = client.models.generate_content(model="gemini-2.5-flash", contents="Say hello in one word.")
    print(f"SUCCESS (2.5-flash): {resp_25.text.strip()}")
except Exception as e:
    print(f"FAILED (2.5-flash): {type(e).__name__} - {e}")

try:
    print("\n--- Testing gemini-1.0-pro ---")
    resp_10 = client.models.generate_content(model="gemini-1.0-pro", contents="Say hello in one word.")
    print(f"SUCCESS (1.0-pro): {resp_10.text.strip()}")
except Exception as e:
    print(f"FAILED (1.0-pro): {type(e).__name__} - {e}")
