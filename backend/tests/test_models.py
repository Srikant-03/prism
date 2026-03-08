import google.generativeai as genai
import sys

# Test the first key which previously failed
key = "AIzaSyCzGltXEXjKYBX1G219PqdkiI23mQHmWxs"
print(f"Testing Key: ...{key[-4:]}")

try:
    genai.configure(api_key=key)
    print("\n--- Testing gemini-1.5-flash ---")
    model_15 = genai.GenerativeModel("gemini-1.5-flash")
    resp_15 = model_15.generate_content("Say hello in one word.")
    print(f"SUCCESS (1.5-flash): {resp_15.text.strip()}")
except Exception as e:
    print(f"FAILED (1.5-flash): {type(e).__name__} - {e}")

try:
    print("\n--- Testing gemini-2.5-flash ---")
    model_25 = genai.GenerativeModel("gemini-2.5-flash")
    resp_25 = model_25.generate_content("Say hello in one word.")
    print(f"SUCCESS (2.5-flash): {resp_25.text.strip()}")
except Exception as e:
    print(f"FAILED (2.5-flash): {type(e).__name__} - {e}")

try:
    print("\n--- Testing gemini-1.0-pro ---")
    model_10 = genai.GenerativeModel("gemini-1.0-pro")
    resp_10 = model_10.generate_content("Say hello in one word.")
    print(f"SUCCESS (1.0-pro): {resp_10.text.strip()}")
except Exception as e:
    print(f"FAILED (1.0-pro): {type(e).__name__} - {e}")
