import asyncio
from google import genai
import os

async def test():
    from dotenv import load_dotenv; load_dotenv()
    from config import AppConfig
    
    keys = AppConfig.llm.GEMINI_API_KEYS
    if len(keys) < 2:
        print("Need at least 2 keys")
        return
        
    print(f"Testing 2 keys. Key 1 ends in {keys[0][-4:]}, Key 2 ends in {keys[1][-4:]}")
    
    # Try one key
    c1 = genai.Client(api_key=keys[0])
    c2 = genai.Client(api_key=keys[-1]) # use last key to be sure they are different
    
    try:
        r1 = c1.models.generate_content(model='gemini-2.5-flash', contents='hi')
        print("Key 1 success")
    except Exception as e:
        print("Key 1 error:", e)
        
    try:
        r2 = c2.models.generate_content(model='gemini-2.5-flash', contents='hi2')
        print("Key 2 success")
    except Exception as e:
        print("Key 2 error:", e)

asyncio.run(test())
