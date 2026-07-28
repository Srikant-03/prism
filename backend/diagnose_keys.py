import asyncio
from google import genai
import time

async def test():
    from dotenv import load_dotenv; load_dotenv()
    from config import AppConfig
    
    keys = AppConfig.llm.GEMINI_API_KEYS
    print(f"Total keys configured: {len(keys)}")
    
    tasks = []
    
    async def test_key(idx, key):
        client = genai.Client(api_key=key)
        try:
            client.models.generate_content(model='gemini-2.5-flash', contents=f'ping {idx}')
            return f"Key {idx} ({key[-4:]}): SUCCESS"
        except Exception as e:
            err = str(e).split('\n')[0][:80]
            if "retry in" in str(e):
                import re
                m = re.search(r'retry in (.*?)s', str(e))
                if m:
                    return f"Key {idx} ({key[-4:]}): RATE_LIMIT (Retry in {m.group(1)}s)"
            return f"Key {idx} ({key[-4:]}): ERROR {err}"

    for i, k in enumerate(keys):
        tasks.append(test_key(i, k))
        
    results = await asyncio.gather(*tasks)
    for r in results:
        print(r)

asyncio.run(test())
