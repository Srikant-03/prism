import asyncio
from google import genai
import time

async def test():
    client = genai.Client(api_key="AIzaSyCcgkA_iiFJo5BuVZTHfncmZiLzZinu-BA")
    
    for i in range(30):
        try:
            client.models.generate_content(model='gemini-2.5-flash', contents=f'hi {i}')
            print(f"[{i}] Success")
        except Exception as e:
            print(f"[{i}] Error:", str(e))
            import re
            m = re.search(r'retry in (.*?)s', str(e))
            if m:
                print("Wait time extracted:", float(m.group(1)))
            break

asyncio.run(test())
