import asyncio
from google import genai
from google.api_core.exceptions import ResourceExhausted, TooManyRequests

async def test():
    client = genai.Client(api_key="AIzaSyCcgkA_iiFJo5BuVZTHfncmZiLzZinu-BA")
    
    async def make_req(i):
        try:
            return client.models.generate_content(
                model='gemini-2.5-flash',
                contents=f'hello count {i}',
            ).text
        except Exception as e:
            return e

    results = await asyncio.gather(*[make_req(i) for i in range(25)])
    for res in results:
        if isinstance(res, Exception):
            print("GOT EXCEPTION:", type(res))
            print("STRING:", str(res))
            break
    print("Test finished.")

asyncio.run(test())
