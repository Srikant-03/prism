import asyncio
from google import genai
from google.api_core.exceptions import ResourceExhausted, TooManyRequests

async def test():
    client = genai.Client(api_key="AIzaSyCcgkA_iiFJo5BuVZTHfncmZiLzZinu-BA")
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents='hello',
        )
        print("Success:", response.text)
    except Exception as e:
        print("Type:", type(e))
        print("Error:", str(e))
        print("Code:", getattr(e, 'code', None))

asyncio.run(test())
