import asyncio
from google import genai

async def test():
    client = genai.Client(api_key="AIzaSyCcgkA_iiFJo5BuVZTHfncmZiLzZinu-BA")
    print("Waiting 30 seconds...")
    await asyncio.sleep(30)
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents='hello again',
        )
        print("Success:", response.text)
    except Exception as e:
        print("Still error:", str(e))

asyncio.run(test())
