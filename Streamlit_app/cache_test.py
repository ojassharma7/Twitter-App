import asyncio
from cache import TTLCache
import pandas as pd

cache = TTLCache(10, ttl=10)


async def main():
    cache["user_1"] = pd.DataFrame([
        {"user_id": "1", "name": "John Doe"},
        {"user_id": "2", "name": "Jane Doe"},
    ])

    try:
        print(cache["user_1"])
        await asyncio.sleep(15)  # sleep for 15 seconds
        print(cache["user_1"])
    except KeyError as e:
        print(e)


asyncio.run(main())
