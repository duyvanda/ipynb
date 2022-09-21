import asyncio, requests
import aiohttp
import time

URL = 'https://httpbin.org/uuid'
print('Hello 1')
start = time.time()


async def fetch():
    async with aiohttp.ClientSession() as session:
        json_response = await session.get(url=URL)
        json_response = await json_response.json()
    return json_response


async def main():
    tasks = []
    for i in range(50):
        task = asyncio.create_task(fetch())
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    return res

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
results_3 = asyncio.run(main())
print('Hello 2')
print(results_3[0])
end = time.time()
total_time = end - start
print(total_time)

