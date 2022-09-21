import asyncio

# async will create a coroutine

'''
async def main():
    print('Tim')
    await foo('From Foo wait for 2s')  # await + coroutine
    task = asyncio.create_task(boo('From Boo wait for 2s but put in task'))
    # So it will run the next code
    # We can put await task, so we can execute task then print Finished
    # await task
    await asyncio.sleep(0.5)
    # -> Run task -> Print(Boo) -> See await 5s -> Run (Finish): Finish is the last code -> End main
    print('Finished')


async def foo(text):
    print(text)
    await asyncio.sleep(2)  # a coroutine


async def boo(text):
    print(text)
    await asyncio.sleep(5)  # a coroutine
    print(text)

asyncio.run(main())
'''


async def fetch_data():
    print('start fetching')
    await asyncio.sleep(2)
    print('done fetching')
    return {'data': 1}


async def print_numbers():
    for i in range(10):
        print(i)
        await asyncio.sleep(0.25)


async def main():
    task1 = asyncio.create_task(fetch_data())
    task2 = asyncio.create_task(print_numbers())
    # value = task1:
    value = await task1
    print(value)
    await task2

asyncio.run(main())