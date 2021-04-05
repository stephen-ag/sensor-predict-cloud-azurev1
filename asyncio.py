
import asyncio
import nest_asyncio

async def count():
    await asyncio.sleep(10)
    print("One")
    await asyncio.sleep(8)
    print("Two")
    await asyncio.sleep(1)
    print("Thress")


async def main():
    await asyncio.gather(count(),count())

if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(main())