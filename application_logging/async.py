import asyncio
import numpy as np
import nest_asyncio
import random
import asyncio.events as events


async def myCoroutine():
    print("good job")


def main():
    nest_asyncio.apply()
    #loop = asyncio.get_event_loop()
    #loop.run_until_complete(myCoroutine())
    # loop.close()
    asyncio.run(main())


main()
