import asyncio
import logging
from aiohttp import ClientSession

from .scrape import download_all_song_infos


def main():
    async def task():
        async with ClientSession() as cs:
            await download_all_song_infos(cs)
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(task())


if __name__ == '__main__':
    main()
