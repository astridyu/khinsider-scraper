import asyncio
import logging
from aiohttp import ClientSession

from .scrape import download_all_song_infos


async def main():
    async with ClientSession() as cs:
        await download_all_song_infos(cs)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())

