

import asyncio
from dataclasses import dataclass
import logging
from pathlib import Path
from tempfile import TemporaryFile
import time
from typing import Iterable, List, NamedTuple
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from khinsider_scraper.parse import SongInfo, get_album_links_on_letter_page, get_last_letter_page, get_song_slug_from_url, get_songs_on_album_page

logger = logging.getLogger(__name__)


letter_url_fmt = "https://downloads.khinsider.com/game-soundtracks/browse/{}"
letters = ["%23"] + list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
letter_urls = [letter_url_fmt.format(l) for l in letters]


class FetchTask:
    async def fetch(self, cs: ClientSession) -> Iterable['FetchTask']:
        raise NotImplemented


@dataclass
class AlbumFetch(FetchTask):
    url: str

    async def fetch(self, cs: ClientSession) -> Iterable['FetchTask']:
        logger.info(f'Fetching album at URL {self.url}')
        res = await cs.get(self.url)
        html = await res.read()
        soup = BeautifulSoup(html, features='html5lib')
        infos = [
            SongFetch(get_song_slug_from_url(self.url + song_url))
            for song_url in get_songs_on_album_page(soup)
        ]
        logger.info(f'Found {len(infos)} songs at {self.url}')
        return infos


@dataclass
class LetterPageFetch(FetchTask):
    url: str

    async def fetch(self, cs: ClientSession) -> Iterable['FetchTask']:
        logger.info(f'Fetching letter page at URL {self.url}')
        res = await cs.get(self.url)
        html = await res.read()
        soup = BeautifulSoup(html, features='html5lib')
        return (
            AlbumFetch(url)
            for url in get_album_links_on_letter_page(soup)
        )


@dataclass
class LetterFetch(FetchTask):
    url: str

    async def fetch(self, cs: ClientSession) -> Iterable['FetchTask']:
        logger.info(f'Fetching information about letter at URL {self.url}')
        res = await cs.get(self.url)
        html = await res.read()

        soup1 = BeautifulSoup(html, features='html5lib')
        count = get_last_letter_page(soup1)

        def result():
            for url in get_album_links_on_letter_page(soup1):
                yield AlbumFetch(url)
            for i in range(2, count + 1):
                yield LetterPageFetch(self.url + '?page=' + str(i))

        return result()


@dataclass
class SongFetch(FetchTask):
    info: SongInfo

    async def fetch(self, cs: ClientSession) -> Iterable['FetchTask']:
        info = self.info
        dest: Path = Path('downloaded') / info.album / info.song

        if dest.exists():
            logger.info(f'{str(dest)} already exists')
            return

        dest.parent.mkdir()
        res = await cs.get(info)

        # To atomically create the file, write download to a temp file.
        tempfile = dest.with_suffix('.tmp')
        with tempfile.open("wb") as f:
            async for data in res.content.iter_chunked(1024):
                f.write(data)

        tempfile.rename(dest)
        logger.info(f'Downloaded {info.url} to {dest}')


async def download_all_song_infos(cs: ClientSession, max_queuesize=10000, n_workers=30) -> Iterable[SongInfo]:
    logger.info("Initializing")

    task_queue: asyncio.Queue[FetchTask] = asyncio.Queue(maxsize=max_queuesize)
    for letter in letter_urls:
        task_queue.put_nowait(LetterFetch(letter))

    async def worker():
        while True:
            task = await task_queue.get()
            logger.debug("Got queue item %s", task)
            result = await task.fetch(cs)
            for i in result:
                await task_queue.put(i)

    tasks = [
        asyncio.create_task(worker())
        for _ in range(n_workers)
    ]

    # Wait until the queue is fully processed.
    await task_queue.join()

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()

    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)
