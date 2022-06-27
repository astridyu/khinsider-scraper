

import asyncio
import csv
from dataclasses import dataclass
import logging
from pathlib import Path
import sys
from tempfile import TemporaryFile
import time
from traceback import print_exception
from typing import Iterable, List, NamedTuple, TextIO
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from khinsider_scraper.parse import SongInfo, get_album_links_on_letter_page, get_last_letter_page, get_songs_on_album_page

logger = logging.getLogger(__name__)


letter_url_fmt = "https://downloads.khinsider.com/game-soundtracks/browse/{}"
letters = ["%23"] + list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
letter_urls = [letter_url_fmt.format(l) for l in letters]
max_attempts = 10


class FetchTask:
    async def fetch(self, cs: ClientSession, csvwriter: csv.writer) -> Iterable['FetchTask']:
        raise NotImplemented


@dataclass
class AlbumFetch(FetchTask):
    url: str

    async def fetch(self, cs: ClientSession, csvwriter: csv.writer) -> Iterable['FetchTask']:
        logger.info(f'Fetching album at URL {self.url}')
        res = await cs.get(self.url)
        html = await res.read()
        soup = BeautifulSoup(html, features='html5lib')
        infos = list(get_songs_on_album_page(soup, self.url))
        logger.info(f'Found {len(infos)} songs at {self.url}')
        for info in infos:
            csvwriter.writerow(info)
        return []


@dataclass
class LetterPageFetch(FetchTask):
    url: str

    async def fetch(self, cs: ClientSession, csvwriter: csv.writer) -> Iterable['FetchTask']:
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

    async def fetch(self, cs: ClientSession, csvwriter: csv.writer) -> Iterable['FetchTask']:
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


async def fetch_and_store_song(song: SongInfo, cs: ClientSession) -> Iterable['FetchTask']:
    dest: Path = Path('songs') / song.file_path
    if dest.exists():
        logger.debug(f'{str(dest)} already exists')
        return

    logger.info('Fetching song %s', song)
    tempfile: Path = Path('.songcache') / song.file_path
    tempfile.parent.mkdir(parents=True, exist_ok=True)

    res = await cs.get(song.url)

    # First download to a temporary file so that incomplete files don't make their way into the results
    with tempfile.open("wb") as f:
        async for data in res.content.iter_chunked(1024):
            f.write(data)

    # Move the temporary file into the destination
    dest.parent.mkdir(parents=True, exist_ok=True)
    tempfile.rename(dest)
    logger.info(f'Downloaded {song.url} to {dest}')
    return []


async def download_all_song_infos(cs: ClientSession, out_file: TextIO, n_workers=800) -> Iterable[SongInfo]:
    logger.info("Initializing")
    csvwriter = csv.writer(out_file)
    csvwriter.writerow(SongInfo._fields)

    task_queue: asyncio.Queue[FetchTask] = asyncio.Queue()
    for letter in letter_urls:
        task_queue.put_nowait(LetterFetch(letter))

    async def worker():
        while True:
            task = await task_queue.get()
            for i in range(max_attempts):
                logger.debug("Attempt %d/%d on %s", i + 1, max_attempts, task)
                try:
                    result = await task.fetch(cs, csvwriter)
                except Exception:
                    logger.exception('Error while fetching object %s', task)
                    continue
                for i in result:
                    await task_queue.put(i)
                break

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
