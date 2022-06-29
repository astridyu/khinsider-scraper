

import asyncio
from concurrent.futures import Executor, ThreadPoolExecutor
import csv
from dataclasses import dataclass
import logging
from multiprocessing.dummy import current_process
from pathlib import Path
import sys
from tempfile import TemporaryFile
import time
from traceback import print_exception
from typing import Iterable, List, NamedTuple, TextIO
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from khinsider_scraper.parse import SongInfo, get_album_links_on_letter_page, get_last_letter_page, get_mp3_on_song_page, get_songs_on_album_page

logger = logging.getLogger(__name__)


letter_url_fmt = "https://downloads.khinsider.com/game-soundtracks/browse/{}"
letters = ["%23"] + list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
letter_urls = [letter_url_fmt.format(l) for l in letters]
max_attempts = 10


class FetchTask:
    async def fetch(self, cs: ClientSession, csvwriter: csv.writer, pool: Executor) -> Iterable['FetchTask']:
        raise NotImplemented


def offloaded(pool: Executor):
    async def decorator(func):
        return asyncio.get_event_loop().run_in_executor(pool, func)
    return decorator


@dataclass
class SongFetch(FetchTask):
    song: SongInfo

    async def fetch(self, cs: ClientSession, csvwriter: csv.writer, pool: Executor) -> Iterable['FetchTask']:
        logger.info(f'Fetching song at URL {self.song.url}')
        res = await cs.get(self.url)
        html = await res.read()

        def parse():
            soup = BeautifulSoup(html, features='html5lib')
            mp3 = get_mp3_on_song_page(soup, self.url)
            return mp3

        mp3 = await asyncio.get_event_loop().run_in_executor(pool, parse)

        csvwriter.writerow(self.song._replace(url=mp3))
        return []


@dataclass
class AlbumFetch(FetchTask):
    url: str

    async def fetch(self, cs: ClientSession, csvwriter: csv.writer, pool: Executor) -> Iterable['FetchTask']:
        logger.info(f'Fetching album at URL {self.url}')
        res = await cs.get(self.url)
        html = await res.read()

        def parse():
            soup = BeautifulSoup(html, features='html5lib')
            infos = list(get_songs_on_album_page(soup, self.url))
            return infos

        infos = await asyncio.get_event_loop().run_in_executor(pool, parse)
        logger.info(f'Found {len(infos)} songs at {self.url}')

        return (SongFetch(info) for info in infos)


@dataclass
class LetterPageFetch(FetchTask):
    url: str

    async def fetch(self, cs: ClientSession, csvwriter: csv.writer, pool: Executor) -> Iterable['FetchTask']:
        logger.info(f'Fetching letter page at URL {self.url}')
        res = await cs.get(self.url)
        html = await res.read()

        def parse():
            soup = BeautifulSoup(html, features='html5lib')
            links = get_album_links_on_letter_page(soup)
            return links

        urls = await asyncio.get_event_loop().run_in_executor(pool, parse)

        return (AlbumFetch(url) for url in urls)


@dataclass
class LetterFetch(FetchTask):
    url: str

    async def fetch(self, cs: ClientSession, csvwriter: csv.writer, pool: Executor) -> Iterable['FetchTask']:
        logger.info(f'Fetching information about letter at URL {self.url}')
        res = await cs.get(self.url)
        html = await res.read()

        def parse():
            soup1 = BeautifulSoup(html, features='html5lib')
            count = get_last_letter_page(soup1)
            links = get_album_links_on_letter_page(soup1)
            return count, links

        count, links = await asyncio.get_event_loop().run_in_executor(pool, parse)

        def result():
            for url in links:
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


async def download_all_song_infos(cs: ClientSession, out_file: TextIO, n_workers=50) -> Iterable[SongInfo]:
    logger.info("Initializing")
    csvwriter = csv.writer(out_file)
    csvwriter.writerow(SongInfo._fields)

    task_queue: asyncio.Queue[FetchTask] = asyncio.Queue()
    for letter in letter_urls:
        task_queue.put_nowait(LetterFetch(letter))

    currently_processing: int = n_workers

    async def worker(pool: Executor):
        nonlocal currently_processing
        while True:
            currently_processing -= 1
            task = await task_queue.get()
            currently_processing += 1
            for i in range(max_attempts):
                logger.debug("Attempt %d/%d on %s", i + 1, max_attempts, task)
                try:
                    result = await task.fetch(cs, csvwriter, pool)
                except Exception:
                    logger.exception('Error while fetching object %s', task)
                    continue
                for i in result:
                    await task_queue.put(i)
                break

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        tasks = [
            asyncio.create_task(worker(pool))
            for _ in range(n_workers)
        ]

        # Wait until the queue is fully processed.
        while currently_processing > 0 or not task_queue.empty():
            await task_queue.join()

        # Cancel our worker tasks.
        for task in tasks:
            task.cancel()

    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)
