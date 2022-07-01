import asyncio
import logging
from concurrent.futures import Executor, ThreadPoolExecutor
from multiprocessing.pool import ThreadPool
from pathlib import Path
from sqlite3 import Connection
from typing import Iterable, NamedTuple

import requests
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from .model import create_tables
from .parse import (SongInfo, get_album_links_on_letter_page,
                    get_last_letter_page, get_mp3_on_song_page,
                    get_songs_on_album_page, parse_album_name)

logger = logging.getLogger(__name__)

max_attempts = 10


class ScrapeContext(NamedTuple):
    dburl: str
    pool: ThreadPool

    def get_db(self) -> Connection:
        return Connection(self.dburl, check_same_thread=False)


def build_index(ctx: ScrapeContext) -> Iterable[SongInfo]:
    logger.info("Initializing")

    with ctx.get_db() as db:
        create_tables(db)
    enumerate_pages(ctx)
    enumerate_albums(ctx)
    fetch_albums_info(ctx)
    fetch_song_mp3_links(ctx)


def enumerate_pages(ctx: ScrapeContext):
    with ctx.get_db() as db:
        if db.execute('SELECT COUNT(*) FROM albumpages').fetchone()[0] > 0:
            logger.info(f'Song page count already enumerated')
            return

    logger.info(f'Counting number of pages of songs')
    html = requests.get(
        "https://downloads.khinsider.com/game-soundtracks").text

    soup1 = BeautifulSoup(html, features='html5lib')
    count = get_last_letter_page(soup1)
    logger.info(f'There are {count} pages of songs')
    links = get_album_links_on_letter_page(soup1)

    with ctx.get_db() as db:
        db.executemany(
            'INSERT INTO albums(album_url) VALUES (?) ON CONFLICT DO NOTHING',
            ((l,) for l in links)
        )
        db.executemany(
            'INSERT INTO albumpages(page, visited) VALUES (?, ?) ON CONFLICT DO NOTHING',
            [(1, 1)] + [(i, 0) for i in range(2, count + 1)]
        )


def enumerate_albums(ctx: ScrapeContext):
    def task(page: int):
        url = f"https://downloads.khinsider.com/game-soundtracks?page={page}"
        logger.info(f'Fetching album listing page {page} at URL {url}')
        html = requests.get(url).text

        soup = BeautifulSoup(html, features='html5lib')
        links = list(get_album_links_on_letter_page(soup))

        logger.info(f'Got {len(links)} albums on page {page}')

        return page, links

    logger.info('Crawling unvisited album listings')
    with ctx.get_db() as conn:
        rows = conn.execute('SELECT page FROM albumpages WHERE visited = 0')
        for page, links in ctx.pool.imap_unordered(lambda r: task(*r), rows, chunksize=2):
            conn.execute(
                'UPDATE albumpages SET visited = 1 WHERE page = ?', (page,))
            conn.executemany(
                'INSERT INTO albums(album_url) VALUES (?) ON CONFLICT DO NOTHING',
                ((l,) for l in links)
            )


def fetch_albums_info(ctx: ScrapeContext):
    def task(album_id: str, url: str):
        logger.info(f'Fetching album at URL {url}')
        html = requests.get(url).text

        soup = BeautifulSoup(html, features='html5lib')
        infos = list(get_songs_on_album_page(soup))
        album_name = parse_album_name(soup)

        logger.info(f'Got {len(infos)} songs on album {url}')

        return album_id, album_name, infos

    logger.info('Crawling unvisited albums')
    with ctx.get_db() as conn:
        rows = conn.execute(
            'SELECT album_id, album_url FROM albums WHERE visited = 0')
        for album_id, album_name, infos in ctx.pool.imap_unordered(lambda r: task(*r), rows, chunksize=16):
            conn.execute(
                'UPDATE albums SET visited = 1, album_name = ? WHERE album_id = ?', (album_name, album_id))
            conn.executemany(
                'INSERT INTO songs(album, album_index, song_name, page_url) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING',
                ((album_id, s.index, s.song_name, s.url) for s in infos)
            )


def fetch_song_mp3_links(ctx: ScrapeContext):
    def task(album_id: str, album_index: int, page_url: str):
        logger.info(f'Fetching song at URL {page_url}')
        html = requests.get(page_url).text

        soup = BeautifulSoup(html, features='html5lib')
        mp3_url = get_mp3_on_song_page(soup)

        logger.info(f'Song at {page_url} has mp3 at {mp3_url}')

        return album_id, album_index, mp3_url

    logger.info('Crawling unvisited songs')
    with ctx.get_db() as conn:
        rows = conn.execute(
            'SELECT album, album_index, page_url FROM songs WHERE visited = 0')
        for album_id, album_index, mp3_url in ctx.pool.imap_unordered(lambda r: task(*r), rows, chunksize=32):
            conn.execute(
                'UPDATE songs SET mp3_url = ?, visited = 1 WHERE album = ? AND album_index = ?',
                (mp3_url, album_id, album_index)
            )


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


async def download_all_song_infos(cs: ClientSession, db: Connection, n_workers=50) -> Iterable[SongInfo]:
    logger.info("Initializing")

    create_tables(db)

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
