from concurrent.futures import ThreadPoolExecutor
import csv
from sqlite3 import Connection
import click
import asyncio
import logging
from aiohttp import ClientSession, TCPConnector

from .parse import SongInfo

from .scrape import ScrapeContext, fetch_and_store_song, build_index


@click.group()
def cli():
    """khinsider scraper script, complete with download resuming (assuming you've built an index first)."""
    pass


@cli.command()
@click.option('-d', '--db', default='index.db', help='The database to use for indexing.', type=str)
@click.option('-j', '--workers', default=400, help='Number of worker coroutines.', type=int)
@click.option('-c', '--max-connections', default=100, help='Max number of connections.', type=int)
def index(workers, max_connections, db):
    """Build an index of the files to download. This is needed before downloading the songs.

    NOTE: This command will destroy any existing index you have!"""

    async def main():
        with Connection(db) as conn, ThreadPoolExecutor(workers) as exec:
            async with ClientSession(connector=TCPConnector(limit=max_connections)) as cs:
                await build_index(ScrapeContext(cs, conn, exec))
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())


@cli.command()
@click.option('-d', '--db', default='index.db', help='The database to use for indexing.', type=str)
@click.option('-c', '--max-connections', default=20, help='Max number of connections.', type=int)
def download(max_connections, index_file):
    """Download all the songs in the given index.

    Make sure you build an index first using the index command."""

    async def main():
        async with ClientSession(connector=TCPConnector(limit=max_connections)) as cs:
            sem = asyncio.Semaphore(max_connections)

            async def task(song: SongInfo):
                async with sem:
                    await fetch_and_store_song(song, cs)

            tasks = []
            with open(index_file, 'r') as f:
                tasks = [
                    task(SongInfo(
                        index=int(r['index']),
                        album_name=str(r['album_name']),
                        album_id=str(r['album_id']),
                        song_name=str(r['song_name']),
                        file_path=str(r['file_path']),
                        url=str(r['url']),
                    ))
                    for r in csv.DictReader(f)
                ]
            await asyncio.gather(*tasks)

    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
