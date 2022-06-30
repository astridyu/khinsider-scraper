from typing import Iterable, NamedTuple, Optional
import bs4
import re

from slugify import slugify


class SongInfo(NamedTuple):
    index: int
    song_name: str
    url: str
    """This value points to the song page path."""


def get_last_letter_page(soup: bs4.BeautifulSoup) -> int:
    anchor = soup.select_one('.pagination .pagination-end a')
    if anchor is None:
        return 1

    href = anchor.attrs.get('href')
    if href is None:
        return 1

    match = re.search(r'page=(\d+)', href)
    if match is None or match.group(1) is None:
        return 1

    return int(match.group(1))


def get_hrefs(tags: Iterable[bs4.Tag]) -> Iterable[str]:
    for t in tags:
        href = t.attrs.get('href')
        if href is not None:
            yield href


def get_album_links_on_letter_page(soup: bs4.BeautifulSoup) -> Iterable[str]:
    return ('https://downloads.khinsider.com' + u for u in get_hrefs(soup.select('.albumList tr .albumIcon a')))


def get_album_id_from_url(url: str):
    return re.search('/game-soundtracks/album/(.*)/?.*', url).group(1)


def parse_album_name(soup: bs4.BeautifulSoup) -> str:
    return soup.select_one('#pageContent > h2:first-of-type').text


def get_songs_on_album_page(soup: bs4.BeautifulSoup) -> Iterable[SongInfo]:
    header = [x.text.strip().lower() for x in soup.select('#songlist_header th')]
    songname_index = header.index('song name')

    for i, row in enumerate(soup.select('#songlist tr:not(:first-child):not(:last-child)')):
        tds = list(row.select('td'))
        song_name = tds[songname_index].text.strip()

        url = 'https://downloads.khinsider.com' + row.select_one('.playlistDownloadSong a').attrs['href']

        yield SongInfo(
            index=i,
            song_name=song_name,
            url=url,
        )


def get_mp3_on_song_page(soup: bs4.BeautifulSoup) -> str:
    return soup.select_one('audio').attrs['src']
