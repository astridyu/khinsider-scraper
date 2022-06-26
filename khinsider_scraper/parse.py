from typing import Iterable, NamedTuple, Optional
import bs4
import re


def get_last_letter_page(soup: bs4.BeautifulSoup) -> int:
    anchor = soup.select_one('.pagination .pagination-end a')
    if anchor is None:
        return 1
    
    href = anchor.attrs.get('href')
    if href is None:
        return 1
    
    match = re.search(r'\?page=(\d+)', href)
    if match is None or match.group(1) is None:
        return 1

    return int(match.group(1))


def get_hrefs(tags: Iterable[bs4.Tag]) -> Iterable[str]:
    for t in tags:
        href = t.attrs.get('href')
        if href is not None:
            yield href


def get_album_links_on_letter_page(soup: bs4.BeautifulSoup) -> Iterable[str]:
    return get_hrefs(soup.select('.albumList tr .albumIcon a'))


def get_songs_on_album_page(soup: bs4.BeautifulSoup) -> Iterable[str]:
    return get_hrefs(soup.select('#songlist tr .playlistDownloadSong a'))


class SongInfo(NamedTuple):
    album: str
    song: Optional[str]
    url: str


def get_song_slug_from_url(url: str) -> Optional[SongInfo]:
    match = re.match(r'https://downloads.khinsider.com/game-soundtracks/album/(.+)/(.*)', url)
    if match is None:
        return None
    _, album, song = match.groups()
    return SongInfo(album=album, song=song, url=url)

