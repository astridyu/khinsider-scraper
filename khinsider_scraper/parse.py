from typing import Iterable, NamedTuple, Optional
import bs4
import re

from slugify import slugify


class SongInfo(NamedTuple):
    cd: Optional[int]
    number: int
    album_name: str
    album_id: str
    song_name: str
    file_path: str
    url: str


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


def get_songs_on_album_page(soup: bs4.BeautifulSoup, url: str) -> Iterable[SongInfo]:
    album_id = re.search('/game-soundtracks/album/(.*)/?.*', url).group(1)
    album_name = soup.select_one('#pageContent > h2:first-of-type').text

    header = [x.text.strip().lower() for x in soup.select('#songlist_header th')]
    try:
        cd_index = header.index('cd')
    except ValueError:
        cd_index = None
    num_index = header.index('#')
    songname_index = header.index('song name')

    for row in soup.select('#songlist tr:not(:first-child):not(:last-child)'):
        tds = list(row.select('td'))

        cd = None
        filename = ''
        if cd_index:
            cd = int(tds[cd_index].text.strip())
            filename = str(cd) + '-'

        num = int(tds[num_index].text.strip()[:-1])
        filename += str(num) + '-'

        song_name = tds[songname_index].text.strip()
        filename += song_name

        url = 'https://downloads.khinsider.com' + row.select_one('.playlistDownloadSong a').attrs['href']

        file_path = album_id + '/' + slugify(filename) + '.mp3'
        yield SongInfo(
            cd=cd,
            number=num,
            album_name=album_name,
            album_id=album_id,
            song_name=song_name,
            file_path=file_path,
            url=url,
        )
