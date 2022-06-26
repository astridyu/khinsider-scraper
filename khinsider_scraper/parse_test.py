import bs4
from .parse import *
import pytest


def album_soup_with_cd() -> str:
    with open('khinsider_scraper/tests/album_with_cd.html') as f:
        return bs4.BeautifulSoup(f.read(), features='html5lib')


def album_soup_without_cd() -> str:
    with open('khinsider_scraper/tests/album_without_cd.html') as f:
        return bs4.BeautifulSoup(f.read(), features='html5lib')


def letter_with_seek_soup():
    with open('khinsider_scraper/tests/letter_with_seek.html') as f:
        return bs4.BeautifulSoup(f.read(), features='html5lib')


def letter_without_seek_soup():
    with open('khinsider_scraper/tests/letter_without_seek.html') as f:
        return bs4.BeautifulSoup(f.read(), features='html5lib')


@pytest.mark.parametrize("soup,expected", [(letter_with_seek_soup(), 7), (letter_without_seek_soup(), 1)])
def test_get_last_letter_page(soup: bs4.BeautifulSoup, expected: int):
    result = get_last_letter_page(soup)
    assert result == expected


def test_get_album_links_on_letter_page():
    soup = letter_with_seek_soup()
    result = list(get_album_links_on_letter_page(soup))
    assert result[3] == 'https://downloads.khinsider.com/game-soundtracks/album/t-e-vr-golf-devils-course-1995-3do'
    assert len(result) == 500


def test_get_songs_on_album_page_with_cd():
    soup = album_soup_with_cd()
    url = 'https://downloads.khinsider.com/game-soundtracks/album/quake-iii-arena-complete-soundtrack'
    result = list(get_songs_on_album_page(soup, url))
    assert result[3] == SongInfo(cd=1, number=4, album_name='Quake 3 - Arena Complete Soundtrack', album_id='quake-iii-arena-complete-soundtrack', song_name="Hell's Gate",
                                 file_path='quake-iii-arena-complete-soundtrack/1-4-hell-s-gate.mp3', url='https://downloads.khinsider.com/game-soundtracks/album/quake-iii-arena-complete-soundtrack/1-04%2520Hell%2527s%2520Gate.mp3')
    assert len(result) == 50


def test_get_songs_on_album_page_without_cd():
    soup = album_soup_without_cd()
    url = 'https://downloads.khinsider.com/game-soundtracks/album/t-e-vr-golf-devils-course-1995-3do'
    result = list(get_songs_on_album_page(soup, url))
    assert result[3] == SongInfo(cd=None, number=4, album_name='T&E VR Golf - Devils Course', album_id='t-e-vr-golf-devils-course-1995-3do', song_name='Tea Break',
                                 file_path='t-e-vr-golf-devils-course-1995-3do/4-tea-break.mp3', url='https://downloads.khinsider.com/game-soundtracks/album/t-e-vr-golf-devils-course-1995-3do/04%2520Tea%2520Break.mp3')
    assert len(result) == 8
