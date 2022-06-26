import bs4
from .parse import *
import pytest

def album_soup() -> str:
    with open('khinsider_scraper/tests/album.html') as f:
        return bs4.BeautifulSoup(f.read())

def letter_with_seek_soup():
    with open('khinsider_scraper/tests/letter_with_seek.html') as f:
        return bs4.BeautifulSoup(f.read())


def letter_without_seek_soup():
    with open('khinsider_scraper/tests/letter_without_seek.html') as f:
        return bs4.BeautifulSoup(f.read())


@pytest.mark.parametrize("soup,expected", [(letter_with_seek_soup(), 7), (letter_without_seek_soup(), 1)])
def test_get_last_letter_page(soup: bs4.BeautifulSoup, expected: int):
    result = get_last_letter_page(soup)
    assert result == expected

def test_get_album_links_on_letter_page():
    soup = letter_with_seek_soup()
    result = list(get_album_links_on_letter_page(soup))
    assert result[3] == 'https://downloads.khinsider.com/game-soundtracks/album/t-e-vr-golf-devils-course-1995-3do'
    assert len(result) == 500

def test_get_songs_on_album_page():
    soup = album_soup()
    result = list(get_songs_on_album_page(soup))
    assert result[3] == r'https://downloads.khinsider.com/game-soundtracks/album/quake-iii-arena-complete-soundtrack/1-04%2520Hell%2527s%2520Gate.mp3'
    assert len(result) == 50
