from sqlite3 import Connection


CREATE_TABLES = '''
CREATE TABLE IF NOT EXISTS albumpages(
    page INTEGER PRIMARY KEY,
    visited INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS albums(
    album_id TEXT PRIMARY KEY,
    visited INTEGER DEFAULT 0,
    album_name TEXT,
    album_url TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS songs(
    visited INTEGER DEFAULT 0,
    album TEXT REFERENCES albums(album_id),
    album_index INTEGER,
    song_name TEXT,
    page_url TEXT UNIQUE,
    mp3_url TEXT UNIQUE,

    PRIMARY KEY (album, album_index)
);
'''

def create_tables(db: Connection):
    db.executescript(CREATE_TABLES)
