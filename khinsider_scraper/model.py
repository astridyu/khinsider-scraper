from sqlite3 import Connection


CREATE_TABLES = '''
CREATE TABLE IF NOT EXISTS albumpages(
    page INTEGER PRIMARY KEY,
    visited INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS albums(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    visited INTEGER DEFAULT 0,
    album_id TEXT UNIQUE,
    album_name TEXT,
    album_url TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS songs(
    visited INTEGER DEFAULT 0,
    album INTEGER REFERENCES albums(id),
    album_index INTEGER,
    song_name TEXT,
    song_id TEXT,
    page_url TEXT UNIQUE,
    mp3_url TEXT UNIQUE,

    PRIMARY KEY (album, album_index)
);
'''

def create_tables(db: Connection):
    db.executescript(CREATE_TABLES)
