"""
This file contains all the SQL queries used in the ETL pipeline.
"""

# DROP TABLES

SONGPLAY_TABLE_DROP = "DROP TABLE IF EXISTS songplays"
USER_TABLE_DROP = "DROP TABLE IF EXISTS users"
SONG_TABLE_DROP = "DROP TABLE IF EXISTS songs"
ARTIST_TABLE_DROP = "DROP TABLE IF EXISTS artists"
TIME_TABLE_DROP = "DROP TABLE IF EXISTS time"

# CREATE TABLES

SONGPLAY_TABLE_CREATE = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
)
"""

USER_TABLE_CREATE = """
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
    )
"""

SONG_TABLE_CREATE = """
CREATE TABLE IF NOT EXISTS songs (
                     song_id VARCHAR PRIMARY KEY,
                     title VARCHAR NOT NULL,
                     artist_id VARCHAR,
                     year int,
                     duration float NOT NULL )
"""

ARTIST_TABLE_CREATE = """
CREATE TABLE IF NOT EXISTS artists (
                       artist_id VARCHAR PRIMARY KEY,
                       name VARCHAR NOT NULL,
                       location VARCHAR,
                       latitude float,
                       longitude float )
"""

TIME_TABLE_CREATE = """
CREATE TABLE IF NOT EXISTS time (
                    start_time timestamp,
                    hour int,
                    day int,
                    week int,
                    month int,
                    year int,
                    weekday int
                    )
"""

# INSERT RECORDS

SONGPLAY_TABLE_INSERT = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO NOTHING
"""

USER_TABLE_INSERT = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
"""

SONG_TABLE_INSERT = SONG_TABLE_INSERT = """
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING
"""

ARTIST_TABLE_INSERT = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING
"""

TIME_TABLE_INSERT = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# FIND SONGS

SONG_SELECT = """
SELECT s.song_id, a.artist_id FROM songs s JOIN artists a ON s.artist_id = a.artist_id WHERE s.title = %s AND a.name = %s
"""

# QUERY LISTS

CREATE_TABLE_QUERIES = [
    SONGPLAY_TABLE_CREATE,
    USER_TABLE_CREATE,
    SONG_TABLE_CREATE,
    ARTIST_TABLE_CREATE,
    TIME_TABLE_CREATE,
]

DROP_TABLE_QUERIES = [
    SONGPLAY_TABLE_DROP,
    USER_TABLE_DROP,
    SONG_TABLE_DROP,
    ARTIST_TABLE_DROP,
    TIME_TABLE_DROP,
]
