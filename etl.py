"""
ETL pipeline for Sparkify database
"""
import os
import glob
import json
from datetime import datetime

from typing import List, Tuple, Any
import psycopg2

from sql_queries import (
    SONG_SELECT,
    ARTIST_TABLE_INSERT,
    SONG_TABLE_INSERT,
    TIME_TABLE_INSERT,
    USER_TABLE_INSERT,
    SONGPLAY_TABLE_INSERT,
)


def get_files_path(filepath: str) -> List[str]:
    """
    Get all files matching extension from directory

    Args:
        filepath (str): path to directory

    Returns:
        List[str]: list of files
    """

    if not isinstance(filepath, str):
        raise ValueError("filepath must be a string")

    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def execute_many(cur, query: str, data: List[Tuple[Any, ...]]):
    """
    Execute many query

    Args:
        cur (cursor): cursor
        query (str): query
        data (List[Tuple[Any, ...]]): data

    Returns:
        None
    """

    if not isinstance(query, str):
        raise ValueError("query must be a string")
    if not isinstance(data, list) or not all(isinstance(d, tuple) for d in data):
        raise ValueError("data must be a list of tuples")

    try:
        for item in data:
            cur.execute(query, item)
        cur.connection.commit()
    except psycopg2.Error as error_message:
        cur.connection.rollback()
        raise error_message


def process_song_file(cur, filepath):
    """
    Process song file

    Args:
        cur (cursor): cursor
        filepath (str): path to song file

    Returns:
        None
    """
    # open song file
    raw_data = [json.loads(line) for line in open(filepath, "r", encoding="utf8")]

    # insert song record
    song_data = [
        (row["song_id"], row["title"], row["artist_id"], row["year"], row["duration"])
        for row in raw_data
    ]

    execute_many(cur, SONG_TABLE_INSERT, song_data)

    # insert artist record
    artist_data = [
        (
            row["artist_id"],
            row["artist_name"],
            row["artist_location"],
            row["artist_latitude"],
            row["artist_longitude"],
        )
        for row in raw_data
    ]

    execute_many(cur, ARTIST_TABLE_INSERT, artist_data)


def process_log_file(cur, filepath):
    """
    Process log file

    Args:
        cur (cursor): cursor
        filepath (str): path to log file

    Returns:
        None
    """
    # open log file
    raw_data = [json.loads(line) for line in open(filepath, "r", encoding="utf8")]

    # filter by NextSong action
    next_song_data = [data for data in raw_data if data["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = [data["ts"] for data in next_song_data]

    # insert time data records
    time_data = [
        (
            datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S"),
            datetime.fromtimestamp(ts / 1000).hour,
            datetime.fromtimestamp(ts / 1000).day,
            datetime.fromtimestamp(ts / 1000).isocalendar()[1],
            datetime.fromtimestamp(ts / 1000).month,
            datetime.fromtimestamp(ts / 1000).year,
            datetime.fromtimestamp(ts / 1000).weekday(),
        )
        for ts in t
    ]

    execute_many(cur, TIME_TABLE_INSERT, time_data)

    # load user table
    user_data = (data for data in next_song_data)

    user_data = [
        (row["userId"], row["firstName"], row["lastName"], row["gender"], row["level"])
        for row in next_song_data
    ]
    # insert user records
    execute_many(cur, USER_TABLE_INSERT, user_data)

    # insert songplay records
    for row in next_song_data:

        # get songid and artistid from song and artist tables
        cur.execute(SONG_SELECT, (row["song"], row["artist"]))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            datetime.fromtimestamp(row["ts"] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
            row["userId"],
            row["level"],
            songid,
            artistid,
            row["sessionId"],
            row["location"],
            row["userAgent"],
        )
        cur.execute(SONGPLAY_TABLE_INSERT, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process data

    Args:
        cur (cursor): cursor
        conn (connection): connection
        filepath (str): path to directory
        func (function): function to process data

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = get_files_path(filepath)

    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    """
    Main function

    Returns:
        None
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
