import os
import glob
import logging
import json
from datetime import datetime

import psycopg2
from psycopg2.errors import UniqueViolation
from typing import List, Dict, Any

from sql_queries import *


def execute_many(cur, query, data):
    for item in data:
        cur.execute(query, item)


def process_song_file(cur, filepath):
    # open song file
    raw_data = [json.loads(line) for line in open(filepath, "r")]

    # insert song record
    song_data = (
        (row["song_id"], row["title"], row["artist_id"], row["year"], row["duration"])
        for row in raw_data
    )

    execute_many(cur, song_table_insert, song_data)
    # insert artist record
    artist_data = ((
        row["artist_id"],
        row["artist_name"],
        row["artist_location"],
        row["artist_latitude"],
        row["artist_longitude"])
        for row in raw_data
    )

    execute_many(cur, artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    raw_data = [json.loads(line) for line in open(filepath, "r")]

    # filter by NextSong action
    next_song_data = (data for data in raw_data if data["page"] == "NextSong")

    # convert timestamp column to datetime
    t = (data["ts"] for data in next_song_data)

    # insert time data records
    time_data = (
        (
            datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M:%S"),
            datetime.fromtimestamp(ts/1000).hour,
            datetime.fromtimestamp(ts/1000).day,
            datetime.fromtimestamp(ts/1000).isocalendar()[1],
            datetime.fromtimestamp(ts/1000).month,
            datetime.fromtimestamp(ts/1000).year,
            datetime.fromtimestamp(ts/1000).weekday(),
        )
        for ts in t
    )

    execute_many(cur, time_table_insert, time_data)

    # load user table
    user_data = (data for data in next_song_data)

    user_data = (
        (row["userId"], row["firstName"], row["lastName"], row["gender"], row["level"])
        for row in next_song_data
    )
    # insert user records
    execute_many(cur, user_table_insert, user_data)

    # insert songplay records
    for row in next_song_data:

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row["song"], row["artist"], row["length"]))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row["ts"],
            row["userId"],
            row["level"],
            songid,
            artistid,
            row["sessionId"],
            row["location"],
            row["userAgent"],
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
