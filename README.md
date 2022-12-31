# Sparkfy

## Overview

Welcome to Sparkfy! This database is designed to provide insights into user behavior, including location, web browser, device, and artist preferences. With this data, we can gain a deeper understanding of our users and discover opportunities for growth and improvement.

For instance, we can use this data to see which music or artists are most popular in specific locations, identify trends in artist or music preferences, and determine which devices and browsers are most popular among free or paid users. The possibilities are endless!

But the real power of this database lies in its versatility and the potential for creative analysis. By combining and analyzing different dimensions of the data, we can uncover valuable insights that can help improve the user experience and drive profits for our startup. With the help of data analytics and data science, we can turn this data into actionable strategies that drive real results. So let's dive in and see what this amazing database can do!


## Repository Contents

This repository contains the following files and directories:

`etl.py`: This script extracts data from various source files, transforms it into a format suitable for analysis, and loads it into the database.

`create_tables.py`: This script creates the necessary tables in the database to store the data. It should be run before running the etl.py script.

`sql_queries.py`: This script contains all the SQL queries used in create_tables.py and etl.py.

`requirements.txt`: This file lists the Python packages that are required to run the scripts in this repository.

`data/song_data`: This directory contains a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist who performed it.

`data/log_data`: This directory contains simulated activity logs generated with an event simulator based on the songs in the data/song_data dataset. These logs simulate activity from a music streaming app based on specified configurations.

`test.ipynb`: This Jupyter notebook tests all tables created after running create_tables.py and etl.py to verify that they meet the requirements of the project.

## Requirements

- PostgreSQL
- Python
- pip


## Get Started
Follow these steps to get started with Sparkfy:

  1. Install the Python requirements by running the following command:

   ```bash
   pip install -r requirements.txt 
   ```
  2. Create a user for PostgreSQL, or update the username and password in the `etl.py` and `create_tables.py` files. To create a user, run the following SQL command:

   ```sql
   CREATE USER newuser WITH PASSWORD 'password';
   ```
  3. Run the `create_tables.py` script to set up the database. This should be done the first time you use the database, or anytime you want to run the `etl.py` script again:
   
   ```bash
   python create_tables.py
   ```

  4. Run the etl.py script to load data into the database:
   
   ```bash
   python etl.py
   ```

  5. Explore the data and try to uncover insights!


## Description

In this project, our goal is to apply the knowledge we have gained about Star Schema, Snowflake Schema, and PostgreSQL. After considering the various options, we decided to go with a Star Schema design for our database. We chose this approach because it is a good fit for our data, which has a simple structure and does not require complex transformations.

Additionally, we found that the Star Schema design is more natural and intuitive, and it allows us to work with the data in a more straightforward way. We decided to use Python's built-in libraries for data transformation and ingestion, as they offer good memory and speed performance for our needs. If our dataset were to grow significantly larger, we might consider using PySpark or Apache Bean to achieve even better performance.

This is the schema we have built:

### Fact Table

1. `songplays`: records in log data associated with song plays i.e. records with page `NextSong`

| FIELD | TYPE | PRIMARY_KEY | NOT NULL |
|-------|------|-------------|----------|
| songplay_id | SERIAL | X | X |
| start_time | TIMESTAMP | | X |
| user_id | INT | | X |
| level | VARCHAR |
| song_id | VARCHAR |
| artist_id | VARCHAR |
| session_id | INT |
| location | VARCHAR |
| user_agent | VARCHAR |


### Dimesion Tables

1. `users`: users in the app

| FIELD | TYPE | PRIMARY_KEY | NOT NULL |
|-------|------|-------------|----------|
| user_id | INT | X | X |
| first_name | VARCHAR | |
| last_name | VARCHAR | |
| gender | VARCHAR | |
| level | VARCHAR | |

2. `artists`: artist in music database

| FIELD | TYPE | PRIMARY_KEY | NOT NULL |
|-------|------|-------------|----------|
| artist_id | VARCHAR | X | X |
| name | VARCHAR | | X |
| location | VARCHAR | |
| latitude | FLOAT | | 
| longitude | FLOAT | |

3. `songs`: songs in music database

| FIELD | TYPE | PRIMARY_KEY | NOT NULL |
|-------|------|-------------|----------|
| song_id | VARCHAR | X | X |
| title | VARCHAR | | X |
| artist_id | VARCHAR | | |
| year | INT | | |
| duration | FLOAT |  | X |

4. `time`: timestamps of records in `songplays` broken down into specific units.

| FIELD | TYPE | PRIMARY_KEY | NOT NULL |
|-------|------|-------------|----------|
| start_time | TIMESTAMP | X | X |
| hour | INT | | |
| day | INT | | |
| week | INT | | |
| month | INT | | |
| year | INT | | |
| weekday | INT | | |