# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id serial NOT NULL PRIMARY KEY,
    start_time bigint NOT NULL,
    user_id varchar NOT NULL,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar NOT NULL PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar NOT NULL PRIMARY KEY,
    title varchar,
    year int,
    artist_id varchar,
    duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar NOT NULL PRIMARY KEY,
    name varchar,
    latitude float,
    longitude float,
    location varchar
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint NOT NULL,
    hour smallint,
    day smallint,
    week smallint,
    month int,
    year smallint,
    weekday smallint
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, song_id,artist_id,session_id,location, user_agent) 
VALUES (%s, %s, %s,%s, %s, %s, %s) 
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(user_id) DO UPDATE SET level = excluded.level
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, year, artist_id, duration ) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id ,name, latitude, longitude, location) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s) 
""")

# FIND SONGS

song_select = ("""
SELECT song_id, artists.artist_id FROM artists \
JOIN songs ON artists.artist_id=songs.artist_id \
WHERE title=%s AND name=%s AND duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]