import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

DWH_DB= config.get("CLUSTER","DB_NAME")
DWH_DB_USER= config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
DWH_PORT = config.get("CLUSTER","DB_PORT")

DWH_ENDPOINT=config.get("CLUSTER","HOST")
DWH_ROLE_ARN=config.get("IAM_ROLE","ARN")

LOG_DATA=config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = (""" 
create table staging_events(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession smallint,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    methold varchar,
    page varchar,
    registration bigint,
    sessionId int,
    song varchar,
    status smallint,
    ts bigint,
    userAgent varchar,
    userId varchar
)
""")

staging_songs_table_create = ("""
create table staging_songs(
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year smallint
)
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY,
    start_time bigint NOT NULL,
    user_id varchar NOT NULL,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar  PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar,
    year int,
    artist_id varchar,
    duration float
)SORTKEY(title)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar,
    latitude float,
    longitude float,
    location varchar
) SORTKEY(name)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint PRIMARY KEY,
    ts timestamp,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint
)
""")

# STAGING TABLES
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")
staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json '{}'
    compupdate off region 'us-west-2';
""").format(LOG_DATA,DWH_ROLE_ARN,LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto'
    compupdate off region 'us-west-2';
""").format(SONG_DATA,DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, song_id,artist_id,session_id,location, user_agent)
(
    SELECT 
        ts as start_time,
        userId as user_id,
        songs.Song_id as song_id,
        artists.artist_id as artist_id,
        sessionId as session_id,
        staging_events.location as location,
        userAgent as user_agent
    FROM
        staging_events
    JOIN songs ON songs.title=staging_events.song
    JOIN artists ON artists.name=staging_events.artist
    WHERE page='NextSong'
)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
(
    SELECT DISTINCT(userId),firstName,lastName, gender,level  
    From staging_events
)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, year, artist_id, duration)
(
    SELECT DISTINCT(song_id), title, year, artist_id, duration
    FROM staging_songs
)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id ,name, latitude, longitude, location)
(
    SELECT DISTINCT(artist_id), artist_name, artist_latitude, artist_longitude, artist_location
    FROM staging_songs
)
""")


time_table_insert = ("""
INSERT INTO time (start_time, ts, hour, day, week, month, year, weekday) 
(
    SELECT  
    DISTINCT(ts) as start_time,
    timestamp 'epoch' + start_time/1000 *INTERVAL '1 second' as t,
    extract(hour from t) as hour,
    extract(day from t) as day,
    extract(week from t) as week,
    extract(month from t) as month,
    extract(year from t) as year,
    extract(weekday from t) as weekday
    from staging_events
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [song_table_insert, artist_table_insert, songplay_table_insert, user_table_insert, time_table_insert]
