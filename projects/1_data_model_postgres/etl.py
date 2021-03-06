import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *

'''
    This procedure processes a song file whose filepath has been provided as an argument.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
'''
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data=df[["song_id", "title", "year", "artist_id", "duration"]].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_latitude","artist_longitude","artist_location"]].values[0]
    cur.execute(artist_table_insert, artist_data)

'''
    This procedure processes a log file whose filepath has been provided as an argument.
    It extracts the timestamp information in order to store it into the time table.
    It extracts the user information in order to store it into the users table.
    It extracts the song playing information in order to store it into the songplay table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
'''
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[(df['page'] == "NextSong")]

    # convert timestamp column to datetime
    t =  df["ts"].map(lambda  x:datetime.datetime.fromtimestamp(x/1000))
    
    # insert time data records
    time_data = pd.Series.dt(t)
    column_labels = {
        "timestamp":df["ts"],
        "hour":time_data.hour,
        "day":time_data.day,
        "week":time_data.week,
        "month":time_data.month,
        "year":time_data.year,
        "weekday":time_data.weekday
    }
    time_df = pd.DataFrame(column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row["ts"], row["userId"], songid, artistid, row["sessionId"], row["location"], row["userAgent"])
        cur.execute(songplay_table_insert, songplay_data)

'''
    This procedure get the name of all files under the argument specified filepath and then passes them to the function specified by the parameter for processing.

    INPUTS: 
    * cur the cursor variable
    * conn the database connection
    * filepath the file path to the song file
    * func the function process the file
'''
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

'''
    This is the main function.
    It connect to database and upload the processed data to the database.
'''
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()