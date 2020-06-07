import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import expr
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.types import IntegerType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')
local = (config["default"]["runlocal"].lower() == "true")
if local:
    os.environ['AWS_ACCESS_KEY_ID'] = config['aws']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['aws']['AWS_SECRET_ACCESS_KEY']


'''
    This function create spark session
'''
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


'''
    This function process song data

    INPUTS: 
    * spark: the spark session
    * input_data: the s3 path prefix of input
    * output_data: the s3 path prefix of ouput
'''
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*.json"
    if local:
        song_data = "data/song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)
    global song_df
    song_df = df

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "year", "artist_id", "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_latitude", "artist_longitude",
                              "artist_location").dropDuplicates(["artist_id"])

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')

    
'''
    This function process log data

    INPUTS: 
    * spark: the spark session
    * input_data: the s3 path prefix of input
    * output_data: the s3 path prefix of ouput
'''
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    if local:
        log_data = "data/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(col("page") == 'NextSong')

    # extract columns for users table
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender",
                                "level").dropDuplicates(["user_id"])

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x, IntegerType())
    df_timestamp = df.withColumn('start_time', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x / 1000), TimestampType())
    df_datetime = df_timestamp.withColumn('datetime', from_unixtime(col('start_time')))

    # extract columns to create time table
    time_table = df_datetime.dropDuplicates(["start_time"]) \
        .select(
        col("start_time"),
        col("datetime"),
        hour(col("datetime")).alias("hour"),
        dayofmonth(col("datetime")).alias("day"),
        weekofyear(col("datetime")).alias("week"),
        month(col("datetime")).alias("month"),
        year(col("datetime")).alias("year"),
        dayofweek(col("datetime")).alias("weekday")
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table/')

    # read in song data to use for songplays table
    global song_df
    song_df = song_df.select("artist_name", "artist_id", "song_id", "title")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df_datetime.select(
        col("start_time"),
        col("userId").alias("user_id"),
        col("song"),
        col("artist"),
        col("sessionId").alias("session_id"),
        col("location"),
        col("userAgent").alias("user_agent"),
        month(col("datetime")).alias("month"),
        year(col("datetime")).alias("year")
    ).join(song_df, (song_df.artist_name == df_timestamp.artist) & (song_df.title == df_timestamp.song)) \
        .select(
        col("start_time"),
        col("user_id"),
        col("song_id"),
        col("artist_id"),
        col("session_id"),
        col("location"),
        col("user_agent"),
        col("month"),
        col("year")
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays_table/')

    
'''
    This function is entrance of the program.
'''
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-eng-project4/"
    if local:
        output_data = "./output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


    
if __name__ == "__main__":
    main()
