from six.moves import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType as Date, TimestampType as Time

# Through configparser one can define the required AWS key pair variables to read and write in s3.
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """ Initiates spark session within an AWS EMR cluster."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version",
                                                      "2")  # This config allows faster writing to s3.
    return spark


def process_song_data(spark, input_data, output_data):
    """ Extracting and processing song dataset within the EMR cluster to load processed songs
    and artists dimensional tables to s3.

    Keyword arguments:
        spark -- active spark session.
        input_data -- address of s3 bucket where to load raw data (song and log datasets).
        output_data -- address of s3 bucket where facts and dimensional tables will be saved to.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song-data/A/A/*/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates(["song_id"])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs/"), mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              F.col('artist_name').alias("name"), F.col('artist_location').alias('location'),
                              F.col('artist_latitude').alias('latitude'),
                              F.col('artist_longitude').alias("longitude")).dropDuplicates(["artist_id"])

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, "artists/"))


def process_log_data(spark, input_data, output_data):
    """ Extracting and processing log dataset within the EMR cluster to load processed users, time and songplays
    dimensional tables to s3.
    
    Keyword arguments:
        spark -- active spark session.
        input_data -- address of s3 bucket where to load raw data (song and log datasets).
        output_data -- address of s3 bucket where facts and dimensional tables will be saved to. 
   """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log-data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select(F.col('userId').alias("user_id"),
                        F.col('firstName').alias("first_name"), F.col('lastName').alias('last_name'),
                            "gender", "level").dropDuplicates(["user_id"])

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, "users/"))

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(int(x) / 1000), Time())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: datetime.fromtimestamp(int(x) / 1000), Date())
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(F.col("timestamp").alias("start_time"),
                           F.hour(df.timestamp).alias("hour"), F.dayofmonth(df.datetime).alias("day"),
                           F.weekofyear(df.datetime).alias("week"), F.month(df.datetime).alias("month"),
                           F.year(df.datetime).alias("year"),
                           F.date_format(df.datetime, "E").alias("weekday")).dropDuplicates(["start_time"])

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time/"), mode="overwrite", partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs/")).drop('year')

    # extract columns from joined song and log datasets to create songplays table
    on_condition = [df.song == song_df.title, df.length == song_df.duration]
    songplays_table = df.join(song_df, on_condition, 'left')
    # Columns songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, month, year
    songplays_table = songplays_table.join(time_table, 
                                           songplays_table.timestamp == time_table.start_time, 
                                           "left").select(F.monotonically_increasing_id().alias("songplay_id"), 
                                                          F.col("datetime").alias("start_time"), 
                                                          F.col("userId").alias("user_id"), "level", "song_id", 
                                                          "artist_id", F.col("sessionId").alias("session_id"), 
                                                          "location", F.col("userAgent").alias("user_agent"), 
                                                          "month", "year")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays/"),
                                                               mode="overwrite")


def main():
    """ Runs pyspark etl functions once the spark session has been initiated. Closes spark session too."""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity--3eets/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    spark.stop()

if __name__ == "__main__":
    main()