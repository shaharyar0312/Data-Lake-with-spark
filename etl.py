import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function loads song_data from S3 and processes it by extracting the songs and artist tables
    and then again loaded back to S3

    Parameters:
    spark       : Spark Session
    input_data  : the path of input song_data (in json format) in S3 
    output_data : the path of S3 where output data (in parquet format) is stored

    """
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/TRAAAAK128F9318786.json'

    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_temp")
    # extract columns to create songs table
    songs_table = spark.sql("""select 
    song_id,
    title,
    artist_id,
    year,
    duration from song_data_temp where song_id is not null""")


    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("artist_id","year").parquet(output_data+'song_table/')

    # extract columns to create artists table
    artists_table = spark.sql(""" select
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude from song_data_temp where artist_id is not null""")

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artist_table/")

 
def process_log_data(spark, input_data, output_data):
    """
    Description: This function loads log_data from S3 and processes it by extracting the songs and artist tables
    and then again loaded back to S3

    Parameters:
    spark       : Spark Session
    input_data  : the path of input song_data (in json format) in S3 
    output_data : the path of S3 where output data (in parquet format) is stored

    """
    # get filepath to log data file
    log_data  = input_data + 'log_data/2018/11/2018-11-01-events.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("df.page=='NextSong'")
    df.createOrReplaceTempView("log_data_temp")
    # extract columns for users table    
    artists_table = spark.sql("""select
    distinct userId as user_id,
    firstName first_name,
    lastName as last_name,
    gender,
    level from log_data_temp where user_id is not null""")

    # write users table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"user_table/")



    # extract columns to create time table
    time_table = spark.sql("""
    SELECT 
    A.start_time_sub as start_time,
    hour(A.start_time_sub) as hour,
    dayofmonth(A.start_time_sub) as day,
    weekofyear(A.start_time_sub) as week,
    month(A.start_time_sub) as month,
    year(A.start_time_sub) as year,
    dayofweek(A.start_time_sub) as weekday
    FROM
    (SELECT to_timestamp(timeSt.ts/1000) as start_time_sub
    FROM log_data_temp timeSt
    WHERE timeSt.ts IS NOT NULL
    ) A
    """)

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"time_table/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  spark.sql("""
    SELECT monotonically_increasing_id() as songplay_id,
    to_timestamp(logTemp.ts/1000) as start_time,
    month(to_timestamp(logTemp.ts/1000)) as month,
    year(to_timestamp(logTemp.ts/1000)) as year,
    logTemp.userId as user_id,
    logTemp.level as level,
    songTemp.song_id as song_id,
    songTemp.artist_id as artist_id,
    logTemp.sessionId as session_id,
    logTemp.location as location,
    logTemp.userAgent as user_agent
    FROM log_data_temp logTemp
    JOIN song_data_temp songTemp 
    on logTemp.artist = songTemp.artist_name and logTemp.song = songTemp.title
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"songplays_table/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/dloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
