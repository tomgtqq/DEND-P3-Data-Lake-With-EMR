import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, monotonically_increasing_id


# Spark local mode
# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    create spark session.
        Parameters:
            Null
               
        Returns:
            spark (Object):  instance of spark
            
    """
    # spark = SparkSession \
    #     .builder \
    #     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    #     .getOrCreate()

    # Spark local mode
    spark = SparkSession.builder.appName("DataLakeLocal").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    process song data to create song and artist dimensional tables 
        Parameters:
            spark (Object):  instance of spark
            input_data (String):   data path  
            output_data (String):   data path   
        Returns:
            Null
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # create a view
    df.createOrReplaceTempView("song_data_view")

    # extract columns to create songs table
    songs_dim_table = spark.sql("""
    SELECT DISTINCT 
        song_id, 
        title, 
        artist_id, 
        duration, 
        year
    FROM 
        song_data_view
    WHERE song_id IS NOT NULL
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_dim_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + "songs_table/")

    # extract columns to create artists table
    artists_dim_table = spark.sql("""
    SELECT DISTINCT 
        artist_id, 
        artist_name, 
        artist_latitude, 
        artist_longitude, 
        artist_location
    FROM song_data_view
    WHERE artist_id IS NOT NULL
    """)
    
    # write artists table to parquet files
    artists_dim_table.write.mode('overwrite').parquet(output_data + "artists_table/")


def process_log_data(spark, input_data, output_data):
    """
    process log data to create users, time dimensional tables and songplays fact table
        Parameters:
            spark (Object):  instance of spark
            input_data (String):  data path  
            output_data (String):  data path   
        Returns:
            Null
    """

    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # Create a view 
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data_view")

    # extract columns for users table    
    users_dim_table =  spark.sql("""
    SELECT DISTINCT
        userId    AS user_id,
        firstName AS first_name,
        lastName  AS last_name,
        gender,
        level
    FROM 
        log_data_view
    WHERE 
        userId IS NOT NULL 
    """)
    
    # write users table to parquet files
    users_dim_table.write.mode('overwrite').parquet(output_data + "users_table/")

    # extract columns to create time table
    time_dim_table = spark.sql("""
    SELECT
        ts             AS start_time,
        hour(ts)       AS hour,
        dayofmonth(ts) AS day,
        weekofyear(ts) AS week,
        month(ts)      AS month,
        year(ts)       AS year,
        dayofweek(ts)  AS weekday
    FROM
        (
            SELECT DISTINCT to_timestamp(ts/1000) AS ts
            FROM log_data_view
            WHERE ts IS NOT NULL 
        )
    """)

    # write time table to parquet files partitioned by year and month
    time_dim_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + "time_table/")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_fact_table = spark.sql("""
    SELECT
        monotonically_increasing_id()           AS songplay_id,
        e.userId                                AS user_id,
        s.song_id                               AS song_id,
        s.artist_id                             AS artist_id,
        to_timestamp(e.ts/1000)                 AS start_time,
        e.sessionId                             AS session_id,
        e.userAgent                             AS user_agent,
        e.level                                 AS level,
        e.location                              AS location,
        month(to_timestamp(e.ts/1000))          AS month,
        year(to_timestamp(e.ts/1000))           AS year
    FROM 
        log_data_view e 
    JOIN
        song_data_view s
    ON
        e.artist=s.artist_name AND e.song=s.title 
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_fact_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + "songplays_table/")


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    # output_data = "s3a://udacity-dend/result"

    # Spark local mode
    input_data = "./data/"
    output_data = "./result/"
    
    process_song_data(spark, input_data, output_data)   
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
