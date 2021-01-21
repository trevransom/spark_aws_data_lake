import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import glob

config = configparser.ConfigParser()
config.read('dl.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

os.environ['AWS_ACCESS_KEY_ID'] = KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET


def create_spark_session():
    """
    - Here we are going initialize the spark session and return it as an object
    """
    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Here we are going load the s3 song files into a spark dataframe
    - We transform it to a new dataframe with specific columns for song and artist tables
    - Then we write it to parquet files in it's own directories
    """

    # get filepath to song data file
    song_data_path = f'{input_data}/song_data/*/*/*/*.json'

    # explicitly define the song_df schema
    song_df_schema = StructType({
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_location', DoubleType(), True),
        StructField('artist_longitude', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('year', IntegerType(), True)
    })

    # read song data file
    songs_data = spark.read.json(song_data_path, schema=song_df_schema)
    songs_data.persist()

    # extract columns to create songs table
    songs_table = songs_data["song_id", "title", "artist_id", "year", "duration"].dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data+'/songs/song_table.parquet', mode='overwrite')

    # extract columns to create artists table
    artists_table = songs_data["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"].dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'/artists/artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    """
    - Here we are going load the s3 data log files into a spark dataframe
    - We transform it to a new dataframe with specific columns for user, time and songplays tables
    - Then we write it to parquet files in it's own directories
    """

    # get filepath to log data file
    log_data_path = f'{input_data}/log_data/*/*/*.json'

    # explicitly define the song_df schema
    log_df_schema = StructType({
        StructField('artist', StringType(), True),
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', IntegerType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', DoubleType(), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('method', StringType(), True),
        StructField('page', StringType(), True),
        StructField('registration', DoubleType(), True),
        StructField('sessionId', IntegerType(), True),
        StructField('song', StringType(), True),
        StructField('status', IntegerType(), True),
        StructField('ts', LongType(), True),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), True)
    })

    # read log data file
    log_df = spark.read.json(log_data_path, schema=log_df_schema)

    log_df = log_df.filter(log_df.userId != '')
    log_df = log_df.withColumn("userId", log_df["userId"].cast(IntegerType()))

    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == "NextSong")

    # extract columns for users table
    users_table = log_df['userId', 'firstName', 'lastName', 'gender', 'level']

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'/users/users_table.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))

    # extract columns to create time table
    time_table = log_df[col('timestamp').alias('start_time'), hour('timestamp').alias('hour'),
                        dayofmonth('timestamp').alias('day'), weekofyear('timestamp').alias('week'),
                        month('timestamp').alias('month'), year('timestamp').alias('year'),
                        date_format('timestamp', 'E').alias('weekday')].drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data+'/time/time_table.parquet')

    # read in song data to use for songplays table
    song_data_path = f'{input_data}/song_data/*/*/*/*.json'

    # explicitly define the song_df schema
    song_df_schema = StructType({
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_location', DoubleType(), True),
        StructField('artist_longitude', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('year', IntegerType(), True)
    })

    # read song data file
    song_df = spark.read.json(song_data_path, schema=song_df_schema)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = log_df.join(song_df, (log_df.artist == song_df.artist_name) & (log_df.song == song_df.title) & (log_df.length == song_df.duration))
    songplays_table = songplays_table.select(col('timestamp').alias('start_time'), col('userId').alias('user_id'),
                                             'level', 'song_id', 'artist_id', col('sessionId').alias('session_id'),
                                             'artist_location', col('userAgent').alias('user_agent'),
                                             month('timestamp').alias('month'), year('timestamp').alias('year')).drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data+'/songplays/songplays_table.parquet')


def main():
    """
    - Here we initialize the spark session and set aws paths for our data
    """
    spark = create_spark_session()

    input_data = "s3a://udacity-dend"
    output_data = "s3a://your-bucket-name"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
