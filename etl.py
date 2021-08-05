import configparser
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, types as pst
from pyspark.sql.functions import udf, col, coalesce, lit, broadcast, year, month, dayofmonth, hour, weekofyear, \
    dayofweek


@dataclass
class DataSetConfig:
    bucket_name: str
    song_data_file_pattern: str
    log_data_file_pattern: str


@dataclass
class EtlProcessedDataSet:
    bucket_name: str
    user_data_prefix: str
    artist_data_prefix: str
    time_data_prefix: str
    song_data_prefix: str
    songplay_data_prefix: str


@dataclass
class DataLakeConfig:
    data_set: DataSetConfig
    etl_processed_data_set: EtlProcessedDataSet


def read_config() -> DataLakeConfig:
    """
    Read raw config from 'dl.cfg' file and construct config data object to hold their values.

    Returns:
        data_lake_config (DataLakeConfig): config data object
    """
    config_parser = configparser.ConfigParser()
    config_parser.read('dl.cfg')

    raw_data_set_config = config_parser['DATA_SET']
    data_set_config = DataSetConfig(
        bucket_name=raw_data_set_config.get('BUCKET_NAME'),
        song_data_file_pattern=raw_data_set_config.get('SONG_DATA_FILE_PATTERN'),
        log_data_file_pattern=raw_data_set_config.get('LOG_DATA_FILE_PATTERN')
    )

    raw_etl_processed_data_set_config = config_parser['ETL_PROCESSED_DATA_SET']
    etl_processed_data_set_config = EtlProcessedDataSet(
        bucket_name=raw_etl_processed_data_set_config['BUCKET_NAME'],
        user_data_prefix=raw_etl_processed_data_set_config['USER_DATA_PREFIX'],
        artist_data_prefix=raw_etl_processed_data_set_config['ARTIST_DATA_PREFIX'],
        time_data_prefix=raw_etl_processed_data_set_config['TIME_DATA_PREFIX'],
        song_data_prefix=raw_etl_processed_data_set_config['SONG_DATA_PREFIX'],
        songplay_data_prefix=raw_etl_processed_data_set_config['SONGPLAY_DATA_PREFIX']
    )

    return DataLakeConfig(
        data_set=data_set_config,
        etl_processed_data_set=etl_processed_data_set_config
    )


def create_spark_session() -> SparkSession:
    """
    Create or retrieve current `SparkSession` instance with `hadoop-aws` dependency.

    Returns:
        spark_session (pyspark.sql.SparkSession): newly created or current Spark session
    """
    return SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()


def build_staging_song_data_frame(
        spark_session: SparkSession,
        song_data_set_s3_path: str
) -> DataFrame:
    """
    Read JSON files to construct staging song data frame

    Parameters:
        spark_session (pyspark.sql.SparkSession): -
        song_data_set_s3_path (str): S3 path to staging song data set

    Returns:
        staging_song_df (pyspark.sql.DataFrame): staging song data frame
    """
    song_data_set_schema: pst.StructType = pst.StructType([
        pst.StructField('num_songs', pst.IntegerType()),
        pst.StructField('artist_id', pst.StringType()),
        pst.StructField('artist_latitude', pst.StringType()),
        pst.StructField('artist_longitude', pst.StringType()),
        pst.StructField('artist_location', pst.StringType()),
        pst.StructField('artist_name', pst.StringType()),
        pst.StructField('song_id', pst.StringType()),
        pst.StructField('title', pst.StringType()),
        pst.StructField('duration', pst.DoubleType()),
        pst.StructField('year', pst.IntegerType()),
    ])

    return spark_session.read.json(
        path=song_data_set_s3_path,
        schema=song_data_set_schema
    )


def build_staging_log_data_frame(
        spark_session: SparkSession,
        log_data_set_s3_path: str
) -> DataFrame:
    """
    Read JSON files to construct staging log data frame

    Parameters:
        spark_session (pyspark.sql.SparkSession): -
        log_data_set_s3_path (str): S3 path to staging log data set

    Returns:
        staging_song_df (pyspark.sql.DataFrame): staging log data frame
    """
    log_data_set_schema: pst.StructType = pst.StructType([
        pst.StructField('artist', pst.StringType()),
        pst.StructField('auth', pst.StringType()),
        pst.StructField('firstName', pst.StringType()),
        pst.StructField('gender', pst.StringType()),
        pst.StructField('itemInSession', pst.IntegerType()),
        pst.StructField('lastName', pst.StringType()),
        pst.StructField('length', pst.DoubleType()),
        pst.StructField('level', pst.StringType()),
        pst.StructField('location', pst.StringType()),
        pst.StructField('method', pst.StringType()),
        pst.StructField('page', pst.StringType()),
        pst.StructField('registration', pst.DoubleType()),
        pst.StructField('sessionId', pst.StringType()),
        pst.StructField('song', pst.StringType()),
        pst.StructField('status', pst.IntegerType()),

        # Since `1541121934796` (epoch in milliseconds) is not complaint with `datetime` that requires epoch in seconds,
        # then we treat it as LongType first.
        # ref: https://github.com/apache/spark/blob/701756ac957b517464cecbea3aa0799404c4b159/python/pyspark/sql/types.py#L186
        pst.StructField('ts', pst.LongType()),

        pst.StructField('userAgent', pst.StringType()),
        pst.StructField('userId', pst.StringType())
    ])

    return spark_session.read.json(
        path=log_data_set_s3_path,
        schema=log_data_set_schema
    ).where(col('page') == 'NextSong')


def build_user_data_frame(
        staging_log_df: DataFrame
) -> DataFrame:
    """
    Construct user data frame from staging log data frame

    Parameters:
        staging_log_df (pyspark.sql.DataFrame): staging log data frame

    Returns:
        user_df (pyspark.sql.DataFrame): user data frame
    """
    target_columns = [
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        col('gender'),
        col('level')
    ]
    return staging_log_df \
        .dropDuplicates(['userId']) \
        .where(coalesce(col('userId'), lit('')) != '') \
        .select(*target_columns)


def build_artist_data_frame(
        staging_song_df: DataFrame
) -> DataFrame:
    """
    Construct artist data frame from staging song data frame

    Parameters:
        staging_song_df (pyspark.sql.DataFrame): staging song data frame

    Returns:
        artist_df (pyspark.sql.DataFrame): artist data frame
    """
    target_columns = [
        col('artist_id'),
        col('artist_name').alias('name'),
        col('artist_location').alias('location'),
        col('artist_latitude').alias('latitude'),
        col('artist_longitude').alias('longitude')
    ]
    return staging_song_df \
        .dropDuplicates(['artist_id']) \
        .where(coalesce(col('artist_id'), lit('')) != '') \
        .select(*target_columns)


def cast_epoch_in_milliseconds_to_timestamp(value) -> Optional[datetime]:
    """
    Convert raw value to `datetime.datetime` instance.

    Parameters:
        value: value in particular column and row

    Returns:
        timestamp (datetime.datetime): -
    """
    if type(value) == int or type(value) == float:
        return datetime.fromtimestamp(value / 1000)
    else:
        return None


cast_epoch_in_milliseconds_to_timestamp_udf = udf(
    f=cast_epoch_in_milliseconds_to_timestamp,
    returnType=pst.TimestampType()
)


def build_time_data_frame(
        staging_log_df: DataFrame
) -> DataFrame:
    """
    Construct time data frame from staging log data frame

    Parameters:
        staging_log_df (pyspark.sql.DataFrame): staging log data frame

    Returns:
        time_df (pyspark.sql.DataFrame): time data frame
    """
    target_columns = [
        col('start_time'),
        hour(col('start_time')).alias('hour'),
        dayofmonth(col('start_time')).alias('day'),
        weekofyear(col('start_time')).alias('week'),
        month(col('start_time')).alias('month'),
        year(col('start_time')).alias('year'),
        dayofweek(col('start_time')).alias('weekday')
    ]
    return staging_log_df \
        .dropDuplicates(['ts']) \
        .where(col('ts').isNotNull()) \
        .select(cast_epoch_in_milliseconds_to_timestamp_udf(col('ts')).alias('start_time')) \
        .where(col('start_time').isNotNull()) \
        .select(*target_columns)


def build_song_data_frame(
        staging_song_df: DataFrame
) -> DataFrame:
    """
    Construct song data frame from staging song data frame

    Parameters:
        staging_song_df (pyspark.sql.DataFrame): staging song data frame

    Returns:
        song_df (pyspark.sql.DataFrame): song data frame
    """
    target_columns = [
        col('song_id'),
        col('title'),
        col('artist_id'),
        col('year'),
        col('duration')
    ]
    return staging_song_df \
        .dropDuplicates(['song_id']) \
        .where(coalesce(col('song_id'), lit('')) != '') \
        .where(col('year').isNotNull()) \
        .where(coalesce(col('artist_id'), lit('')) != '') \
        .select(*target_columns) \
        .repartition(col('year'))


def build_songplay_data_frame(
        staging_log_df: DataFrame,
        song_df: DataFrame,
        artist_df: DataFrame
) -> DataFrame:
    """
    Construct songplay data frame from staging log, song, and artist data frames

    Parameters:
        staging_log_df (pyspark.sql.DataFrame): staging log data frame
        song_df (pyspark.sql.DataFrame): song data frame
        artist_df (pyspark.sql.DataFrame): artist data frame

    Returns:
        songplay_df (pyspark.sql.DataFrame): songplay data frame
    """
    repartitioned_staging_log_df = staging_log_df \
        .withColumn('start_time', cast_epoch_in_milliseconds_to_timestamp_udf(staging_log_df['ts'])) \
        .withColumn('year', year(col('start_time'))) \
        .withColumn('month', month(col('start_time'))) \
        .repartition(col('year'), col('month'))
    # prevent ambiguous column of `artist_id` on `song_df` and `artist_df`
    broad_casted_song_df = broadcast(song_df).drop(col('artist_id'))
    broad_casted_artist_df = broadcast(artist_df)
    song_and_staging_log_conditions = [
        broad_casted_song_df['title'] == repartitioned_staging_log_df['song'],
        broad_casted_song_df['duration'] == repartitioned_staging_log_df['length']
    ]
    artist_and_staging_log_conditions = [
        broad_casted_artist_df['name'] == repartitioned_staging_log_df['artist']
    ]
    joined_df = repartitioned_staging_log_df \
        .join(broad_casted_song_df, on=song_and_staging_log_conditions) \
        .join(broad_casted_artist_df, on=artist_and_staging_log_conditions)

    target_columns = [
        repartitioned_staging_log_df['start_time'],
        repartitioned_staging_log_df['year'],
        repartitioned_staging_log_df['month'],
        repartitioned_staging_log_df['userId'].alias('user_id'),
        repartitioned_staging_log_df['level'],
        broad_casted_song_df['song_id'],
        broad_casted_artist_df['artist_id'],
        repartitioned_staging_log_df['sessionId'].alias('session_id'),
        repartitioned_staging_log_df['location'],
        repartitioned_staging_log_df['userAgent'].alias('user_agent')
    ]

    return joined_df.select(*target_columns)


s3a_resource_format = 's3a://{bucket_name}/{prefix}'


def write_user_data_frame_to_parquet(
        user_df: DataFrame,
        data_lake_config: DataLakeConfig
) -> None:
    """
    Write user data frame as Parquet files

    Parameters:
        user_df (pyspark.sql.DataFrame): user data frame
        data_lake_config (DataLakeConfig): -

    Returns:
        None
    """
    user_df.write.parquet(
        path=s3a_resource_format.format(
            bucket_name=data_lake_config.etl_processed_data_set.bucket_name,
            prefix=data_lake_config.etl_processed_data_set.user_data_prefix
        ),
        mode='overwrite'
    )


def write_artist_data_frame_to_parquet(
        artist_df: DataFrame,
        data_lake_config: DataLakeConfig
) -> None:
    """
    Write artist data frame as Parquet files

    Parameters:
        artist_df (pyspark.sql.DataFrame): artist data frame
        data_lake_config (DataLakeConfig): -

    Returns:
        None
    """
    artist_df.write.parquet(
        path=s3a_resource_format.format(
            bucket_name=data_lake_config.etl_processed_data_set.bucket_name,
            prefix=data_lake_config.etl_processed_data_set.artist_data_prefix
        ),
        mode='overwrite'
    )


def write_time_data_frame_to_parquet(
        time_df: DataFrame,
        data_lake_config: DataLakeConfig
) -> None:
    """
    Write time data frame as Parquet files

    Parameters:
        time_df (pyspark.sql.DataFrame): time data frame
        data_lake_config (DataLakeConfig): -

    Returns:
        None
    """
    time_df.write.parquet(
        path=s3a_resource_format.format(
            bucket_name=data_lake_config.etl_processed_data_set.bucket_name,
            prefix=data_lake_config.etl_processed_data_set.time_data_prefix
        ),
        mode='overwrite',
        partitionBy=['year', 'month']
    )


def write_song_data_frame_to_parquet(
        song_df: DataFrame,
        data_lake_config: DataLakeConfig
) -> None:
    """
    Write song data frame as Parquet files

    Parameters:
        song_df (pyspark.sql.DataFrame): song data frame
        data_lake_config (DataLakeConfig): -

    Returns:
        None
    """
    song_df.write.parquet(
        path=s3a_resource_format.format(
            bucket_name=data_lake_config.etl_processed_data_set.bucket_name,
            prefix=data_lake_config.etl_processed_data_set.song_data_prefix
        ),
        mode='overwrite',
        partitionBy=['year', 'artist_id']
    )


def write_songplay_data_frame_to_parquet(
        songplay_df: DataFrame,
        data_lake_config: DataLakeConfig
) -> None:
    """
    Write songplay data frame as Parquet files

    Parameters:
        songplay_df (pyspark.sql.DataFrame): songplay data frame
        data_lake_config (DataLakeConfig): -

    Returns:
        None
    """
    songplay_df.write.parquet(
        path=s3a_resource_format.format(
            bucket_name=data_lake_config.etl_processed_data_set.bucket_name,
            prefix=data_lake_config.etl_processed_data_set.songplay_data_prefix
        ),
        mode='overwrite',
        partitionBy=['year', 'month']
    )


def main() -> None:
    """
    1. load data set to Spark staging data frames from S3
    2. transform staging data frames to dimensional-table-like data frames
    3. save dimensional-table-like data frames to S3 as parquet files
        - song data frame is partitioned by year and artist respectively
        - time data frame is partitioned by year and month respectively
        - songplay data frame is partitioned by year and month respectively
    """

    data_lake_config: DataLakeConfig = read_config()

    song_data_set_s3_path = s3a_resource_format.format(
        bucket_name=data_lake_config.data_set.bucket_name,
        prefix=data_lake_config.data_set.song_data_file_pattern
    )
    log_data_set_s3_path = s3a_resource_format.format(
        bucket_name=data_lake_config.data_set.bucket_name,
        prefix=data_lake_config.data_set.log_data_file_pattern
    )

    with create_spark_session() as spark_session:
        # 1. load data set to Spark staging data frames from S3
        staging_song_df = build_staging_song_data_frame(spark_session, song_data_set_s3_path)
        staging_log_df = build_staging_log_data_frame(spark_session, log_data_set_s3_path)

        # 2. transform staging data frames to dimensional-table-like data frames
        user_df = build_user_data_frame(staging_log_df=staging_log_df)
        artist_df = build_artist_data_frame(staging_song_df=staging_song_df)
        time_df = build_time_data_frame(staging_log_df=staging_log_df)
        song_df = build_song_data_frame(staging_song_df=staging_song_df)
        songplay_df = build_songplay_data_frame(
            staging_log_df=staging_log_df,
            song_df=song_df,
            artist_df=artist_df
        )

        # 3. save dimensional-table-like data frames to S3 as parquet files
        write_user_data_frame_to_parquet(user_df, data_lake_config)
        write_artist_data_frame_to_parquet(artist_df, data_lake_config)
        write_time_data_frame_to_parquet(time_df, data_lake_config)
        write_song_data_frame_to_parquet(song_df, data_lake_config)
        write_songplay_data_frame_to_parquet(songplay_df, data_lake_config)


if __name__ == '__main__':
    main()
