###### INIT ######
# python module
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Set, Tuple

# pyspark lib
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, IntegerType

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# constants
NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01B21DD213814000

# env
DATABASE_IP = os.getenv("DATABASE_IP")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_URL = (
    f"jdbc:mysql://{DATABASE_IP}:3306/{MYSQL_DATABASE}"
    "?allowPublicKeyRetrieval=true&useSSL=false"
)


###### SPARK SESSION ######
def get_spark_session(app_name: str = "Local-ETL-Test") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.cassandra.connection.host", DATABASE_IP)
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


###### IO ######
def get_min_timeuuid_str(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    ts_100ns = int(dt.timestamp() * 1e7) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH
    time_low = ts_100ns & 0xffffffff
    time_mid = (ts_100ns >> 32) & 0xffff
    time_hi_version = ((ts_100ns >> 48) & 0x0fff) | 0x1000
    u = uuid.UUID(fields=(time_low, time_mid, time_hi_version, 0, 0, 0))
    return str(u)


def get_start_watermark(spark: SparkSession, pipeline_name: str = "tracking_etl") -> datetime:
    query = f"""
    (SELECT MAX(max_event_time) as last_run
     FROM event_metadata
     WHERE pipeline_name = '{pipeline_name}'
     AND status = 'SUCCESS') as tmp
    """
    try:
        df = (
            spark.read.format("jdbc")
            .options(url=MYSQL_URL, driver="com.mysql.cj.jdbc.Driver", dbtable=query,
                     user=MYSQL_USER, password=MYSQL_PASSWORD)
            .load()
        )
        last_run = df.collect()[0]["last_run"]
        if last_run:
            logger.info(f"Found watermark: {last_run}")
            return last_run
        logger.info("First run (No watermark). Defaulting to 1970-01-01.")
        return datetime(1970, 1, 1)
    except Exception as e:
        logger.warning(f"Error reading watermark: {e}. Defaulting to full load.")
        return datetime(1970, 1, 1)


def update_watermark(
    spark: SparkSession,
    pipeline_name: str,
    min_time: datetime,
    max_time: datetime,
    count: int,
    status: str = "SUCCESS",
):
    schema = StructType([
        StructField("pipeline_name", StringType(), False),
        StructField("min_event_time", TimestampType(), True),
        StructField("max_event_time", TimestampType(), False),
        StructField("row_count", IntegerType(), True),
        StructField("status", StringType(), True),
    ])
    try:
        meta_df = spark.createDataFrame([(pipeline_name, min_time, max_time, count, status)], schema)
        (
            meta_df.write.format("jdbc")
            .option("url", MYSQL_URL)
            .option("dbtable", "event_metadata")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .mode("append")
            .save()
        )
        logger.info(f"Updated watermark successfully: {max_time}")
    except Exception as e:
        logger.error(f"Failed to update watermark: {e}")


def read_tracking_incremental(spark: SparkSession, start_time: datetime) -> DataFrame:
    min_uuid_str = get_min_timeuuid_str(start_time)
    logger.info(f"Predicate Pushdown: Reading create_time > {min_uuid_str}")
    return (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(keyspace="recruitment", table="tracking")
        .load()
        .filter(col("create_time") > min_uuid_str)
    )


def read_jobs_dimension(spark: SparkSession) -> DataFrame:
    query = "(SELECT id AS job_id, company_id, campaign_id, group_id FROM job) as job_sub"
    return (
        spark.read.format("jdbc")
        .options(url=MYSQL_URL, driver="com.mysql.cj.jdbc.Driver", dbtable=query,
                 user=MYSQL_USER, password=MYSQL_PASSWORD)
        .load()
    )


def write_data(final: DataFrame):
    (
        final.write.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", "events")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .mode("append")
        .save()
    )
    logger.info("Data written to MySQL events table")


###### TRANSFORM ######
@udf(returnType=StringType())
def timeuuid_to_ts(uuid_str: Optional[str]) -> Optional[str]:
    """Convert Cassandra TimeUUID (v1) to timestamp string '%Y-%m-%d %H:%M:%S'."""
    if not uuid_str:
        return None
    try:
        u = uuid.UUID(uuid_str)
        if u.version != 1:
            return None
        ts = (u.time - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 1e7
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as v:
        logger.error("Failed to parse timeuuid %s: %s", uuid_str, v)
        return None
    except Exception as e:
        logger.warning("Failed to parse timeuuid %s: %s", uuid_str, e)
        return None


def normalize_event_time(df: DataFrame) -> DataFrame:
    return df.withColumn("ts", timeuuid_to_ts(col("create_time")))


def validate_event_timestamp(df: DataFrame, ratio: float = 0.0, stage: str = "normalize_event_time") -> None:
    total = df.count()
    invalid = df.filter(col("ts").isNull()).count()
    if total == 0:
        raise ValueError(f"[{stage}] Input DataFrame is empty")
    invalid_ratio = invalid / total
    if invalid_ratio > ratio:
        raise ValueError(
            f"[{stage}] Invalid ts detected: {invalid}/{total} rows ({invalid_ratio:.2%})"
        )


def build_base_event(df: DataFrame) -> DataFrame:
    return df.select("create_time", "job_id", "ts", "custom_track", "bid", "campaign_id", "group_id", "publisher_id")


def clean_and_filter_custom_track(df: DataFrame, allowed_values: Set[str], stage: str = "custom_track_filter") -> DataFrame:
    is_valid = col("custom_track").isin(list(allowed_values))
    total_count = df.count()
    valid_df = df.filter(is_valid)
    invalid_count = total_count - valid_df.count()
    if invalid_count > 0:
        logger.warning(f"[{stage}] Dropped {invalid_count} rows with invalid custom_track.")
    return valid_df


def aggregate_metrics(df: DataFrame) -> DataFrame:
    is_click = col("custom_track") == "click"
    is_conversion = col("custom_track") == "conversion"
    is_qualified = col("custom_track") == "qualified"
    is_unqualified = col("custom_track") == "unqualified"
    return df.groupBy(
        F.to_date("ts").alias("dates"),
        F.hour("ts").alias("hours"),
        "job_id", "publisher_id", "campaign_id", "group_id",
    ).agg(
        F.sum(F.when(is_click, 1).otherwise(0)).alias("clicks"),
        F.round(F.avg(F.when(is_click, col("bid"))), 2).alias("bid_set"),
        F.sum(F.when(is_click, col("bid")).otherwise(0)).alias("spend_hour"),
        F.sum(F.when(is_conversion, 1).otherwise(0)).alias("conversion"),
        F.sum(F.when(is_qualified, 1).otherwise(0)).alias("qualified_application"),
        F.sum(F.when(is_unqualified, 1).otherwise(0)).alias("disqualified_application"),
    )


def enrich_job_dimension(df: DataFrame, jobs_df: DataFrame) -> DataFrame:
    return (
        df.join(jobs_df, on="job_id", how="left")
        .drop(jobs_df.campaign_id)
        .drop(jobs_df.group_id)
    )


def add_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("processed_at", F.current_timestamp()).withColumn("sources", F.lit("Cassandra"))


def transform_data(df: DataFrame, jobs_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    logger.info("Normalizing event time")
    df = normalize_event_time(df)

    logger.info("Validating timestamp")
    validate_event_timestamp(df, 0.00, "timestamp_validation")

    logger.info("Building base event")
    base_df = build_base_event(df).cache()

    logger.info("Validating custom_track enum")
    clean_df = clean_and_filter_custom_track(
        base_df,
        allowed_values={"click", "conversion", "qualified", "unqualified"},
    )

    logger.info("Aggregating metrics")
    fact_df = aggregate_metrics(clean_df)

    logger.info("Enriching Job Dimension")
    fact_df = enrich_job_dimension(fact_df, jobs_df)

    logger.info("Adding metadata")
    final = add_metadata(fact_df)

    return final, clean_df


###### CONTROL FLOW ######
def control_flow(spark: SparkSession):
    """
    Control flow function. This function is used to manage all the ETL process
    1. Read data from Cassandra, MySQL
    2. Schema checked data
    3. Transform data
    4. Write data to MySQL
    """
    logger.info(">>> Checking Watermark")
    start_time = get_start_watermark(spark)

    logger.info(f">>> Reading Cassandra > {start_time}")
    raw_df = read_tracking_incremental(spark, start_time)

    if raw_df.rdd.isEmpty():
        logger.info(">>> No new data. Finished.")
        return

    jobs_df = read_jobs_dimension(spark)

    logger.info("Transforming")
    final_df, base_df = transform_data(raw_df, jobs_df)
    final_df.cache()

    row_count = final_df.count()
    if row_count == 0:
        logger.info(">>> 0 rows after transform. Finished.")
        return

    logger.info("Calculating batch statistics...")
    stats = base_df.withColumn("ts_dt", F.to_timestamp("ts")).agg(F.min("ts_dt"), F.max("ts_dt")).collect()[0]
    batch_min, batch_max = stats[0], stats[1]
    if not batch_max:
        batch_max = start_time

    logger.info(f"Batch Time Range: {batch_min} -> {batch_max}")

    logger.info(f">>> Writing {row_count} rows")
    write_data(final_df)

    logger.info(">>> Updating Metadata")
    update_watermark(spark, "tracking_etl", batch_min, batch_max, row_count)


###### MAIN ######
if __name__ == "__main__":
    logger.info("STARTING ETL PROCESS")
    spark = get_spark_session("Local-ETL-Test")
    control_flow(spark)
    spark.stop()
    logger.info("ETL PROCESS COMPLETED")
