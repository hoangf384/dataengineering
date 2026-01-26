###### INIT ######
# python module
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Iterable, Optional, Set, Tuple

# pyspark lib
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, IntegerType

# variables
TAILSCALE_IP = os.getenv("TAILSCALE_IP")

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_URL = f"jdbc:mysql://{TAILSCALE_IP}:3306/{MYSQL_DATABASE}\
    ?allowPublicKeyRetrieval=true&useSSL=false"
NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01B21DD213814000
# database variables
SCHEMA = (
    "create_time",
    "job_id",
    "custom_track",
    "bid",
    "campaign_id",
    "group_id",
    "publisher_id",
)

# logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# spark config
spark = (
    SparkSession.builder.appName("Local-ETL-Test")  # type: ignore
    .master("local[*]")
    .config("spark.driver.memory", "1g")
    .config(
        "spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024
    )  # 256 * 1024 * 1024 bytes
    .config(
        "spark.sql.shuffle.partitions", "200"
    )  # 200 partitions for shuffle operations
    .config("spark.cassandra.connection.host", TAILSCALE_IP)
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


###### IO HELPERS ######
def get_min_timeuuid_str(dt: datetime) -> str:
    """
    """

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    # Convert Unix timestamp → 100ns since UUID epoch
    ts_100ns = int(dt.timestamp() * 1e7) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH

    time_low = ts_100ns & 0xffffffff
    time_mid = (ts_100ns >> 32) & 0xffff
    time_hi_version = ((ts_100ns >> 48) & 0x0fff) | 0x1000  # UUID v1

    # clock_seq = 0, node = 0 → MIN uuid for that time
    u = uuid.UUID(fields=(time_low, time_mid, time_hi_version, 0, 0, 0))

    return str(u)


def get_start_watermark(pipeline_name: str = "tracking_etl") -> datetime:
    """
    get start watermark from mydb.event_metadata
    """
    query = f"""
    (SELECT MAX(max_event_time) as last_run 
     FROM event_metadata 
     WHERE pipeline_name = '{pipeline_name}' 
     AND status = 'SUCCESS') as tmp
    """

    try:
        df = spark.read.format("jdbc") \
            .options(
            url=MYSQL_URL,
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=query,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        ).load()

        last_run = df.collect()[0]["last_run"]

        if last_run:
            logger.info(f"Found watermark: {last_run}")
            return last_run
        else:
            logger.info("First run (No watermark). Defaulting to 1970-01-01.")
            return datetime(1970, 1, 1)

    except Exception as e:
        logger.warning(f"Error reading watermark: {e}. Defaulting to full load.") # should we raise error in here
        return datetime(1970, 1, 1)


def update_watermark(pipeline_name: str, min_time: datetime, max_time: datetime, count: int, status: str = "SUCCESS"):
    """

    """

    try:
        # 1. define table schema
        schema = StructType([
            StructField("pipeline_name", StringType(), False),
            StructField("min_event_time", TimestampType(), True),
            StructField("max_event_time", TimestampType(), False),
            StructField("row_count", IntegerType(), True),
            StructField("status", StringType(), True)
        ])

        # 2. create DataFrame
        data = [(pipeline_name, min_time, max_time, count, status)]
        meta_df = spark.createDataFrame(data, schema)
        # 3. write to MySQL
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


def read_tracking_incremental(start_time: datetime) -> DataFrame:
    """Đọc Cassandra có Filter (Pushdown)"""

    # Dùng hàm tự viết để lấy chuỗi UUID
    min_uuid_str = get_min_timeuuid_str(start_time)

    logger.info(f"Predicate Pushdown: Reading create_time > {min_uuid_str}")

    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(keyspace="recruitment", table="tracking")
        .load()
        # So sánh chuỗi UUID vẫn hoạt động tốt với Cassandra connector
        .filter(col("create_time") > min_uuid_str)
    )
    return df


def read_jobs_dimension() -> DataFrame:
    query = "(SELECT id AS job_id, company_id, campaign_id, group_id FROM job) as job_sub"
    return spark.read.format("jdbc") \
        .options(
            url=MYSQL_URL, driver="com.mysql.cj.jdbc.Driver", dbtable=query,
            user=MYSQL_USER, password=MYSQL_PASSWORD
        ).load()


def write_data(final: DataFrame):
    final.write.format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", "events") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .mode("append") \
        .save()
    logger.info("Data written to MySQL events table")

###### validation function ######
def clean_and_filter_custom_track(
        df: DataFrame,
        allowed_values: Set[str],
        stage: str = "custom_track_filter",
) -> DataFrame:
    """
    Clean and filter custom_track column. Drop rows with invalid custom_track values.

    Parameter
    ----------
    df : DataFrame
        Input DataFrame containing the 'custom_track' column.
    allowed_values : Set[str]
        Set of allowed values for the 'custom_track' column.
    stage : str
        Name of the satge where filtering is perormed.

    Returns
    -------
    DataFrame
        DataFrame with only valid custom_track values.
    """

    is_valid = col("custom_track").isin(list(allowed_values))

    total_count = df.count()
    valid_df = df.filter(is_valid)
    valid_count = valid_df.count()
    invalid_count = total_count - valid_count

    if invalid_count > 0:
        logger.warning(
            f"[{stage}] Dropped {invalid_count} rows with invalid custom_track. "
            f"Retaining {valid_count}/{total_count} rows."
        )

    df = df.filter(is_valid)

    return df


def validate_event_timestamp(
    df: DataFrame, ratio: float = 0.0, stage: str = "normalize_event_time"
) -> None:
    """
    Validate ts column after timeuuid normalization.
    Fail fast if invalid timestamp rate exceeds threshold.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing the 'ts' column.
    ratio : float
        Maximum allowed ratio of invalid timestamps.
    stage : str
        Name of the stage where validation is performed.

    Raises
    ------
    ValueError
        If the invalid timestamp rate exceeds the threshold.
    """

    total = df.count()
    invalid = df.filter(col("ts").isNull()).count()

    if total == 0:
        raise ValueError(f"[{stage}] Input DataFrame is empty")

    invalid_ratio = invalid / total

    if invalid_ratio > ratio:
        raise ValueError(
            f"[{stage}] Invalid ts detected: "
            f"{invalid}/{total} rows ({invalid_ratio:.2%})"
        )


###### transform function ######
@udf(returnType=StringType())
def timeuuid_to_ts(uuid_str: Optional[str]) -> Optional[str]:
    """
    User Defined Function (UDF) to convert Cassandra UUID to timestamp string.

    Uses python built-in module (uuid) to extract the timestamp.

    Parameters
    ----------
    uuid_str : str or None
        The UUID string from Cassandra (TimeUUID version 1).

    Returns
    -------
    str or None
        The formatted timestamp string '%Y-%m-%d %H:%M:%S' if valid,
        otherwise None.
    """
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
    """
    Adds a `ts` column to the DataFrame by converting `create_time`.

    This function applies the `timeuuid_to_ts (@UDF)` to the `create_time` column.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing the `create_time` column.

    Returns
    -------
    DataFrame
        DataFrame with the new `ts` column added.
    """
    df = df.withColumn("ts", timeuuid_to_ts(col("create_time")))
    return df


def build_base_event(df: DataFrame) -> DataFrame:
    """
    Get useful events data

    Parameters
    ----------
    df: DataFrame

    Returns
    -------
    DataFrame
    """

    df = df.select(
        "create_time",
        "job_id",
        "ts",  # -> tự thêm chỉ số này
        "custom_track",
        "bid",
        "campaign_id",
        "group_id",
        "publisher_id",
    )

    return df


def aggregate_metrics_single_pass(df: DataFrame) -> DataFrame:
    """
    aggregate metric single pass

    Parameters
    ----------
        df: DataFrame

    Returns
    -------
        DataFrame
    """

    # 1. define conditions
    is_click = col("custom_track") == "click"
    is_conversion = col("custom_track") == "conversion"
    is_qualified = col("custom_track") == "qualified"
    is_unqualified = col("custom_track") == "unqualified"
    # is_alive = col("custom_track") == "alive"
    # is_interview = col("custom_track") == "interview_scheduled"

    # 2. implement grouby and aggregate function together
    df = df.groupBy(
        F.to_date("ts").alias("dates"),
        F.hour("ts").alias("hours"),
        "job_id",
        "publisher_id",
        "campaign_id",
        "group_id",
    ).agg(
        # --- Metrics cho Click (Có tính tiền) ---
        # COUNT(IF(track='click', 1, 0))
        F.sum(F.when(is_click, 1).otherwise(0)).alias("clicks"),
        # AVG(IF(track='click', bid, NULL)) -> NULL để không ảnh hưởng trung bình
        F.round(F.avg(F.when(is_click, col("bid"))), 2).alias("bid_set"),
        # SUM(IF(track='click', bid, 0))
        F.sum(F.when(is_click, col("bid")).otherwise(0)).alias("spend_hour"),
        # --- Metrics cho các loại khác (Chỉ đếm) ---
        F.sum(F.when(is_conversion, 1).otherwise(0)).alias("conversion"),
        F.sum(F.when(is_qualified, 1).otherwise(0)).alias("qualified_application"),
        F.sum(F.when(is_unqualified, 1).otherwise(0)).alias("disqualified_application"),
    )

    return df


def enrich_job_dimension(
    df: DataFrame, jobs_df: DataFrame
) -> DataFrame:  # chưa đọc chưa tối ưu
    """
    Enrich job dimension

    Parameters
    ----------
    df : DataFrame
        Input DataFrame
    jobs_df : DataFrame
        Job dimension DataFrame

    Returns
    -------
    DataFrame
        Enriched DataFrame
    """
    df = (
        df.join(jobs_df, on="job_id", how="left")
        .drop(jobs_df.campaign_id)
        .drop(jobs_df.group_id)
    )

    return df


def add_metadata(df: DataFrame) -> DataFrame:
    """
    add column procesed_at by using `current_timestamp()`

    Parameters
    ----------
    df: DataFrame
        Input DataFrame

    Returns
    -------
    DataFrame

    """
    df = df.withColumn("processed_at", F.current_timestamp()).withColumn(
        "sources", F.lit("Cassandra")
    )
    return df


def transform_data(df: DataFrame, jobs_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Transform data function. This function is used to manage the transformation process of the input DataFrame.

    Parameters
    ----------
    df: DataFrame
        Input DataFrame
    jobs_df: DataFrame
        Input job dimension DataFrame

    Returns
    -------
    DataFrame
        Transformed DataFrame
    """

    logger.info("Normalizing event time")
    df = normalize_event_time(df)

    logger.info("Validating timestamp")
    validate_event_timestamp(df, 0.00, "timestamp_validation")

    logger.info("Building base event")
    base_df = build_base_event(df).cache()

    logger.info("Validating custom_track enum")
    clean_df = clean_and_filter_custom_track(
        base_df,
        allowed_values={
            "click",
            "conversion",
            "qualified",
            "unqualified",
        },
        stage="custom_track_filter",
    )

    logger.info("Aggregating metrics")
    fact_df = aggregate_metrics_single_pass(clean_df)

    logger.info("Enriching Job Dimension")
    fact_df = enrich_job_dimension(fact_df, jobs_df)

    logger.info("Adding metadata")
    final = add_metadata(fact_df)

    return final, clean_df


###### CONTROL FLOW ######
def control_flow():
    """
    Control flow function. This function is used to manage all the ETL process
    1. Read data from Cassandra, MySQL
    2. Schema checked data
    3. Transform data
    4. Write data to MySQL
    """
    # 1. Start Watermark
    logger.info(">>> Checking Watermark")
    start_time = get_start_watermark()

    # 2. Read
    logger.info(f">>> Reading Cassandra > {start_time}")
    raw_df = read_tracking_incremental(start_time)

    if raw_df.rdd.isEmpty():
        logger.info("No new data. Finished.")
        return

    jobs_df = read_jobs_dimension()

    # 3. Transform
    logger.info(">>> Transforming")
    final_df, base_df = transform_data(raw_df, jobs_df)
    final_df.cache()

    row_count = final_df.count()
    if row_count == 0:
        logger.info("0 rows after transform. Finished.")
        return

    # Calculate Max Time
    stats = base_df.withColumn("ts_dt", F.to_timestamp("ts")) \
        .agg(F.min("ts_dt"), F.max("ts_dt")).collect()[0]
    batch_min, batch_max = stats[0], stats[1]

    if not batch_max: batch_max = start_time

    # 4. Write Data
    logger.info(f">>> Writing {row_count} rows")
    write_data(final_df)

    # 5. Write Metadata (Using Spark JDBC)
    logger.info(">>> Updating Metadata")
    update_watermark("tracking_etl", batch_min, batch_max, row_count)


###### MAIN ######
if __name__ == "__main__":
    logger.info("STARTING ETL PROCESS")
    control_flow()
    logger.info("ETL PROCESS COMPLETED")
