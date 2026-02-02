###### INIT ######
# python module
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Iterable, Optional, Set

# pyspark lib
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# variables
DATABASE_IP = os.getenv("DATABASE_IP")

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
URL = f"jdbc:mysql://{DATABASE_IP}:3306/{MYSQL_DATABASE}\
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
    .config("spark.cassandra.connection.host", DATABASE_IP)
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


###### IO HELPERS ######
def read_data(database: str) -> DataFrame:
    """
    Read data from source database.

    Parameters
    ----------
    database : str
        Data source name: 'cassandra' or 'mysql'

    Returns
    -------
    DataFrame
    """

    db = database.lower()

    if db == "cassandra":
        df = spark.read.format("org.apache.spark.sql.cassandra").load(
            keyspace="recruitment", table="tracking", read_conf={"fetch_size": 10000}
        )
        return df

    elif db == "mysql":
        sql = "(SELECT id AS job_id, company_id, campaign_id, group_id FROM job) as job_sub"
        df = (
            spark.read.format("jdbc")
            .options(
                url=URL,
                driver="com.mysql.cj.jdbc.Driver",
                dbtable=sql,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                batchsize=10000,
            )
            .load()
        )
        return df

    else:
        raise ValueError(
            f"Unsupported database '{database}'. Expected 'cassandra' or 'mysql'."
        )


def write_data(final: DataFrame):
    """
    write data to MySQL.

    Parameters
    ----------
    final: DataFrame

    Returns
    -------
    None
    """

    (
        final.write.format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", URL)
        .option("dbtable", "events")
        .mode("append")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("batchsize", 10000)
        .save()
    )

    logger.info("Data written to MySQL successfully")


###### validation function ######
def schema_check(
    df: DataFrame, required_columns: Iterable[str], stage: str = "unknown"
) -> None:
    """
    Validate that DataFrame contains all required columns.
    Fail fast if any required column is missing.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame
    required_columns : Iterable[str]
        Columns that MUST exist for the pipeline to work
    stage : str
        Pipeline stage name (for logging / debugging)

    Raises
    ------
    ValueError
        If any required column is missing
    """

    actual_cols = set(df.columns)
    required_cols = set(required_columns)

    missing_cols = required_cols - actual_cols

    if missing_cols:
        raise ValueError(
            f"[SCHEMA_CHECK][{stage}] "
            f"Missing required columns: {sorted(missing_cols)}. "
            f"Actual columns: {sorted(actual_cols)}"
        )


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


def compute_event_watermark(
    source_df: DataFrame, ts_col: str = "ts", stage: str = "event_watermark"
) -> datetime:
    """
    Compute max event-time watermark.
    Fail fast if ts is invalid.

    Parameters
    ----------
    source_df: DataFrame
        Input DataFrame
    """

    df = source_df.withColumn("_ts", F.to_timestamp(col(ts_col), "yyyy-MM-dd HH:mm:ss"))

    total = df.count()
    invalid = df.filter(col("_ts").isNull()).count()

    if total == 0:
        raise ValueError(f"[{stage}] source_df is empty")

    if invalid > 0:
        raise ValueError(f"[{stage}] Found {invalid}/{total} invalid event timestamps")

    watermark = df.agg(F.max("_ts")).collect()[0][0]

    if watermark is None:
        raise ValueError(f"[{stage}] watermark is NULL")

    return watermark


def transform_data(df: DataFrame, jobs_df: DataFrame) -> DataFrame:
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
    fact_df = aggregate_metrics_single_pass(base_df)

    logger.info("Enriching Job Dimension")
    fact_df = enrich_job_dimension(fact_df, jobs_df)

    logger.info("Adding metadata")
    watermark = compute_event_watermark(base_df)
    final = add_metadata(fact_df).withColumn("event_watermark", F.lit(watermark))

    return final


###### CONTROL FLOW ######
def control_flow():
    """
    Control flow function. This function is used to manage all the ETL process
    1. Read data from Cassandra, MySQL
    2. Schema checked data
    3. Transform data
    4. Write data to MySQL
    """

    # read data from database
    logger.info("reading data from Cassandra")
    df = read_data("cassandra")
    schema_check(df, SCHEMA, "Cassandra_schema_check")

    logger.info("reading data from MySQL")
    job = read_data("mysql")
    schema_check(
        job, ("job_id", "company_id", "campaign_id", "group_id"), "MySQL_schema_check"
    )
    df.show(10, truncate=False)

    # transfroming data
    logger.info("transforming data...")
    final = transform_data(df, job)
    final.show(10, truncate=False)

    # writing data to database
    logger.info("writing data into MySQL")
    write_data(final)


###### MAIN ######
if __name__ == "__main__":
    logger.info("STARTING ETL PROCESS")
    control_flow()
    logger.info("ETL PROCESS COMPLETED")
