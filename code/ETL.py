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
TAILSCALE_IP = os.getenv("TAILSCALE_IP")

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
URL = f"jdbc:mysql://{TAILSCALE_IP}:3306{MYSQL_DATABASE}\
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
METRIC_COLUMNS = [
    "clicks",
    "conversions",
    "qualifieds",
    "unqualifieds",
    "alives",
    "interview_scheduleds",
]

# logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# spark config
spark = (
    SparkSession.builder.appName("Local-ETL-Test")
    .master("spark://spark-master:7077")
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
        sql = "SELECT id AS job_id, company_id, campaign_id, group_id FROM job"
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
    Schema check - Make it work

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


def validate_custom_track_enum(
    df: DataFrame,
    allowed_values: Set[str],
    stage: str = "custom_track_validation",
) -> None:
    """
    Validate custom_track values.
    Fail fast if any unexpected value is found.
    """

    actual_values = {
        row["custom_track"]
        for row in df.select("custom_track").distinct().collect()
        if row["custom_track"] is not None
    }

    unexpected = actual_values - allowed_values

    if unexpected:
        raise ValueError(
            f"[{stage}] Unexpected custom_track values found: "
            f"{sorted(unexpected)}. "
            f"Allowed values: {sorted(allowed_values)}"
        )


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
    Adds a 'ts' column to the DataFrame by converting 'create_time'.

    This function applies the timeuuid_to_ts (UDF) to the 'create_time' column.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing the 'create_time' column.

    Returns
    -------
    DataFrame
        DataFrame with the new 'ts' column added.
    """
    return df.withColumn("ts", timeuuid_to_ts(col("create_time")))


def build_base_event(df: DataFrame) -> DataFrame:
    """
    Get useful event data

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


def aggregate_function(
    df: DataFrame, filter_condition: str, col_name: Optional[str] = None
) -> DataFrame:  # có thể tối ưu thêm
    """
    aggregate, filtering, rolling data

    Parameters
    ----------
    df : DataFrame

    Filter condition: str

    Column name: str | None

    Returns
    -------
    DataFrame
    """

    df = df.filter(col("custom_track") == filter_condition).groupBy(
        F.to_date("ts").alias("date"),
        F.hour("ts").alias("hour"),
        "job_id",
        "publisher_id",
        "campaign_id",
        "group_id",
    )

    # 2. Xử lý riêng cho trường hợp 'click'
    if filter_condition == "click":
        df = df.agg(
            F.round(F.avg("bid"), 2).alias("bid_set"),
            F.sum("bid").alias("spend_hour"),
            F.count("*").alias("clicks"),
        )

        return df

    # 3. Xử lý cho các trường hợp còn lại (conversion, qualified...)
    else:
        col_name = col_name if col_name else filter_condition
        df = df.agg(F.count("*").alias(col_name + "s"))

        return df


def merge_metrics(
    click: DataFrame,
    conversion: DataFrame,
    qualified: DataFrame,
    unqualified: DataFrame,
    alive: DataFrame,
    interview: DataFrame,
) -> DataFrame:  # join 6 lần -> có thể tối ưu thêm
    join_cols = ["date", "hour", "job_id", "publisher_id", "campaign_id", "group_id"]

    result = (
        click.join(conversion, on=join_cols, how="full")
        .join(qualified, on=join_cols, how="full")
        .join(unqualified, on=join_cols, how="full")
        .join(alive, on=join_cols, how="full")
        .join(interview, on=join_cols, how="full")
    )

    return result


def enrich_job_dimension(
    df: DataFrame, jobs_df: DataFrame
) -> DataFrame:  # chưa đọc chưa tối ưu
    """
    Enrich job dimension
    """
    df = (
        df.join(jobs_df, on="job_id", how="left")
        .drop(jobs_df.campaign_id)
        .drop(jobs_df.group_id)
    )

    return df


def add_metadata(df: DataFrame) -> DataFrame:
    """
    Processing-time metadata (make it work)
    """
    df = df.withColumn("processed_at", F.current_timestamp()).withColumn(
        "source", F.lit("Cassandra")
    )
    return df


def compute_event_watermark(
    source_df: DataFrame, ts_col: str = "ts", stage: str = "event_watermark"
) -> datetime:
    """
    Compute max event-time watermark.
    Fail fast if ts is invalid.
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
    Transform data
    """

    logger.info("Normalizing event time")
    df = normalize_event_time(df)

    logger.info("Validating timestamp")
    validate_event_timestamp(df, 0.00, "timestamp_validation")

    logger.info("Building base event")
    base_df = build_base_event(df).cache()

    logger.info("Validating custom_track enum")
    validate_custom_track_enum(
        base_df,
        allowed_values={
            "click",
            "conversion",
            "qualified",
            "unqualified",
            "alive",
            "interview_scheduled",
        },
        stage="custom_track_validation",
    )

    logger.info("Aggregating metrics")
    click = aggregate_function(base_df, "click")
    conversion = aggregate_function(base_df, "conversion")
    qualified = aggregate_function(base_df, "qualified")
    unqualified = aggregate_function(base_df, "unqualified")
    alive = aggregate_function(base_df, "alive")
    interview = aggregate_function(base_df, "interview_scheduled")

    logger.info("Merging metrics")
    fact_df = merge_metrics(click, conversion, qualified, unqualified, alive, interview)
    fact_df = fact_df.fillna(0, subset=METRIC_COLUMNS)

    logger.info("Enriching Job Dimension")
    fact_df = enrich_job_dimension(fact_df, jobs_df)

    logger.info("Adding metadata")
    watermark = compute_event_watermark(base_df)
    final = add_metadata(fact_df).withColumn("event_watermark", F.lit(watermark))

    return final


###### CONTROL FLOW ######
def control_flow():
    """abc"""

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
