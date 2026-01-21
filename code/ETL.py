###### INIT ######
# python module
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

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
            keyspace="recruitment", table="tracking", read_conf={"fetch_size": 1000}
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


###### transform function ######
@udf(returnType=StringType())
def timeuuid_to_ts(uuid_str: Optional[str]) -> Optional[str]:
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
    Convert Cassandra TimeUUID to timestamp string
    using Python built-in uuid (UUID v1).
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
    # 1. Filtering
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
        df = df.agg(F.count("*").alias(col_name))

        return df


def merge_metrics(
    click: DataFrame,
    conversion: DataFrame,
    qualified: DataFrame,
    unqualified: DataFrame,
) -> DataFrame:  # join 4 lần -> có thể tối ưu thêm
    join_cols = ["date", "hour", "job_id", "publisher_id", "campaign_id", "group_id"]

    result = (
        click.join(conversion, on=join_cols, how="full")
        .join(qualified, on=join_cols, how="full")
        .join(unqualified, on=join_cols, how="full")
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


def add_metadata(
    df: DataFrame, source_df: DataFrame
) -> DataFrame:  # chưa đọc chưa tối ưu
    """
    Add metadata into dataframe
    """
    try:
        max_ts_row = source_df.agg(F.max("ts")).collect()
        max_ts = (
            max_ts_row[0][0]
            if max_ts_row
            else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    except Exception as e:
        logger.warning("Failed to compute max(ts), fallback to now(): %s", e)
        max_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = (
        df.withColumn("updated_at", F.lit(max_ts))
        .withColumn("sources", F.lit("Cassandra"))
        .withColumnRenamed("date", "dates")
        .withColumnRenamed("hour", "hours")
        .withColumnRenamed("qualified", "qualified_application")
        .withColumnRenamed("unqualified", "disqualified_application")
        .withColumnRenamed("click", "clicks")
    )
    return df


def transform_data(df: DataFrame, jobs_df: DataFrame) -> DataFrame:
    """
    Transform data
    """

    logger.info("Normalizing event time")
    df = normalize_event_time(df)

    logger.info("Building base event")
    base_df = build_base_event(df).cache()

    logger.info("Aggregating metrics")

    click = aggregate_function(base_df, "click")
    conversion = aggregate_function(base_df, "conversion")
    qualified = aggregate_function(base_df, "qualified")
    unqualified = aggregate_function(base_df, "unqualified")

    logger.info("Merging metrics")
    fact_df = merge_metrics(click, conversion, qualified, unqualified)

    logger.info("Enriching Job Dimension")
    fact_df = enrich_job_dimension(fact_df, jobs_df)

    logger.info("Adding metadata")
    final = add_metadata(fact_df, base_df)

    return final


###### CONTROL FLOW ######
def control_flow():
    """abc"""

    # read data from database
    logger.info("reading data from Cassandra")
    df = read_data("cassandra")
    jobs = read_data("mysql")
    df.show(10, truncate=False)

    # transfroming data
    logger.info("transforming data...")
    final = transform_data(df, jobs)
    final.show(10, truncate=False)

    # writing data to database
    logger.info("writing data into MySQL")
    write_data(final)


###### MAIN ######
if __name__ == "__main__":
    logger.info("STARTING ETL PROCESS")
    control_flow()
    logger.info("ETL PROCESS COMPLETED")
