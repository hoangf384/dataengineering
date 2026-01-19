###### INIT ######
# python module
import logging
import os
import uuid

# pyspark lib
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# variables
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
TAILSCALE_IP = os.getenv("TAILSCALE_IP")

# logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# spark config
spark = (
    SparkSession.builder.appName("Local-ETL-Test")
    .master("spark://spark-master:7077")
    .config("spark.driver.memory", "2g")
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
def read_data():
    """READ Data from Cassandra"""
    df = spark.read.format("org.apache.spark.sql.Cassandra").load(
        keyspace="recruitment", table="tracking", read_conf={"fetch_size": 1000}
    )

    return df


def write_data(final: DataFrame):
    """WRITE DATA TO MYSQL"""

    url = (
        f"jdbc:mysql://{TAILSCALE_IP}:3306",
        "?allowPublicKeyRetrieval=true&useSSL=false",
    )

    (
        final.write.format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", url)
        .option("dbtable", "events")
        .mode("append")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("batchsize", 10000)
        .save()
    )

    logger.info("Data written to MySQL successfully")


###### transform function ######


def normalize_event_time(df: DataFrame):
    """
    Convert Cassandra TimeUUID to timestamp string
    """

    @udf(returnType=StringType())
    def timeuuid_to_ts(x):
        return (
            time_uuid.TimeUUID(bytes=UUID(x).bytes)
            .get_datetime()
            .strftime("%Y-%m-%d %H:%M:%S")
        )

    return df.withColumn("ts", timeuuid_to_ts(col("create_time")))


def build_base_event(df: DataFrame) -> DataFrame:
    """
    Select necessary event-level columns
    """

    df = df.select(
        "create_time",
        "job_id",
        ## chatgpt bảo thêm "ts" nữa mà t k biết
        "custom_track",
        "bid",
        "campaign_id",
        "group_id",
        "publisher_id",
    )

    return df


def aggregate_click(df: DataFrame) -> DataFrame:
    """
    Aggregate click data
    """

    #
    df = df.filter(col("custom_track") == "click").groupBy(
        F.to_date("ts").alias("date"),
        F.hour("ts").alias("hour"),
        "job_id",
        "publisher_id",
        "campaign_id",
        "group_id",
    )

    #
    df = df.agg(
        F.round(F.avg("bid"), 2).alias("bid_set"),
        F.sum("bid").alias("spend_hour"),
        F.count("*").alias("click"),
    )

    return df


def aggregate_conversion(df: DataFrame) -> DataFrame:
    """
    Aggregate conversion data
    """
    df = (
        df.filter(col("custom_track") == "conversion")
        .groupBy(
            F.to_date("ts").alias("date"),
            F.hour("ts").alias("hour"),
            "job_id",
            "publisher_id",
            "campaign_id",
            "group_id",
        )
        .agg(F.count("*").alias("conversion"))
    )

    return df


def aggregate_qualified(df: DataFrame) -> DataFrame:
    """
    Aggregate qualified data
    """
    df = (
        df.filter(col("custom_track") == "qualified")
        .groupBy(
            F.to_date("ts").alias("date"),
            F.hour("ts").alias("hour"),
            "job_id",
            "publisher_id",
            "campaign_id",
            "group_id",
        )
        .agg(F.count("*").alias("qualified"))
    )
    return df


def aggregate_unqualified(df: DataFrame) -> DataFrame:
    """
    Aggregate unqualified data
    """

    df = (
        df.filter(col("custom_track") == "unqualified")
        .groupBy(
            F.to_date("ts").alias("date"),
            F.hour("ts").alias("hour"),
            "job_id",
            "publisher_id",
            "campaign_id",
            "group_id",
        )
        .agg(F.count("*").alias("unqualified"))
    )

    return df


def merge_metrics(
    click: DataFrame,
    conversion: DataFrame,
    qualified: DataFrame,
    unqualified: DataFrame,
) -> DataFrame:
    """
    Merge metrics
    """

    # merge conversion
    click = click.join(
        conversion,
        on=["date", "hour", "job_id", "publisher_id", "campaign_id", "group_id"],
        how="full",
    )

    # merge qualified
    click = click.join(
        qualified,
        on=["date", "hour", "job_id", "publisher_id", "campaign_id", "group_id"],
        how="full",
    )

    # merge unqualified
    click = click.join(
        unqualified,
        on=["date", "hour", "job_id", "publisher_id", "campaign_id", "group_id"],
        how="full",
    )
    return click


def enrich_job_dimension(df: DataFrame, jobs_df: DataFrame) -> DataFrame:
    """
    Enrich job dimension
    """
    df = (
        df.join(jobs_df, on="job_id", how="left")
        .drop(jobs_df.campaign_id)
        .drop(jobs_df.group_id)
    )

    return df


def add_metadata(df: DataFrame, source_df: DataFrame) -> DataFrame:
    """
    Add metadata into dataframe
    """
    max_ts = source_df.agg(F.max("ts")).collect()[0][0]

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


def transform_data(df: DataFrame) -> DataFrame:
    """
    Transform data
    """

    logger.info("Normalizing event time")
    df = normalize_event_time(df)

    logger.info("Building base event")
    base_df = build_base_event(df).cache()

    logger.info("Aggregating metrics")
    click = aggregate_click(base_df)
    conversion = aggregate_conversion(base_df)
    qualified = aggregate_qualified(base_df)
    unqualified = aggregate_unqualified(base_df)

    logger.info("Merging metrics")
    fact_df = merge_metrics(click, conversion, qualified, unqualified)

    logger.info("Adding metadata")
    final = add_metadata(fact_df, base_df)

    return final


###### CONTROL FLOW ######
def control_flow():
    """ "abc"""

    # read data from database
    logger.info("reading data from Cassandra")
    df = read_data()
    df.show(10, truncate=False)

    # transfroming data
    logger.info("transforming data...")
    final = transform_data(df)
    final.show(10, truncate=False)

    # writing data to database
    logger.info("writing data into MySQL")
    write_data(final)


###### MAIN ######
if __name__ == "__main__":
    logger.info("STARTING ETL PROCESS")
    control_flow()
    logger.info("ETL PROCESS COMPLETED")
