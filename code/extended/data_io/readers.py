import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from config.settings import MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD, CASSANDRA_KEYSPACE
from core.utils import get_min_timeuuid_str

logger = logging.getLogger(__name__)


def read_tracking_incremental(spark: SparkSession, start_time: datetime) -> DataFrame:
    """
    Read incremental Tracking table from Cassandra.
    """

    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    logger.info(f"Filtering data with ts > '{start_time_str}'")

    # 2. Đọc và Lọc
    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(keyspace=CASSANDRA_KEYSPACE, table="tracking")
        .load()
        .filter(col("ts") > start_time_str)
    )

    return df


def read_jobs_dimension(spark: SparkSession) -> DataFrame:
    """
    Read Jobs dimension data from MySQL.

    parameter
    -----------
        spark: SparkSession

    Returns:
        DataFrame: Jobs dimension data
    """
    query = "(SELECT id AS job_id, company_id, campaign_id, group_id FROM job) as job_sub"

    logger.info("Reading Jobs dimension from MySQL...")

    df = (
        spark.read.format("jdbc")
        .options(
            url=MYSQL_URL,
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=query,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        ).load()
    )

    return df