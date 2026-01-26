import logging
from datetime import datetime
from logging import getLogger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from config.settings import MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD
from core.utils import get_min_timeuuid_str

logger = getLogger(__name__)


def read_tracking_incremental(spark: SparkSession, start_time: datetime) -> DataFrame:
    """
        Read incremental Tracking table from Cassandra using predicate pushdown.

    parameters
    -----------
    spark: SparkSession

    start_time: datetime
        time to filter create_time > start_time
    Returns
    -------
        DataFrame: Filtered Tracking data.
    """

    # 1. create min UUID (from core/utils.py)
    min_uuid_str = get_min_timeuuid_str(start_time)

    logger.info(f"Predicate Pushdown: Reading create_time > {min_uuid_str}")

    # 2. read and filter data in cassandra source
    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(keyspace="recruitment", table="tracking")
        .load()
        .filter(col("create_time") > min_uuid_str)
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