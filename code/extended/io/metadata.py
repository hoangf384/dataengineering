# python module
import logging
from datetime import datetime
# pyspark module
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# project module
from config.settings import MYSQL_PASSWORD, MYSQL_URL, MYSQL_USER

logger = logging.getLogger(__name__)

def get_start_watermark(spark: SparkSession, pipeline_name: str = "tracking_etl") -> datetime:
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


def update_watermark(spark: SparkSession, pipeline_name: str, min_time: datetime, max_time: datetime, count: int, status: str = "SUCCESS"):
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