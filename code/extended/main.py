# python module
import logging

# pyspark module
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# project module
# config
from config.spark import get_spark_session
# io
from data_io.readers import read_tracking_incremental, read_jobs_dimension
from data_io.metadata import get_start_watermark, update_watermark
from data_io.writers import write_data
# process
from process.transformations import transform_data, parse_timestamp

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def control_flow(spark: SparkSession):
    """
    Control flow function. This function is used to manage all the ETL process
    1. Read data from Cassandra, MySQL
    2. Schema checked data
    3. Transform data
    4. Write data to MySQL
    """
    # 1. Start Watermark
    logger.info(">>> Checking Watermark")
    start_time = get_start_watermark(spark)

    # 2. Read
    logger.info(f">>> Reading Cassandra > {start_time}")
    raw_df = read_tracking_incremental(spark, start_time)

    if raw_df.rdd.isEmpty():
        logger.info(">>> No new data. Finished.")
        return

    jobs_df = read_jobs_dimension(spark)

    # 3. Transform
    logger.info("Transforming")
    final_df, base_df = transform_data(raw_df, jobs_df)
    final_df.cache()

    row_count = final_df.count()
    if row_count == 0:
        logger.info(">>> 0 rows after transform. Finished.")
        return

    logger.info("Calculating batch statistics...")
    stats = base_df.agg(F.min("ts"), F.max("ts")).collect()[0]

    # 2. Parse Min/Max Timestamps
    batch_min = parse_timestamp(stats[0], default_value=start_time)
    batch_max = parse_timestamp(stats[1], default_value=start_time)

    logger.info(f"Batch Time Range: {batch_min} -> {batch_max}")

    # 4. Write Data
    logger.info(f">>> Writing {row_count} rows")
    write_data(final_df)

    # 5. Write Metadata (Using Spark JDBC)
    logger.info(">>> Updating Metadata")
    update_watermark(spark,"tracking_etl", batch_min, batch_max, row_count)


###### MAIN ######
if __name__ == "__main__":
    logger.info("STARTING ETL PROCESS")
    spark = get_spark_session("Hybrid-ETL-tracking")
    control_flow(spark)
    spark.stop()
    logger.info("ETL PROCESS COMPLETED")