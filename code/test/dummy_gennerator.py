import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import os

# environment variables
DATABASE_IP = os.getenv("DATABASE_IP", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "123456")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "data_engineering")
MYSQL_URL = f"jdbc:mysql://{DATABASE_IP}:3306/{MYSQL_DATABASE}?allowPublicKeyRetrieval=true&useSSL=false"
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "recruitment")
NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000

# logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_spark_session():
    return (
        SparkSession.builder.appName("Dummy-Data-Generator")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.cassandra.connection.host", DATABASE_IP)
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )


def get_reference_data(spark: SparkSession) -> Tuple[list, list]:
    """
    get reference data from MySQL via Spark JDBC

    Parameters
    ---------
    spark : SparkSession
        spark session object

    Returns
    -------
    jobs_list : list of Rows
        list of job reference data
    pub_list : list of int
        list of publisher IDs
    """
    logger.info("Loading reference data from MySQL via Spark...")

    # 1.
    jobs_df = spark.read.format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", "(SELECT id as job_id, campaign_id, group_id FROM job) as tmp") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD).load()

    jobs_list = jobs_df.collect()

    # 2. Đọc Publisher
    pub_df = spark.read.format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", "(SELECT distinct(id) as publisher_id FROM master_publisher) as tmp") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD).load()

    pub_list = [row['publisher_id'] for row in pub_df.collect()]

    print(f"Loaded {len(jobs_list)} jobs and {len(pub_list)} publishers.")
    return jobs_list, pub_list


def generate_and_write_batch(spark: SparkSession, n_records: int, jobs_list: list, pub_list: list):
    """
    Generate fake data and write to Cassandra
    """

    data = []
    for _ in range(n_records):
        # 1. Random Logic
        job = random.choice(jobs_list)
        pub_id = random.choice(pub_list)


        bid = random.randint(0, 1)
        custom_track = random.choices(['click', 'conversion', 'qualified', 'unqualified'], weights=(70, 10, 10, 10))[0]

        # --- SỬA 1: Định nghĩa biến 'now' trước ---
        u = uuid.uuid1()
        create_time = str(u)

        ts_timestamp = (u.time - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 1e7
        dt_object = datetime.fromtimestamp(ts_timestamp, tz=timezone.utc)

        ts = dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        event_date = dt_object.strftime("%Y-%m-%d")
        # -----------------------------------------

        group_id = int(job['group_id']) if job['group_id'] else 0

        data.append((
            event_date,
            create_time,
            bid,
            int(job['campaign_id']),
            custom_track,
            group_id,
            int(job['job_id']),
            pub_id,
            ts,
        ))

    # 2. create Spark DataFrame
    schema = StructType([
        StructField("event_date", StringType(), False),
        StructField("create_time", StringType(), False),
        StructField("bid", IntegerType(), True),
        StructField("campaign_id", IntegerType(), True),
        StructField("custom_track", StringType(), True),
        StructField("group_id", IntegerType(), True),
        StructField("job_id", IntegerType(), True),
        StructField("publisher_id", IntegerType(), True),
        StructField("ts", StringType(), True)
    ])

    batch_df = spark.createDataFrame(data, schema)

    # 3. write into Cassandra
    logging.info(f"Writing {n_records} records to Cassandra...")
    (
        batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace=CASSANDRA_KEYSPACE, table="tracking")
        .mode("append")
        .save()
    )

if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    jobs_data, pubs_data = get_reference_data(spark)

    if not jobs_data:
        raise ValueError("CRITICAL: No Jobs found in MySQL. Generator cannot start.")

    logger.info("STARTING GENERATOR")

    n = random.randint(10, 100)
    generate_and_write_batch(spark, n, jobs_data, pubs_data)

    logger.info("GENERATION FINISHED")