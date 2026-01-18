###### INIT ######
# python module
import logging
import os
import uuid

# pyspark lib
from pyspark.sql import SparkSession
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
def read_data(df: pyspark.sql.DataFrame):
    """READ Data from Cassandra"""
    df = spark.read.format("org.apache.spark.sql.Cassandra").load(
        keyspace="recruitment", table="tracking", read_conf={"fetch_size": 1000}
    )

    return df


def write_data(final: pyspark.sql.DataFrame):
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
