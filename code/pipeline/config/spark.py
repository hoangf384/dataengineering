# --- SPARK FACTORY ---
from pyspark.sql import SparkSession

from config.settings import DATABASE_IP


def get_spark_session(app_name: str = "Hybrid-ETL-test") -> SparkSession:
    """
    Get and create spark session

    parameter
    ----------
    app_name : str
        Name of Spark Application
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)
        .config("spark.sql.shuffle.partitions", "200")
    )

    if DATABASE_IP:
        builder = builder.config("spark.cassandra.connection.host", DATABASE_IP).config(
            "spark.cassandra.connection.port", "9042"
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark
