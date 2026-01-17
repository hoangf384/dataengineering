from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("ReadCassandra")
    .config("spark.cassandra.connection.host", "100.124.171.81")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

spark.read.format("org.apache.spark.sql.cassandra").options(
    keyspace="recruitment", table="tracking"
).load().show()

spark.stop()
