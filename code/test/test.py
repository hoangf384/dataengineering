from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.appName("CSV_to_Cassandra")
        .config("spark.cassandra.connection.host", "100.124.171.81")
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )

    # Đọc CSV
    csv_path = (
        "/home/hp/Dataengineering/data/cassandra/Cassandra/tracking_with_event_date.csv"
    )

    df = spark.read.option("header", True).csv(csv_path)

    # Ghi vào Cassandra
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .options(keyspace="recruitment", table="tracking")
        .mode("append")
        .save()
    )

    print("CSV successfully written to Cassandra")

    spark.stop()


if __name__ == "__main__":
    main()
