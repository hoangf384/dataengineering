from pyspark.sql import SparkSession


def main():
    # Khởi tạo Spark Session
    spark = SparkSession.builder.appName("HelloAirflowSpark").getOrCreate()

    print(">>> KHOI TAO SPARK THANH CONG! <<<")

    # Tạo một DataFrame đơn giản
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    columns = ["Language", "Users"]

    df = spark.createDataFrame(data, columns)

    print(">>> HIEN THI DATAFRAME: <<<")
    df.show()

    # In ra phiên bản Spark
    print(f">>> Spark Version: {spark.version}")

    spark.stop()


if __name__ == "__main__":
    main()
