import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_simple_spark_job():
    print("--- BẮT ĐẦU TEST SPARK ---")

    # 1. Import PySpark (Nếu lỗi ở đây là do chưa cài pyspark)
    from pyspark.sql import SparkSession

    # 2. In biến môi trường để check xem Docker nhận đúng chưa
    print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
    print(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")

    # 3. Khởi tạo Spark Session
    spark = (
        SparkSession.builder.appName("Hello_Airflow_Spark")
        .master("local[*]")
        .getOrCreate()
    )

    print(f"--> Spark Version: {spark.version}")

    # 4. Tạo một DataFrame giả để test tính toán
    data = [("Java", 1), ("Python", 2), ("Spark", 3)]
    df = spark.createDataFrame(data, ["Name", "Value"])

    print("--> Hiển thị DataFrame:")
    df.show()

    spark.stop()
    print("--- KẾT THÚC TEST SPARK (THÀNH CÔNG) ---")


# Định nghĩa DAG
with DAG(
    dag_id="00_test_spark_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    test_task = PythonOperator(
        task_id="run_spark_hello_world", python_callable=run_simple_spark_job
    )

    test_task
