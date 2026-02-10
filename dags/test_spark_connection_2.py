# dags/test_spark_dag.py
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="test_spark_connection",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    # Task submit job Spark
    submit_job = SparkSubmitOperator(
        task_id="submit_spark_job",
        conn_id="spark_default",  # Trỏ vào connection đã tạo ở Bước 2
        application="/opt/project/code/hello_spark.py",  # Đường dẫn tới file script (lưu ý đường dẫn trong container)
        verbose=True,
        conf={
            "spark.master": "local[*]",  # Chạy mode local
            "spark.driver.memory": "512m",  # Quan trọng: Giới hạn RAM vì máy EC2 của bạn yếu
        },
    )

    submit_job
