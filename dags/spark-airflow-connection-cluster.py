# dags/test_spark_dag.py
from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# env
SPARK_IP = getenv("SPARK_IP")
AIRFLOW_TAILNET = getenv("AIRFLOW_TAILNET")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="spark_airflow_connection_cluster",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    submit_job = SparkSubmitOperator(
        task_id="submit_spark_job",
        conn_id="spark_default",
        application="/opt/project/code/test/hello_spark.py",
        verbose=True,
        conf={
            "spark.submit.deployMode": "cluster",
            "spark.driver.memory": "512m",
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
        },
    )
    submit_job
