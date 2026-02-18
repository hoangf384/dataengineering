# dags/gen-data-job-test.py
from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

AIRLOW_IP = getenv("AIRLOW_IP")
DATABASE_IP = getenv("DATABASE_IP")
MYSQL_USER = getenv("MYSQL_USER")

MYSQL_PASSWORD = getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = getenv("MYSQL_DATABASE")
CASSANDRA_KEYSPACE = getenv("CASSANDRA_KEYSPACE")


PROJECT_ROOT = "/opt/project/code"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    # "email": ["hoangphuc030804@gmail.com"],
    # "email_on_failure": True,
}

with DAG(
    dag_id="gen-dummy-data",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    submit_job = SparkSubmitOperator(
        task_id="submit_dummy_data_generator_job",
        conn_id="spark_default",
        application=f"{PROJECT_ROOT}/scripts/dummy-gennerator.py",
        verbose=True,
        conf={
            "spark.submit.deployMode": "client",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": AIRLOW_IP,
            "spark.driver.port": "30000",
            "spark.blockManager.port": "30001",
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
        },
        env_vars={
            "PYTHONPATH": PROJECT_ROOT,
            "DATABASE_IP": DATABASE_IP,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PASSWORD,
            "MYSQL_DATABASE": MYSQL_DATABASE,
            "CASSANDRA_KEYSPACE": CASSANDRA_KEYSPACE,
        },
    )

    submit_job
