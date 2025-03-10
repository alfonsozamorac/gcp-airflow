import datetime
import os

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.getenv("PROJECT")
REGION = os.getenv("REGION")
BUCKET = os.getenv("BUCKET")
PYTHON_FILE_LOCATION = f"gs://{BUCKET}/batch.py"
SPARK_BIGQUERY_JAR_FILE = "gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.0.jar"

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "region": REGION,
    "bucket": BUCKET,
}

with models.DAG(
    "dataproc_batch_demo",  # The id you will see in the DAG airflow page
    default_args=default_args,  # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:
    create_batch_1 = DataprocCreateBatchOperator(
        task_id="batch-create-1",
        batch={
            "runtime_config": {
                "version": "2.2"
            },
            "pyspark_batch": {
                "main_python_file_uri": PYTHON_FILE_LOCATION,
                "jar_file_uris": [SPARK_BIGQUERY_JAR_FILE],
                "args": [
                    "--batch_id", "1",
                    "--bucket", BUCKET,
                ],
            },
        },
        batch_id="batch-create-1",
    )

    create_batch_2 = DataprocCreateBatchOperator(
        task_id="batch-create-2",
        batch={
            "runtime_config": {
                "version": "2.2"
            },
            "pyspark_batch": {
                "main_python_file_uri": PYTHON_FILE_LOCATION,
                "jar_file_uris": [SPARK_BIGQUERY_JAR_FILE],
                "args": [
                    "--batch_id", "2",
                    "--bucket", BUCKET,
                ],
            },
        },
        batch_id="batch-create-2",
    )

    create_batch_3 = DataprocCreateBatchOperator(
        task_id="batch-create-3",
        batch={
            "runtime_config": {
                "version": "2.2"
            },
            "pyspark_batch": {
                "main_python_file_uri": PYTHON_FILE_LOCATION,
                "jar_file_uris": [SPARK_BIGQUERY_JAR_FILE],
                "args": [
                    "--batch_id", "3",
                    "--bucket", BUCKET,
                ],
            },
        },
        batch_id="batch-create-3",
    )

    create_batch_4 = DataprocCreateBatchOperator(
        task_id="batch-create-4",
        batch={
            "runtime_config": {
                "version": "2.2"
            },
            "pyspark_batch": {
                "main_python_file_uri": PYTHON_FILE_LOCATION,
                "jar_file_uris": [SPARK_BIGQUERY_JAR_FILE],
                "args": [
                    "--batch_id", "4",
                    "--bucket", BUCKET,
                ],
            },
        },
        batch_id="batch-create-4",
    )

    delete_batch_1 = DataprocDeleteBatchOperator(
        task_id="delete-batch-1",
        batch_id="batch-create-1",
    )

    delete_batch_2 = DataprocDeleteBatchOperator(
        task_id="delete-batch-2",
        batch_id="batch-create-2",
    )

    delete_batch_3 = DataprocDeleteBatchOperator(
        task_id="delete-batch-3",
        batch_id="batch-create-3",
    )

    delete_batch_4 = DataprocDeleteBatchOperator(
        task_id="delete-batch-4",
        batch_id="batch-create-4",
    )

    create_batch_1 >> [create_batch_2, create_batch_3, delete_batch_1]
    create_batch_2 >> [delete_batch_2, create_batch_4]
    create_batch_3 >> [create_batch_4, delete_batch_3]
    create_batch_4 >> delete_batch_4