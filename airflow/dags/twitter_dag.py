import sys, os, pathlib
sys.path.insert(0,os.path.abspath(os.path.join(os.path.dirname(__file__),os.path.pardir)))

from airflow.models import DAG
from plugins.operators.twitter_operator import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago

import datetime as dt


ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6),
}
BASE_FOLDER = os.path.join(
    str(pathlib.Path("~/").expanduser()),
    "cursos/airflow_alura/datapipeline/datalake/{stage}/twitter_aluraonline/{partition}"
)
PARTITION_FOLDER = "extract_date={{ ds }}"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"


with DAG(dag_id="twitter_dag", default_args=ARGS, schedule_interval="0 9 * * *", max_active_runs=1) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path=os.path.join(
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "AluraOnline_{{ ds_nodash }}.json"
        ),
        start_time="{{" f"execution_date.strftime('{TIMESTAMP_FORMAT}')" "}}",
        end_time="{{" f"next_execution_date.strftime('{TIMESTAMP_FORMAT}')" "}}"
    )

    twitter_transform = SparkSubmitOperator(
        task_id="twitter_transform_silver_aluraonline",
        application=os.path.join(
            str(pathlib.Path(__file__).parents[2]),
            "datapipeline/spark/transformation.py"
        ),
        name="twitter_transformation",
        application_args=[
            "--src", BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "--dest", BASE_FOLDER.format(stage="silver", partition=""),
            "--process-date", "{{ ds }}"
        ]
    )

    twitter_operator >> twitter_transform
