from airflow import DAG
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

# This flow work fine if there is only one file per store

with DAG(
    dag_id='store_metrics_with_filesensor',
    description="A workflow for ingesting stores data, demostrating File Sensor",
    start_date = datetime(2022,2,23,2),
    schedule_interval="@daily",
    catchup=False,
    default_args={"depend_on_part": True},
    concurrency=10
) as dag:
    create_aggregation = DummyOperator(task_id = "create_aggregation")
    for store_id in ['a','b','c','d']:
        wait = FileSensor(task_id = f"wait_for_store_{store_id}",
        filepath= f"/opt/airflow/data/store_{store_id}",
        )
        copy = BashOperator(
            task_id = f"copy_store_data_{store_id}",
            bash_command="sleep 1")
        process = BashOperator(
            task_id = f"process_store_data_{store_id}",
            bash_command="sleep 1")
        wait >> copy >> process >> create_aggregation
