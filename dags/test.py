from datetime import  datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'nilesh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id= 'tag_extension',
    start_date=datetime(2022, 4, 15),
    schedule_interval="@daily"
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command='echo dag with cron expression',
    )
    task1