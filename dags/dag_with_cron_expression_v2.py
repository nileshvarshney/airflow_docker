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
    dag_id= 'dag_with_cron_expression_v2',
    start_date=datetime(2022, 4, 1),
    schedule_interval="0 0 * * *"
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command='echo dag with cron expression',
    )
    task1