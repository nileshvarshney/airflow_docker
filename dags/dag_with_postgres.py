from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'nilesh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id= 'dag_postgres_conn_v1',
    start_date=datetime(2022, 4, 1),
    schedule_interval="4 5 * * Tue,Wed,Thu,Fri"
) as dag:
    task1 = PostgresOperator(
        task_id = "create_table",
        # bash_command="Hello Postgres"
        postgres_conn_id='postgres_test',
        sql="""
            CREATE TABLE IF NOT exists accounts (
                user_id   VARCHAR ( 50 ) ,
                username VARCHAR ( 50 ) ,
                password VARCHAR ( 50 ) NOT NULL,
                email VARCHAR ( 255 ) ,
                created_on date       
            )
        """
    )
    task_delete = PostgresOperator(
        task_id = 'delete_data',
        postgres_conn_id='postgres_test',
        sql = "delete from accounts"
    )

    task_insert = PostgresOperator(
        task_id = "populate_data",
        postgres_conn_id="postgres_test",
        sql = """
            insert into accounts values('aa101', 'airflow', 'airflow_password', 
            'airflow@yahoo.com','{{ds}}')
        """
    )
    task1 >> task_delete >> task_insert
