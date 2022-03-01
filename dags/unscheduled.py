from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
 
def _convert_to_json(input_path,output_path):
    data = pd.read_csv(input_path)
    data.to_json(output_path)


with DAG(
    dag_id='scheduled',
    start_date= datetime(2022,2,22,13,5,0),
    end_date=datetime(2022,2,25),
    schedule_interval="@daily",
    catchup=False
) as dag:
    convert_csv_to_json = PythonOperator(
        task_id = 'convert_csv_to_json',
        python_callable=_convert_to_json,
        op_kwargs={
            "input_path" : '/opt/airflow/data/prod.csv',
            "output_path": "/opt/airflow/data/prod.json"
        }
    )

convert_csv_to_json