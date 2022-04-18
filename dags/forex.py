from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

import csv
import json
import requests


def download_rates():
    BASE_URL = "https://gist.github.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    END_POINTS = {
        "EUR" :"api_forex_exchange_eur.json",
        "USD" :"api_forex_exchange_usd.json"
    }
    with open("/opt/airflow/data/forex_currencies.csv") as f:
        reader = csv.DictReader(f, delimiter=",")
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(" ")
            indata = requests.get(f"{BASE_URL}{END_POINTS}[base]").json()
            outdata = {'base' : base, 'rates':{}, 'last_updated' : indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates']['pair']
            with open ("/opt/airflow/data/forex_rates.json",'aw') as outut_file:
                json.dumps(outdata, outut_file)
                outut_file.write("\n")

defalt_args = {
    "owner" : "airflow",
    "email_on_failure": False,
    "email_on_retry" : False,
    "email" : "admin@localhost.com",
    "retries" : 2,
    "retry_delay" : timedelta(seconds=5)
}

with DAG(
    dag_id = 'forex_data_pipeline', description="Forex Data Pipeline", 
    start_date=datetime(2022,2,22),catchup=False,
    schedule_interval="@daily", default_args=defalt_args) as dag:

    is_forex_rate_available = HttpSensor(
        task_id = 'is_forex_rate_available',
        http_conn_id="forex_api",
        endpoint='marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b',
        response_check=lambda response : "rates" in response.text,
        poke_interval =5,
        timeout = 20
    )

    is_forex_currencies_file_available = FileSensor(
        task_id = 'is_forex_currencies_file_available',
        filepath='forex_currencies.csv',
        fs_conn_id='forex_path',
        recursive=False,
        poke_interval= 5,
        timeout = 20
    )

    download_rates = PythonOperator(
        task_id = 'download_rates',
        python_callable=download_rates
    )

    copy_file_to_hdfs = BashOperator(
        task_id = 'copy_file_to_hdfs',
        # bash_command="""
        #     hdfs dfs -mkdir -p /forex && \
        #     hdfs dfs -put -f /opt/airflow/data/forex_rates.json /forex
        # """
        bash_command="""
            mkdir -p /opt/airflow/data/forex && 
            cp /opt/airflow/data/forex_rates.json /opt/airflow/data/forex/forex_rates.json
        """
    )



