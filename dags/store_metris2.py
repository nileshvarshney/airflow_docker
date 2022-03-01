from pathlib import Path
from xml.etree.ElementPath import ops
from airflow import DAG
from datetime import datetime
from airflow.sensors.python import PythonSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import airflow

# This flow work fine if there is more than one file per store with _SUCCESS file

def _wait_for_store_files(store_id):
    store_path = Path("/opt/airflow/data/",store_id)
    data_files = store_path.glob("data-*.csv")
    success_file =  store_path / "_SUCCESS"
    return data_files and success_file.exists()

inner_dag = DAG(dag_id='inner_dag', start_date=airflow.utils.dates.days_ago(3), schedule_interval=None, catchup=False)

main_dag =  DAG(
    dag_id='store_metrics_with_python_sensor',
    description="A workflow for ingesting stores data, demostrating File Sensor",
    start_date = datetime(2022,2,23,2),
    schedule_interval="@daily",
    catchup=False,
    default_args={"depend_on_part": True},
    concurrency=10
)

for store_id in ['a','b','c','d']:
    wait = PythonSensor(
        task_id = f"wait_for_store_{store_id}",
        python_callable=_wait_for_store_files,
        op_args={'store_id' : store_id},
        dag = main_dag
    )
    copy = BashOperator(
        task_id = f"copy_store_{store_id}_data",
        bash_command="sleep 1",
        dag = main_dag
    )
    process = BashOperator(
        task_id = f"process_store_{store_id}_data",
        bash_command="sleep 1",
        dag = main_dag
    )

    create_metrics = TriggerDagRunOperator(
        task_id = f"create_metrics_of_{store_id}",
        trigger_dag_id="inner_dag",
        dag = main_dag
    )

    wait >> copy >> process >> create_metrics

compute_final_metrics = DummyOperator(task_id = "compute_final_metrics", dag=inner_dag)
update_dashboard = DummyOperator(task_id = "update_dashboard", dag=inner_dag)
notify_users = DummyOperator(task_id = "notify_users", dag=inner_dag)


compute_final_metrics >> update_dashboard