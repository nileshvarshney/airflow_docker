from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _fetch_pageviews(pagenames, execution_date):
    result = dict.fromkeys(pagenames,0)
    with open("/opt/airflow/data/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_name, view_count,_ = line.split(" ")
            if domain_code == "en" and page_name in pagenames:
                result[page_name] = view_count

    with open("/opt/airflow/data/ins_wikipageviews.sql", "w") as f:
        for pagename, viewcount in result.items():
            f.write(
                "INSERT INTO wikipageviews VALUES ("
                f"'{pagename}', {viewcount}, '{execution_date}');\n"
                )

with DAG(dag_id = 'stocksense', start_date = datetime(2022,2,23,2), schedule_interval="@daily", catchup=False) as dag:
    get_data = BashOperator(
        task_id = 'get_data',
        # dowbload file ex path https://dumps.wikimedia.org/other/pageviews/2022/2022-02/pageviews-20220224-060000.gz
        bash_command=("curl -o /opt/airflow/data/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ execution_date.year }}/"
            "{{ execution_date.year }}-"
            "{{ '{:02}'.format(execution_date.month) }}/"
            "pageviews-{{ execution_date.year }}{{ '{:02}'.format(execution_date.month) }}"
            "{{ '{:02}'.format(execution_date.day) }}"
             "-{{ '{:02}'.format(execution_date.hour) }}0000.gz"
        )
    )


    extract_data = BashOperator(
        task_id = 'extract_data',
        bash_command=("sleep 60 |gunzip --force /opt/airflow/data/wikipageviews.gz")
    )

    fetch_pageview = PythonOperator(
        task_id = 'fetch_pageview',
        python_callable=_fetch_pageviews,
        op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}, },
    )

    remove_datafile = BashOperator(
        task_id  = 'remove_datafile',
        bash_command="rm /opt/airflow/data/wikipageviews"
    )

    get_data >> extract_data >> fetch_pageview >> remove_datafile