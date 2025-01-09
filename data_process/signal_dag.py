from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from package.crawl_data import main as crawl_data_5m

default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 12,17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="calculate_signal", 
    description="Calculate signal",
    default_args=default_args,
    start_date=datetime(2024, 6, 12),
    schedule_interval="5 * * * *",  
) as dag:
    # task0 = PythonOperator(
    #     task_id="print", 
    #     python_callable=print,
    #     op_args=["start"]
    # )
    task1 = PythonOperator(
        task_id="tmp_1", 
        python_callable=crawl_data_5m,
    )
    # task0  
    task1
    # # >> task2 >> task3
