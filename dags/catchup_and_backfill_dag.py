# Backfill is the process of manually triggering past DAG runs using the airflow dags backfill command.
# It is useful when you need to process historical data.
# The command syntax is (inside scheduler container):
# airflow dags backfill -s 2025-02-01 -e 2025-02-10 example_dag

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "amanrique",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def print_execution_date(**kwargs):
    execution_date = kwargs['execution_date']
    print(f"The execution date is: {execution_date}")

with DAG(
    dag_id = "catchup_backfill_dag_v2",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 2, 10),
    catchup=False, # Determines whether past DAG runs. If catchup=True (default) Airflow schedules all DAG runs between start_date and today, executing them one by one
) as dag:
    task1 = BashOperator(
        task_id = "task1",
        bash_command = "echo 'This is a simple bash command!!!'"
    )

    print_date_task = PythonOperator(
        task_id="print_execution_date",
        python_callable=print_execution_date,
        provide_context=True  # This ensures execution_date is passed
    )

    task1 >> print_date_task