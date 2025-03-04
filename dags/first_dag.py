from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "amanrique",
    "retries": 3,
    "retry_delay": timedelta(minutes=2) }

with DAG(
    dag_id="first_dag_v2",
    default_args=default_args,
    description="My first dag",
    schedule_interval= timedelta(minutes=7),
    start_date=datetime(2024, 2, 15, 0, 6),
) as dag:
    task1 = BashOperator(
        task_id="task1_echo",
        bash_command='echo "Hello World this is my first task!!!!"')

    task2 = BashOperator(
        task_id="task2_echo",
        bash_command='echo "I am the second task, I run after the first one!!!"')
    
    task3 = BashOperator(
        task_id="task3_echo",
        bash_command='echo "I am the third task, I run after the first one!!!"')
    
    task4 = BashOperator(
        task_id="task4_echo",
        bash_command='echo "I am the fourth task, I run after the second and third one!!!"')

    task1 >> [task2, task3]
    task2.set_downstream(task4)
    task3.set_downstream(task4)