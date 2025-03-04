from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "amanrique",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def birthday(date: datetime, ti):
    name = ti.xcom_pull(task_ids="get_birthday_guy", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_birthday_guy", key="last_name")
    age = ti.xcom_pull(task_ids="get_birthday_guy_age", key="age")
    str_date = date.strftime("%Y-%m-%d")
    print(f"Hello {name} {last_name}, born on {str_date}, happy {age}th birthday!!!")

def greet():
    print("Hello World, this is my first task using Python!!!!")

def get_birthday_guy(ti):
    ti.xcom_push(key="first_name", value="Andres")
    ti.xcom_push(key="last_name", value="Manrique")

def get_birthday_guy_age(ti):
    ti.xcom_push(key="age", value=25)

with DAG(
    dag_id="first_python_dag",
    default_args=default_args,
    description="My first dag",
    schedule_interval="0 6 * * 2,5",
    start_date=datetime(2025, 2, 15),
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id="greet_python",
        python_callable=greet
    )
    task2 = PythonOperator(
        task_id="get_birthday_guy",
        python_callable=get_birthday_guy
    )
    task3 = PythonOperator(
        task_id="get_birthday_guy_age",
        python_callable=get_birthday_guy_age
    )

    task4 = PythonOperator(
        task_id="birthday_greeting",
        python_callable=birthday,
        op_args=[datetime.now()]
    )

    task1  >> [task2, task3] >> task4