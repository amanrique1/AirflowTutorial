from datetime import datetime, timedelta
from airflow.decorators import task, dag

default_args = {
    "owner": "amanrique",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(dag_id="first_python_dec_dag",
    default_args=default_args,
    description="My first dag",
    schedule_interval="0 6 * * 1-5",
    start_date=datetime(2025, 2, 15),
    catchup=False)
def happy_birthday_etl():

    @task(task_id="birthday_greeting")
    def birthday(date,name, last_name, age):
        str_date = date.strftime("%Y-%m-%d")
        print(f"Hello {name} {last_name}, born on {str_date}, happy {age}th birthday!!!")

    @task(task_id="greet_python")
    def greet():
        print("Hello World, this is my first task using Python!!!!")

    @task(task_id="get_birthday_guy", multiple_outputs=True)
    def get_birthday_guy():
        return {"name": "Andres", "last_name": "Manrique"}

    @task(task_id="get_birthday_guy_age")
    def get_birthday_guy_age():
        return 25

    task1 = greet()
    task2 = get_birthday_guy()
    task3 = get_birthday_guy_age()
    task4 = birthday(datetime.now(),task2["name"], task2["last_name"] , task3)

    task1 >> [task2, task3] >> task4

birthday_dag = happy_birthday_etl()