from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from airflow.hooks.postgres_hook import PostgresHook

# Function to fetch users from PostgreSQL and push to XCom
def fetch_users_from_db(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    records = hook.get_records("SELECT id, name FROM airflow_test;")  # Fetch results
    kwargs["ti"].xcom_push(key="users", value=records)

# Function to retrieve users from XCom and greet them
def greet_users(**kwargs):
    ti: TaskInstance = kwargs["ti"]
    users = ti.xcom_pull(task_ids="fetch_users", key="users")  # Retrieve from XCom

    if users:
        for user in users:
            print(f"Hello, {user[1]}!")  # Assuming the second column is the name
    else:
        print("No users found.")

# Define the DAG
with DAG(
    dag_id="postgres_example_dag",
    template_searchpath=['/opt/airflow/dags', '/opt/airflow/sql'],
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Task to create a table if it doesn't exist
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_conn",
        sql="create_airflow_test_table.sql"
    )

    # Task to insert a new record with the user-provided name
    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgres_conn",
        sql="""
        INSERT INTO airflow_test (name, created_at)
        VALUES ('{{ dag_run.conf["user_name"] | default("Airflow User") }}', '{{ ts }}');
        """
    )

    # Fetch users from DB and store in XCom
    fetch_users = PythonOperator(
        task_id="fetch_users",
        python_callable=fetch_users_from_db
    )

    # Task to process the selected data and greet each user
    greet_task = PythonOperator(
        task_id="greet_users",
        python_callable=greet_users
    )

    # Define task execution order
    create_table >> insert_data >> fetch_users >> greet_task