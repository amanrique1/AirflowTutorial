from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from airflow.hooks.postgres_hook import PostgresHook

def fetch_users_with_hook(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    records = hook.get_records("SELECT id, name FROM airflow_test;")
    kwargs["ti"].xcom_push(key="hook_users", value=records)

def validate_integrity(**kwargs):
    ti: TaskInstance = kwargs['ti']
    users_op = ti.xcom_pull(task_ids="fetch_users_op", key="return_value")
    users_hook = ti.xcom_pull(task_ids="fetch_users_hook", key="hook_users")

    if users_op != users_hook:
        raise ValueError("Data mismatch between fetch_users_op and fetch_users_hook")
    print("Data Integrity Verified")
    return users_op

# Function to retrieve users from XCom and greet them
def greet_users(**kwargs):
    ti: TaskInstance = kwargs["ti"]
    users = ti.xcom_pull(task_ids="integrity_validator", key="return_value")  # Retrieve from XCom

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

    fetch_users_op = PostgresOperator(
        task_id="fetch_users_op",
        postgres_conn_id="postgres_conn",
        sql="SELECT id, name FROM airflow_test;",
    )

    fetch_users_hook = PythonOperator(
        task_id="fetch_users_hook",
        python_callable=fetch_users_with_hook
    )

    integrity_validator = PythonOperator(
        task_id="integrity_validator",
        python_callable=validate_integrity
    )

    # Task to process the selected data and greet each user
    greet_task = PythonOperator(
        task_id="greet_users",
        python_callable=greet_users
    )

    # Define task execution order
    create_table >> insert_data >> [fetch_users_op, fetch_users_hook] >> integrity_validator >> greet_task