from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import logging
import os
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 Configuration
USE_MINIO = True  # Set to False for AWS S3
AWS_CONN_ID = "minio_conn" if USE_MINIO else "aws_default"

BUCKET_NAME = "my-cdc-bucket"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ecommerce_orders_to_s3_temp_file',
    default_args=default_args,
    description='A DAG to extract daily e-commerce orders from PostgreSQL, convert to Parquet, and upload to S3 using a temporary file',
    schedule_interval='@daily',  # Run daily
    catchup=False,
)

# Task 1: Query PostgreSQL for orders created the day before the execution day
def query_orders(**kwargs):
    # Get the execution date (logical date) from the context
    execution_date = kwargs['execution_date']

    # Calculate the day before the execution day
    day_before_execution = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    # PostgreSQL query to fetch orders created on the day before the execution day
    query = f"""
        SELECT order_id, customer_id, order_date, total_amount, status
        FROM orders
        WHERE order_date >= '{day_before_execution} 00:00:00'
        AND order_date < '{day_before_execution} 23:59:59';
    """

    # Fetch data from PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = postgres_hook.get_conn()
    df = pd.read_sql(query, connection)

    # Push the DataFrame to XCom for the next task
    kwargs['ti'].xcom_push(key='orders_data', value=df)

    logger.info(f"Orders data for {day_before_execution} queried successfully.")

# Task 2: Convert the orders data into a Parquet file and save it to a temporary directory
def convert_to_parquet(**kwargs):
    # Pull the DataFrame from XCom
    ti = kwargs['ti']
    df = ti.xcom_pull(key='orders_data', task_ids='query_orders')

    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    parquet_file_path = os.path.join(temp_dir, 'orders.parquet')

    # Convert DataFrame to Parquet and save it to the temporary file
    df.to_parquet(parquet_file_path, engine='pyarrow', compression="snappy")

    # Push the temporary file path to XCom for the next task
    kwargs['ti'].xcom_push(key='parquet_file_path', value=parquet_file_path)

    logger.info(f"Orders data converted to Parquet and saved to {parquet_file_path}.")

# Task 3: Upload the Parquet file to S3 and clean up the temporary file
def upload_to_s3(**kwargs):
    # Pull the temporary file path from XCom
    ti = kwargs['ti']
    parquet_file_path = ti.xcom_pull(key='parquet_file_path', task_ids='convert_to_parquet')

    # Get the execution date (logical date) from the context
    execution_date = kwargs['execution_date']

    # Calculate the day before the execution day
    day_before_execution = (execution_date - timedelta(days=1))

    # Generate the S3 key with partitioning by year, month, and day
    s3_key = f"orders/year={day_before_execution.year}/month={day_before_execution.month}/day={day_before_execution.day}/orders.parquet"

    # Upload the Parquet file to S3
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_file(
        filename=parquet_file_path,
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )

    logger.info(f"Parquet file for {day_before_execution} uploaded to S3 at {s3_key}.")

    # Clean up the temporary file
    os.remove(parquet_file_path)
    logger.info(f"Temporary file {parquet_file_path} deleted.")

# Define the tasks
task_query_orders = PythonOperator(
    task_id='query_orders',
    python_callable=query_orders,
    provide_context=True,
    dag=dag,
)

task_convert_to_parquet = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=convert_to_parquet,
    provide_context=True,
    dag=dag,
)

task_upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task_query_orders >> task_convert_to_parquet >> task_upload_to_s3