import io
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.exceptions import AirflowSkipException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 Configuration
USE_MINIO = True  # Set to False for AWS S3
AWS_CONN_ID = "minio_conn" if USE_MINIO else "aws_default"

BUCKET_NAME = "my-data-bucket"
BRONZE_PREFIX = "bronze/"
SILVER_PREFIX = "silver/"
GOLD_PREFIX = "gold/"
PROCESSED_FILES_VAR = "s3_medallion_processed_files"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "s3_continuous_medallion_pipeline",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # Run every minute
    catchup=False,
    max_active_runs=1,  # Ensure only one DAG run at a time
)

def delete_dag_run_if_no_files_processed(**kwargs):
    """Mark the DAG run as skipped if no files were processed."""
    ti = kwargs["ti"]
    latest_silver_file = ti.xcom_pull(task_ids="clean_bronze_to_silver")
    latest_gold_file = ti.xcom_pull(task_ids="transform_silver_to_gold")

    if not latest_silver_file and not latest_gold_file:
        logger.info("No files were processed. Marking DAG run as skipped.")
        raise AirflowSkipException("No files were processed.")
    else:
        logger.info(f"Files processed: Silver={latest_silver_file}, Gold={latest_gold_file}")

def read_from_s3(bucket_name, key):
    """Reads a CSV file from S3 and returns it as a Pandas DataFrame."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    response = s3_hook.get_key(bucket_name=bucket_name, key=key)

    if response is None:
        raise ValueError(f"File {key} not found in bucket {bucket_name}")

    data = response.get()["Body"].read().decode("utf-8")
    df = pd.read_csv(io.StringIO(data))
    logger.info(f"Successfully read {key} from {bucket_name}")
    return df

def write_to_s3(df, bucket_name, key):
    """Writes a Pandas DataFrame to S3 as a CSV file."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
    logger.info(f"Successfully wrote {key} to {bucket_name}")

def get_processed_files():
    """Retrieve the list of already processed files from Airflow Variables."""
    try:
        processed_files = Variable.get(PROCESSED_FILES_VAR, deserialize_json=True)
    except KeyError:
        # If the variable doesn't exist yet, create it with an empty list
        processed_files = []
        Variable.set(PROCESSED_FILES_VAR, processed_files, serialize_json=True)
    return processed_files

def mark_file_as_processed(file_path):
    """Mark a file as processed in the Airflow Variables."""
    processed_files = get_processed_files()
    if file_path not in processed_files:
        processed_files.append(file_path)
        Variable.set(PROCESSED_FILES_VAR, processed_files, serialize_json=True)
        logger.info(f"Marked {file_path} as processed")

def generate_bronze_key_pattern(**kwargs):
    """Generate a pattern for unprocessed files."""
    processed_files = get_processed_files()
    # Construct a pattern that excludes already processed files
    # This is a simplified approach - in practice, you might need
    # a more sophisticated pattern or filtering method
    pattern = f"{BRONZE_PREFIX}*"

    logger.info(f"Looking for new files with pattern: {pattern}")
    logger.info(f"Already processed {len(processed_files)} files")

    # We push the processed files list to XCom for later use
    kwargs['ti'].xcom_push(key='processed_files', value=processed_files)

    return pattern

def find_latest_bronze_file(**kwargs):
    """Find the latest file in the bronze directory that hasn't been processed yet."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    bronze_files = s3_hook.list_keys(
        bucket_name=BUCKET_NAME,
        prefix=BRONZE_PREFIX
    )

    if not bronze_files:
        logger.info(f"No files found in {BUCKET_NAME}/{BRONZE_PREFIX}")
        return None

    # Get list of processed files from XCom
    ti = kwargs['ti']
    processed_files = ti.xcom_pull(task_ids='generate_bronze_key_pattern', key='processed_files')

    # Filter out already processed files
    unprocessed_files = [f for f in bronze_files if f not in processed_files]

    if not unprocessed_files:
        logger.info("No new files to process")
        return None

    # Sort by last modified time
    latest_file = sorted(
        unprocessed_files,
        key=lambda k: s3_hook.get_key(k, BUCKET_NAME).last_modified,
        reverse=True
    )[0]

    logger.info(f"Found unprocessed file: {latest_file}")
    return latest_file

def clean_data(**kwargs):
    """Reads Bronze data from S3, cleans it, and stores it in Silver."""
    ti = kwargs["ti"]
    new_file = ti.xcom_pull(task_ids="find_latest_bronze_file")

    if not new_file:
        logger.info("No new files to process. Skipping this step.")
        return None

    df = read_from_s3(BUCKET_NAME, new_file)

    # Drop duplicates and missing values
    df.drop_duplicates(inplace=True)
    df.dropna(subset=["sale_date", "category", "total_sales"], inplace=True)

    # Convert data types
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df["total_sales"] = pd.to_numeric(df["total_sales"], errors="coerce").fillna(0)

    silver_file = new_file.replace(BRONZE_PREFIX, SILVER_PREFIX)
    logger.info(f"Cleaned data stored as: {silver_file}")
    write_to_s3(df, BUCKET_NAME, silver_file)

    # Mark the bronze file as processed
    mark_file_as_processed(new_file)

    return silver_file

def transform_data(**kwargs):
    """Reads Silver data from S3, applies transformations, and stores the result in Gold."""
    ti = kwargs["ti"]
    latest_silver_file = ti.xcom_pull(task_ids="clean_bronze_to_silver")

    if not latest_silver_file:
        logger.info("No silver file to process. Skipping this step.")
        return None

    df = read_from_s3(BUCKET_NAME, latest_silver_file)

    # Aggregate sales data by date and category
    df_gold = df.groupby(["sale_date", "category"], as_index=False).agg(
        total_sales=("total_sales", "sum")
    )

    gold_file = latest_silver_file.replace(SILVER_PREFIX, GOLD_PREFIX)
    logger.info(f"Transformed data stored as: {gold_file}")
    write_to_s3(df_gold, BUCKET_NAME, gold_file)
    return gold_file

# Task to generate pattern for S3KeySensor
bronze_key_pattern = PythonOperator(
    task_id="generate_bronze_key_pattern",
    python_callable=generate_bronze_key_pattern,
    provide_context=True,
    dag=dag,
)

# Sensor to detect new files in Bronze storage
wait_for_bronze_file = S3KeySensor(
    task_id="wait_for_bronze_file",
    bucket_name=BUCKET_NAME,
    wildcard_match=True,
    bucket_key=f"{BRONZE_PREFIX}*",  # We'll check for any file in Bronze
    aws_conn_id=AWS_CONN_ID,
    poke_interval=60,  # Check every 60 seconds
    timeout=3600,  # 1 hour timeout
    mode="reschedule",  # Important: Use reschedule mode to free up worker slots
    soft_fail=True,  # Continue the DAG even if the sensor times out
    dag=dag,
)

# Task to find the latest unprocessed bronze file
latest_bronze_file = PythonOperator(
    task_id="find_latest_bronze_file",
    python_callable=find_latest_bronze_file,
    provide_context=True,
    dag=dag,
)

# Task to clean Bronze data and store it in Silver
clean_bronze_to_silver = PythonOperator(
    task_id="clean_bronze_to_silver",
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

# Task to transform Silver data and store it in Gold
transform_silver_to_gold = PythonOperator(
    task_id="transform_silver_to_gold",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task to mark DAG run as skipped if no files were processed
skip_dag_run = PythonOperator(
    task_id="skip_dag_run_if_no_files_processed",
    python_callable=delete_dag_run_if_no_files_processed,
    provide_context=True,
    dag=dag,
)

# Update task dependencies
bronze_key_pattern >> wait_for_bronze_file >> latest_bronze_file >> clean_bronze_to_silver >> transform_silver_to_gold >> skip_dag_run