"""
Branching in Airflow refers to the ability to run different tasks based on
certain conditions. This means that the path or flow of the workflow can change
dynamically based on the results or outputs of previous tasks.
"""


import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
import random

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


def create_data():
    # Create a mock dataframe
    df = pd.DataFrame({
        'numbers': range(10),
        'letters': list('abcdefghij')
    })
    # Save to CSV to simulate extraction and to use in subsequent tasks
    df.to_csv('/tmp/mock_data.csv', index=False)


def process_data():
    # Load the data
    df = pd.read_csv('/tmp/mock_data.csv')
    # Increase all numbers by 2
    df['numbers'] = df['numbers'] + 2
    # Save modified data
    df.to_csv('/tmp/processed_data.csv', index=False)


def check_data_quality(**kwargs):
    # Load processed data
    df = pd.read_csv('/tmp/processed_data.csv')
    # Check if all numbers are even as a mock "quality check"
    quality = all(df['numbers'] % 2 == 0)
    return 'generate_report' if quality else 'handle_error' # Return a list for running multiple branches


def generate_report():
    # Load processed data
    df = pd.read_csv('/tmp/processed_data.csv')
    # Generate a report – in this case, just print to stdout
    print("Report Generated!")
    print(df.describe())


def handle_error():
    print("Data Quality is not sufficient!")


with DAG(
    'branching_example_with_pandas',
    default_args=default_args,
    description='An example DAG with branching using Pandas',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['example'],
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=create_data,
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    data_quality_check = BranchPythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        provide_context=True,
    )

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    handle_error = PythonOperator(
        task_id='handle_error',
        python_callable=handle_error,
    )

    extract_data >> process_data >> data_quality_check
    data_quality_check >> [generate_report, handle_error]