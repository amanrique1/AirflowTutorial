from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base import BaseSensorOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

db_conn_id = 'postgres_conn'  # Airflow connection ID for PostgreSQL

class EnhancedSqlSensor(BaseSensorOperator):
    """
    Custom sensor that keeps poking until it finds at least one record
    matching the criteria, then pushes those records to XCom.
    """
    def __init__(self, conn_id, sql, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql

    def poke(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        records = hook.get_records(self.sql)

        if records and len(records) > 0:
            print(f"Found {len(records)} records, pushing to XCom")
            context['ti'].xcom_push(key='records', value=records)
            return True
        else:
            print("No records found, continuing to poke...")
            return False

def transform_and_store_data(**kwargs):
    """
    Retrieves validated data from XCom, applies transformations, and stores processed data.
    """
    ti = kwargs['ti']
    records = ti.xcom_pull(key='records', task_ids='wait_for_data')

    if not records:
        print("No data received for transformation.")
        return

    print(f"Retrieved {len(records)} records for transformation.")

    # Connect to database
    hook = PostgresHook(postgres_conn_id=db_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Prepare transformed records and insert one by one
        for record in records:
            order_name = str(record[0]).upper()  # Convert to uppercase
            order_date = str(record[1].strftime('%Y-%m-%d'))  # Standardize date format
            order_id = int(record[2])  # Ensure ID is an integer

            # Categorize order based on ID range
            order_category = "HIGH_PRIORITY" if order_id > 75 else "STANDARD"

            # Use direct SQL insertion instead of insert_rows
            cursor.execute(
                "INSERT INTO processed_orders (order_name, order_date, category) VALUES (%s, %s, %s)",
                (order_name, order_date, order_category)
            )

        # Commit all records at once
        conn.commit()
        print(f"Successfully stored {len(records)} transformed records.")

    except Exception as e:
        conn.rollback()
        print(f"Error during transformation and storage: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


with DAG(
    'sql_sensor_with_transformation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Create necessary tables
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id=db_conn_id,
        sql="""
        CREATE TABLE IF NOT EXISTS orders_sensor (
            id SERIAL PRIMARY KEY,
            order_name TEXT NOT NULL,
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS processed_orders (
            id SERIAL PRIMARY KEY,
            order_name TEXT NOT NULL,
            order_date DATE NOT NULL,
            category TEXT NOT NULL
        );
        """
    )

    # Clear data for a fresh run
    clear_data = PostgresOperator(
        task_id='clear_data',
        postgres_conn_id=db_conn_id,
        sql="TRUNCATE TABLE orders_sensor RESTART IDENTITY;"
    )

    # Insert some initial data (IDs 1-20)
    insert_initial_data = PostgresOperator(
        task_id='insert_initial_data',
        postgres_conn_id=db_conn_id,
        sql="""
        INSERT INTO orders_sensor (order_name)
        SELECT 'Order_' || generate_series(1, 20);
        """
    )

    # The sensor will keep checking until records with id > 50 exist
    wait_for_data = EnhancedSqlSensor(
        task_id='wait_for_data',
        conn_id=db_conn_id,
        sql="""
        SELECT order_name, order_date, id FROM orders_sensor WHERE id > 50;
        """,
        mode='poke',  # 'poke' mode for periodic checking
        timeout=600,  # Timeout in seconds (10 minutes)
        poke_interval=30,  # Check every 30 seconds
    )

    # Insert more records after 60 seconds to trigger the sensor
    insert_more_data = PostgresOperator(
        task_id='insert_more_data',
        postgres_conn_id=db_conn_id,
        sql="""
        INSERT INTO orders_sensor (order_name)
        SELECT 'Order_' || generate_series(21, 100);
        """,
        execution_timeout=timedelta(seconds=60)
    )

    # Transformation step
    transform_store = PythonOperator(
        task_id='transform_and_store_data',
        python_callable=transform_and_store_data,
        provide_context=True,
    )

    # Define task dependencies
    create_tables >> clear_data >> insert_initial_data >> [insert_more_data, wait_for_data] >> transform_store