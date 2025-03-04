from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import numpy as np
import logging
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

def fetch_sales_data(hook):
    """Fetch aggregated sales data from PostgreSQL."""
    query = """
        SELECT sale_date, category, total_sales
        FROM sales_summary
        WHERE sale_date = CURRENT_DATE;
    """
    return hook.get_pandas_df(query)

def preprocess_data(df):
    """Process sales data: encoding, scaling, PCA, and discount calculation."""
    df = df.copy()  # Avoid modifying original data

    # Convert category to numeric
    df['category_code'], _ = pd.factorize(df['category'])

    # Standardize features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(df[['category_code', 'total_sales']])

    # Apply PCA
    pca = PCA(n_components=1)
    principal_components = pca.fit_transform(features_scaled)

    df['scaled_sales'] = features_scaled[:, 1]
    df['principal_component'] = principal_components.flatten()

    # Calculate discounted sales
    max_pc = df['principal_component'].abs().max()
    discount_factor = np.where(
        max_pc > 0, 0.9 - (0.1 * df['principal_component'] / max_pc), 0.9
    )
    df['discounted_sales'] = df['total_sales'] * discount_factor

    # Rename column for consistency
    df.rename(columns={"total_sales": "original_sales"}, inplace=True)

    return df[['sale_date', 'category', 'original_sales', 'discounted_sales']]

def export_to_csv(df, path="/tmp/sales_data_export.csv"):
    """Save DataFrame to CSV."""
    df.to_csv(path, index=False)
    logger.info(f"Data exported to {path}")
    return path

def insert_transformed_data(hook, transformed_data):
    """Insert processed data into sales_transformed table."""
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Clear previous data
        cursor.execute("DELETE FROM sales_transformed WHERE sale_date = CURRENT_DATE;")

        # Batch insert
        insert_sql = """
            INSERT INTO sales_transformed (sale_date, category, original_sales, discounted_sales)
            VALUES (%s, %s, %s, %s);
        """
        cursor.executemany(insert_sql, transformed_data.values.tolist())
        conn.commit()
        logger.info(f"Successfully inserted {len(transformed_data)} rows")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def transform_sales_data(**kwargs):
    """Main function to orchestrate data transformation."""
    hook = PostgresHook(postgres_conn_id="postgres_conn")

    # Fetch, process, export, and insert data
    df = fetch_sales_data(hook)
    transformed_data = preprocess_data(df)
    local_path = export_to_csv(transformed_data)
    insert_transformed_data(hook, transformed_data)

    return {"local_csv_path": local_path}


# S3 upload function (commented out for now)
def upload_to_s3(**kwargs):
    """
    # Uncomment when ready to implement S3 upload
    # Get the local path from the previous task
    ti = kwargs['ti']
    local_csv_path = ti.xcom_pull(task_ids='transform_sales', key='return_value')['local_csv_path']

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    s3_bucket = 'your-bucket-name'
    s3_key = f'sales_data/sales_export_{kwargs["ds"]}.csv'

    s3_hook.load_file(
        filename=local_csv_path,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )

    print(f"File {local_csv_path} uploaded to s3://{s3_bucket}/{s3_key}")
    """
    print("S3 upload task is commented out for now")


# DAG Definition
with DAG(
    dag_id="postgres_sales_processing_sklearn",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # 1️⃣ Create a sales table if it doesn't exist
    create_sales_table = PostgresOperator(
        task_id="create_sales_table",
        postgres_conn_id="postgres_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS sales (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            category VARCHAR(50) NOT NULL
        );
        """
    )

    # 2️⃣ Insert sample data (simulating daily sales)
    insert_sales_data = PostgresOperator(
        task_id="insert_sales_data",
        postgres_conn_id="postgres_conn",
        sql="""
        INSERT INTO sales (sale_date, amount, category)
        VALUES (CURRENT_DATE, 100.00, 'Electronics'),
               (CURRENT_DATE, 50.00, 'Books'),
               (CURRENT_DATE, 200.00, 'Furniture');
        """
    )

    # 3️⃣ Create an aggregated table if it doesn't exist
    create_aggregated_table = PostgresOperator(
        task_id="create_aggregated_table",
        postgres_conn_id="postgres_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS sales_summary (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            category VARCHAR(50) NOT NULL,
            total_sales DECIMAL(10, 2) NOT NULL
        );
        """
    )

    # 4️⃣ Aggregate total sales per category and insert into `sales_summary`
    aggregate_sales = PostgresOperator(
        task_id="aggregate_sales",
        postgres_conn_id="postgres_conn",
        sql="""
        INSERT INTO sales_summary (sale_date, category, total_sales)
        SELECT sale_date, category, SUM(amount)
        FROM sales
        WHERE sale_date = CURRENT_DATE
        GROUP BY sale_date, category;
        """
    )

    # 5️⃣ Create a transformed sales table
    create_transformed_table = PostgresOperator(
        task_id="create_transformed_table",
        postgres_conn_id="postgres_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS sales_transformed (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            category VARCHAR(50) NOT NULL,
            original_sales DECIMAL(10, 2) NOT NULL,
            discounted_sales DECIMAL(10, 2) NOT NULL
        );
        """
    )

    # 6️⃣ Retrieve and process data using scikit-learn, export to CSV
    transform_sales = PythonOperator(
        task_id="transform_sales",
        python_callable=transform_sales_data,
        provide_context=True
    )

    # 7️⃣ Upload CSV to S3 (commented out for now)
    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True
    )

    # 8️⃣ Delete sales data older than 30 days
    cleanup_old_data = PostgresOperator(
        task_id="cleanup_old_data",
        postgres_conn_id="postgres_conn",
        sql="DELETE FROM sales WHERE sale_date < CURRENT_DATE - INTERVAL '30 days';"
    )

    # Define task execution order
    create_sales_table >> insert_sales_data >> create_aggregated_table >> aggregate_sales
    aggregate_sales >> create_transformed_table >> transform_sales >> upload_to_s3_task >> cleanup_old_data