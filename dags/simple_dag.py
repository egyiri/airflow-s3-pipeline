import datetime as dt
from io import StringIO
import csv

import duckdb
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

source_bucket_name = "bright-first-s3-bucket-demo-563649768086-us-east-1-an"
source_key = "raw_data/packages/packages.csv"
destination_bucket_name = "bright-first-s3-bucket-demo-563649768086-us-east-1-an"
destination_key = "transformation_results/transformed_packages.csv"


def extract(**context):
    s3_hook = S3Hook(aws_conn_id="aws_default")
    csv_content = s3_hook.read_key(key=source_key, bucket_name=source_bucket_name)
    context['ti'].xcom_push(key='extracted_data', value=csv_content)
    print(f"Extracted data from S3 bucket '{source_bucket_name}' with key '{source_key}'.")




def transform(**context):
    csv_content = context['ti'].xcom_pull(key='extracted_data', task_ids='extract')

    lines = csv_content.splitlines()
    reader = csv.reader(lines)
    header = next(reader)
    expected_cols = len(header)

    good_rows = []
    bad_row_count = 0

    for row in reader:
        if len(row) == expected_cols:
            good_rows.append(row)
        else:
            bad_row_count += 1

    print(f"Total data rows: {bad_row_count + len(good_rows)}")
    print(f"Good rows: {len(good_rows)}")
    print(f"Malformed rows dropped: {bad_row_count}")

    df = pd.DataFrame(good_rows, columns=header)

    # Now safely convert numeric columns
    for col in ["depth_cm", "height_cm", "width_cm", "planned_service_time_seconds"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    transformed_df = duckdb.sql("""
        SELECT
            package_id,
            route_id,
            stop_id,
            scan_status,
            time_window_start,
            time_window_end,
            planned_service_time_seconds,
            depth_cm,
            height_cm,
            width_cm,
            ROUND(depth_cm * height_cm * width_cm, 2) AS volume_cm3,
            CASE
                WHEN (depth_cm * height_cm * width_cm) > 20000 THEN 'Oversized'
                WHEN (depth_cm * height_cm * width_cm) > 5000 THEN 'Medium'
                ELSE 'Small'
            END AS size_category,
            CASE
                WHEN scan_status IS NULL OR scan_status = '' THEN 'Unscanned'
                ELSE scan_status
            END AS scan_status_clean
        FROM df
        WHERE depth_cm IS NOT NULL
          AND height_cm IS NOT NULL
          AND width_cm IS NOT NULL
        ORDER BY route_id, volume_cm3 DESC
    """).df()

    output_buffer = StringIO()
    transformed_df.to_csv(output_buffer, index=False)

    context['ti'].xcom_push(key='transformed_data', value=output_buffer.getvalue())
    print(f"Transformed {len(df)} rows -> {len(transformed_df)} rows after transformation.")


def load(**context):
    transformed_csv_content = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_string(
        string_data=transformed_csv_content,
        key=destination_key,
        bucket_name=destination_bucket_name,
        replace=True
    )
    print(f"Loaded transformed data to S3 bucket '{destination_bucket_name}' with key '{destination_key}'.")


with DAG(
    dag_id="simple_dag",
    schedule= "0 2 * * * *",  
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
) as dag:
    start_task = EmptyOperator(task_id="start")
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)
    end_task = EmptyOperator(task_id="end")

    start_task >> extract_task >> transform_task >> load_task >> end_task
