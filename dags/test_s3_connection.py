from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
from io import StringIO

def read_csv_from_s3(**context):
    bucket_name =  "bright-first-s3-bucket-demo-563649768086-us-east-1-an"
    key = "raw_data/stops/stops.csv"

    s3 = S3Hook(aws_conn_id="aws_default")
    csv_content = s3.read_key(key=key, bucket_name=bucket_name)

    df = pd.read_csv(StringIO(csv_content))
    print(df.head())
    print(f"Rows: {len(df)}")

with DAG(
    dag_id="test_s3_connection",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    read_task = PythonOperator(
        task_id="read_csv_from_s3",
        python_callable=read_csv_from_s3,
    )
