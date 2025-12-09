from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import glob
import boto3

RAW_DIR = "/opt/data/telemetry_raw/"
aws_access_key = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

def upload_raw_to_s3(**context):
    files = glob.glob(RAW_DIR + "*.parquet")
    if not files:
        print("No raw files found")
        return

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )
    bucket = "smartfarm-pipeline-backup-demo"
    existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket not in existing_buckets:
        print(f"Bucket '{bucket}' does not exist. Creating...")
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print("Bucket created.")

    for f in files:
        name = os.path.basename(f)
        s3.upload_file(f, bucket, f"telemetry_raw/{name}")
        os.remove(f)
        print("Uploaded:", name)


with DAG(
    "upload_telemetry_raw",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    run = PythonOperator(
        task_id="upload_telemetry_raw_files",
        python_callable=upload_raw_to_s3,
    )
