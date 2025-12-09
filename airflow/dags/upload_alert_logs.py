from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import glob
import gzip
import boto3

ALERT_LOG_DIR = "/opt/data/alert_logs/"
OUTPUT_TMP = "/opt/data/alert_logs/tmp/"
aws_access_key = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

os.makedirs(OUTPUT_TMP, exist_ok=True)

def combine_and_upload(**context):
    now = datetime.utcnow()

    files = glob.glob(ALERT_LOG_DIR + "*.log")

    if not files:
        print("No alert log files found.")
        return

    combined_path = os.path.join(OUTPUT_TMP, f"alerts_{now:%Y%m%d_%H%M}.log")

    with open(combined_path, "wb") as out:
        for f in sorted(files):
            with open(f, "rb") as src:
                out.write(src.read())

    gz_path = combined_path + ".gz"
    with open(combined_path, 'rb') as f_in:
        with gzip.open(gz_path, 'wb') as f_out:
            f_out.writelines(f_in)

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

    try:
        s3.upload_file(gz_path, bucket, f"alerts/{os.path.basename(gz_path)}")
        print("Uploaded:", gz_path)
    except Exception as e:
        print("S3 upload failed:", e)

    for f in files:
        os.remove(f)


with DAG(
    "upload_alert_logs",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    run = PythonOperator(
        task_id="combine_and_upload_alert_logs",
        python_callable=combine_and_upload,
    )
