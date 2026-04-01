# dags/asset_scheduling.py
from airflow.sdk import DAG, Asset, task
from datetime import datetime

report_asset = Asset("s3://my-bucket/reports/daily_report.csv")

# --- Producer ---
with DAG(
    dag_id="producer_dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
):
    @task(outlets=[report_asset])
    def write_report():
        print("Writing report to S3...")
        # In practice: write the file, update a DB table, etc.

    write_report()


# --- Consumer ---
# This DAG runs automatically whenever report_asset is updated.
# No cron needed — the asset IS the schedule.
with DAG(
    dag_id="consumer_dag",
    schedule=report_asset,
    start_date=datetime(2024, 1, 1),
    catchup=False,
):
    @task
    def process_report():
        print("Processing the new report...")

    process_report()