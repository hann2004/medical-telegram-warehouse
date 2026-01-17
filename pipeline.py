from dagster import job, op, ScheduleDefinition, Definitions, failure_hook
import datetime
import os
# Failure alert hook (prints to console, can be replaced with email/Slack)
@failure_hook
def notify_on_failure(context):
    print(f"[ALERT] Pipeline failed at {datetime.datetime.now()}! Run ID: {context.run_id}")
    # Example: send email or Slack notification here
import subprocess

@op
def scrape_telegram_data():
    result = subprocess.run(["python", "src/scraper.py"], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Scraper failed: {result.stderr}")
    return "Scraping complete"

@op
def load_raw_to_postgres():
    result = subprocess.run(["python", "scripts/load_to_postgres.py"], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Load to Postgres failed: {result.stderr}")
    return "Raw data loaded to Postgres"

@op
def run_dbt_transformations():
    result = subprocess.run([
        "dbt", "run",
        "--project-dir", "medical_warehouse",
        "--profiles-dir", "medical_warehouse"
    ], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}\n{result.stdout}")
    return "dbt transformations complete"

@op
def run_yolo_enrichment():
    result = subprocess.run(["python", "src/yolo_detect.py"], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"YOLO enrichment failed: {result.stderr}")
    # Load YOLO results to Postgres
    result2 = subprocess.run(["python", "scripts/load_yolo_to_postgres.py"], capture_output=True, text=True)
    if result2.returncode != 0:
        raise Exception(f"Load YOLO to Postgres failed: {result2.stderr}")
    return "YOLO enrichment and load complete"


@job(hooks={notify_on_failure})
def medical_telegram_pipeline():
    # Enforce execution order by calling ops in sequence
    scrape_telegram_data()
    load_raw_to_postgres()
    run_dbt_transformations()
    run_yolo_enrichment()

# Daily schedule at 2:00 AM
daily_schedule = ScheduleDefinition(
    job=medical_telegram_pipeline,
    cron_schedule="0 2 * * *",  # 2:00 AM every day
    name="daily_medical_telegram_pipeline",
)

# Register job and schedule with Dagster
defs = Definitions(
    jobs=[medical_telegram_pipeline],
    schedules=[daily_schedule],
)
