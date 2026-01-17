"""
Script to load YOLO detection results from CSV into PostgreSQL table raw.yolo_detections
"""
import os
import csv
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load DB credentials from .env
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SCHEMA = os.getenv("DB_SCHEMA", "raw")

CSV_PATH = "data/yolo_detections.csv"
TABLE = f"{DB_SCHEMA}.yolo_detections"
COLUMNS = ["image_path", "detected_class", "confidence_score", "image_category"]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    image_path TEXT,
    detected_class INTEGER,
    confidence_score FLOAT,
    image_category TEXT
);
"""

INSERT_SQL = f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES %s"

def load_yolo_detections():
    with psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            with open(CSV_PATH, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = [tuple(row[col] for col in COLUMNS) for row in reader]
            if rows:
                execute_values(cur, INSERT_SQL, rows)
            conn.commit()
    print(f"Loaded {len(rows)} rows into {TABLE}")

if __name__ == "__main__":
    load_yolo_detections()
