"""
Script to load raw JSON messages from data lake into PostgreSQL table raw.telegram_messages
"""
import os
import json
import glob
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

DATA_DIR = "data/raw/telegram_messages"

# Define table and columns
TABLE = f"{DB_SCHEMA}.telegram_messages"
COLUMNS = [
    "message_id", "channel_name", "message_date", "message_text", "has_media",
    "image_path", "views", "forwards", "scraped_at", "message_length",
    "contains_price", "contains_contact"
]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    message_id BIGINT,
    channel_name TEXT,
    message_date TIMESTAMP,
    message_text TEXT,
    has_media BOOLEAN,
    image_path TEXT,
    views INTEGER,
    forwards INTEGER,
    scraped_at TIMESTAMP,
    message_length INTEGER,
    contains_price BOOLEAN,
    contains_contact BOOLEAN
);
"""

INSERT_SQL = f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES %s"

def get_json_files():
    files = glob.glob(os.path.join(DATA_DIR, "*", "*.json"))
    return [f for f in files if not f.endswith("_summary.json")]

def load_messages():
    all_messages = []
    for file in get_json_files():
        with open(file, "r", encoding="utf-8") as f:
            messages = json.load(f)
            for msg in messages:
                # Ensure all columns are present
                row = [msg.get(col) for col in COLUMNS]
                all_messages.append(row)
    return all_messages

def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    messages = load_messages()
    if messages:
        execute_values(cur, INSERT_SQL, messages)
        conn.commit()
        print(f"Inserted {len(messages)} messages into {TABLE}")
    else:
        print("No messages found to insert.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
