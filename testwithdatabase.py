import csv
import psycopg2
import logging
import os
from datetime import datetime

# ==== CONFIG ====
COMPLETION_THRESHOLD = True  # Change to True if you want only completed tasks

DB_CONFIG = {
    "host": "localhost",
    "dbname": "reporting",
    "user": "postgres",
    "password": "admin"
}

# Create output and log directories
OUTPUT_DIR = "output"
LOG_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Generate timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
CSV_FILE = os.path.join(OUTPUT_DIR, f"filtered_tasks_{timestamp}.csv")
RUN_LOG_FILE = os.path.join(LOG_DIR, f"etl_run_{timestamp}.log")
GENERAL_LOG_FILE = os.path.join(LOG_DIR, "etl_pipeline.log")

# ==== SETUP LOGGING ====
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clear existing handlers
if logger.hasHandlers():
    logger.handlers.clear()

# Add log handlers
run_handler = logging.FileHandler(RUN_LOG_FILE)
run_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(run_handler)

general_handler = logging.FileHandler(GENERAL_LOG_FILE)
general_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(general_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(console_handler)

# ==== EXTRACT from PostgreSQL ====
def extract_tasks_from_db():
    logging.info("Extracting tasks from PostgreSQL table `source_tasks`...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT id, title, user_Id, completed FROM source_tasks")
        rows = cur.fetchall()
        tasks = [
            {
                "id": row[0],
                "title": row[1],
                "user_Id": row[2],
                "completed": row[3]
            }
            for row in rows
        ]
        cur.close()
        conn.close()
        logging.info(f"Extracted {len(tasks)} tasks from source_tasks.")
        return tasks
    except Exception as e:
        logging.error(f"Failed to extract from database: {e}")
        return []

# ==== TRANSFORM ====
def transform_tasks(tasks):
    logging.info("Transforming tasks with completion threshold...")
    filtered = []
    for task in tasks:
        if task["completed"] == COMPLETION_THRESHOLD:
            filtered.append({
                "task_id": task["id"],
                "title": task["title"],
                "user_id": task["user_Id"],
                "completed": task["completed"]
            })
    logging.info(f"{len(filtered)} tasks passed the threshold filter.")
    return filtered

# ==== LOAD TO CSV ====
def load_to_csv(data):
    logging.info(f"Saving filtered tasks to CSV at {CSV_FILE}...")
    try:
        with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=["task_id", "title", "user_id", "completed"])
            writer.writeheader()
            writer.writerows(data)
        logging.info("CSV write completed.")
    except Exception as e:
        logging.error(f"Failed to write CSV: {e}")

# ==== LOAD TO POSTGRESQL ====
def load_to_postgres(data):
    logging.info("Loading data into PostgreSQL table `todo_metrics`...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS todo_metrics (
                task_id INTEGER PRIMARY KEY,
                title TEXT,
                user_id INTEGER,
                completed BOOLEAN
            )
        """)
        for row in data:
            cur.execute("""
                INSERT INTO todo_metrics (task_id, title, user_id, completed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (task_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    user_id = EXCLUDED.user_id,
                    completed = EXCLUDED.completed
            """, (row["task_id"], row["title"], row["user_id"], row["completed"]))
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Data loaded into `todo_metrics` successfully.")
    except Exception as e:
        logging.error(f"PostgreSQL load failed: {e}")

# ==== RUN ETL ====
if __name__ == "__main__":
    logging.info(f"===== ETL Run Started at {timestamp} =====")
    raw_tasks = extract_tasks_from_db()
    transformed_tasks = transform_tasks(raw_tasks)
    load_to_csv(transformed_tasks)
    load_to_postgres(transformed_tasks)
    logging.info("ETL pipeline completed successfully.")
    logging.info("=============================================")
