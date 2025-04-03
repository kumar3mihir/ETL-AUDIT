import os
import pandas as pd
import logging
import psycopg2
import json
import smtplib
import hashlib
import time
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
from cryptography.fernet import Fernet

# Load environment variables
load_dotenv()

# Logging Setup (Auditability)
logging.basicConfig(filename="etl_log.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Database Configurations (From Secure .env File)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# File Paths
DATA_SOURCE = "data/source_data.csv"
ARCHIVE_PATH = "data/archive/"
ERROR_LOG_PATH = "data/error_records.csv"
COMPLIANCE_REPORT = "data/compliance_report.json"

# Database Connection String
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Encryption Key (For Sensitive Data Masking)
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", Fernet.generate_key().decode())
cipher = Fernet(ENCRYPTION_KEY.encode())

def send_alert_email(subject, message):
    """Sends an email alert to administrators in case of errors."""
    sender_email = os.getenv("ALERT_EMAIL")
    receiver_email = os.getenv("ADMIN_EMAIL")
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(sender_email, receiver_email, f"Subject: {subject}\n\n{message}")
        server.quit()
        logging.info("Alert email sent successfully.")
    except Exception as e:
        logging.error("Error sending alert email: %s", str(e))

def requires_admin_approval():
    """Checks if the script modification requires admin approval."""
    approval_file = "admin_approval.json"
    if os.path.exists(approval_file):
        with open(approval_file, "r") as f:
            approval_data = json.load(f)
            if approval_data.get("approved") == True:
                logging.info("Admin approval granted.")
                return True
    logging.error("ETL script modification requires admin approval!")
    send_alert_email("ETL Job Approval Required", "An attempt was made to modify the ETL script without approval.")
    return False

def extract_data(file_path):
    """Extract data from CSV file."""
    try:
        logging.info("Extracting data from %s", file_path)
        df = pd.read_csv(file_path)
        logging.info("Successfully extracted %d records.", len(df))
        return df
    except Exception as e:
        logging.error("Error in extraction: %s", str(e))
        return None

def validate_data(df):
    """Validates data for correctness and completeness."""
    logging.info("Validating data quality...")
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    df = df[df["email"].str.contains("@")]
    df = df[df["age"] > 0]
    logging.info("Data validation completed. %d valid records remaining.", len(df))
    return df

def transform_data(df):
    """Perform data transformation, cleaning, and encryption."""
    try:
        logging.info("Starting data transformation...")
        df["email"] = df["email"].str.lower()
        df["created_at"] = pd.to_datetime(df["created_at"])
        df["ssn"] = df["ssn"].apply(lambda x: cipher.encrypt(x.encode()).decode())  # Encrypt sensitive data
        logging.info("Transformation completed successfully. %d valid records remaining.", len(df))
        return df
    except Exception as e:
        logging.error("Error in transformation: %s", str(e))
        return None

def load_data(df, table_name="customers"):
    """Load data into PostgreSQL database with row count validation."""
    try:
        logging.info("Connecting to database...")
        engine = create_engine(DATABASE_URL)
        conn = engine.connect()

        # Track Changes in ETL Configurations (Data Governance)
        config_version = hashlib.sha256(df.to_json().encode()).hexdigest()
        with open("etl_config_version.txt", "w") as f:
            f.write(config_version)

        df.to_sql(table_name, engine, if_exists="append", index=False)

        # Validate Row Counts (Reconciliation)
        count_query = f"SELECT COUNT(*) FROM {table_name};"
        row_count = pd.read_sql(count_query, engine).iloc[0, 0]
        logging.info("Data loaded successfully. Total records in table: %d", row_count)

        conn.close()
    except Exception as e:
        logging.error("Error in loading data: %s", str(e))
        send_alert_email("ETL Load Error", f"Data load failed: {str(e)}")



def get_last_checkpoint():
    """Retrieve last successfully processed record ID from the checkpoint table."""
    try:
        engine = create_engine(DATABASE_URL)
        query = "SELECT last_processed_id FROM etl_checkpoint ORDER BY checkpoint_time DESC LIMIT 1;"
        result = pd.read_sql(query, engine)
        return result.iloc[0, 0] if not result.empty else None
    except Exception as e:
        logging.warning("Checkpoint retrieval failed: %s", str(e))
        return None

def save_checkpoint(last_id):
    """Save the last successfully processed record ID for restartability."""
    try:
        engine = create_engine(DATABASE_URL)
        engine.execute(f"INSERT INTO etl_checkpoint (last_processed_id, checkpoint_time) VALUES ({last_id}, NOW());")
        logging.info("Checkpoint saved: Last processed ID = %d", last_id)
    except Exception as e:
        logging.error("Error saving checkpoint: %s", str(e))

def load_data(df, table_name="customers"):
    """Load data into PostgreSQL, handling restartability with checkpoints."""
    try:
        engine = create_engine(DATABASE_URL)
        last_id = get_last_checkpoint()
        
        # If there's a checkpoint, filter new records only
        if last_id:
            df = df[df["id"] > last_id]
            logging.info("Resuming ETL from record ID %d", last_id)
        
        if df.empty:
            logging.info("No new records to process. Skipping load step.")
            return

        df.to_sql(table_name, engine, if_exists="append", index=False)
        save_checkpoint(df["id"].max())  # Save last processed record ID
        logging.info("Data loaded successfully.")

    except Exception as e:
        logging.error("Error loading data: %s", str(e))
        send_alert_email("ETL Load Error", f"Data load failed: {str(e)}")



def generate_compliance_report():
    """Generates a compliance report for auditing purposes."""
    try:
        report_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "etl_version": open("etl_config_version.txt").read().strip(),
            "data_loaded": True
        }
        with open(COMPLIANCE_REPORT, "w") as f:
            json.dump(report_data, f, indent=4)
        logging.info("Compliance report generated successfully.")
    except Exception as e:
        logging.error("Error in generating compliance report: %s", str(e))

def archive_processed_file(file_path):
    """Move processed file to archive (Restartability)."""
    try:
        if not os.path.exists(ARCHIVE_PATH):
            os.makedirs(ARCHIVE_PATH)
        new_file_path = os.path.join(ARCHIVE_PATH, f"processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        os.rename(file_path, new_file_path)
        logging.info("File archived successfully: %s", new_file_path)
    except Exception as e:
        logging.error("Error in archiving file: %s", str(e))
def get_source_count():
    """Retrieve the total number of records from the source file."""
    try:
        df = pd.read_csv(SOURCE_FILE_PATH)  # Adjust for different sources (DB, API, etc.)
        return len(df)
    except Exception as e:
        logging.error("Error getting source count: %s", str(e))
        return None

def get_target_count(table_name="customers"):
    """Retrieve the total number of records from the target database."""
    try:
        engine = create_engine(DATABASE_URL)
        query = f"SELECT COUNT(*) FROM {table_name};"
        result = pd.read_sql(query, engine)
        return result.iloc[0, 0]
    except Exception as e:
        logging.error("Error getting target count: %s", str(e))
        return None

def reconcile_data():
    """Compare source and target record counts to ensure reconciliation."""
    source_count = get_source_count()
    target_count = get_target_count()

    if source_count is None or target_count is None:
        logging.warning("Skipping reconciliation due to missing data.")
        return

    if source_count == target_count:
        logging.info("✅ Reconciliation successful: Source and target counts match (%d records).", source_count)
    else:
        discrepancy = source_count - target_count
        logging.warning("⚠️ Reconciliation failed: Source (%d) and Target (%d) counts do not match. Discrepancy: %d", 
                        source_count, target_count, discrepancy)
        send_alert_email("ETL Reconciliation Issue", 
                         f"Discrepancy detected! Source: {source_count}, Target: {target_count}, Difference: {discrepancy}")

def etl_pipeline():
    """Complete ETL Process."""
    logging.info("Starting ETL pipeline...")

    if not requires_admin_approval():
        logging.error("ETL job modification requires approval. Process aborted.")
        return

    df = extract_data(DATA_SOURCE)
    if df is None:
        logging.error("Extraction failed. ETL process terminated.")
        return

    df = validate_data(df)
    df = transform_data(df)
    if df is None:
        logging.error("Transformation failed. ETL process terminated.")
        return

    load_data(df)
    archive_processed_file(DATA_SOURCE)
    generate_compliance_report()

    logging.info("ETL pipeline completed successfully.")

if __name__ == "__main__":
    etl_pipeline()
