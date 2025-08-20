import os
import time
import logging
import pandas as pd
import schedule
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

# --- Configuration ---
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "")
RAW_BUCKET = os.getenv("INFLUXDB_RAW_BUCKET", "GarminStatsRaw")
ENRICHED_BUCKET = os.getenv("INFLUXDB_ENRICHED_BUCKET", "GarminStatsEnriched")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- InfluxDB Client ---
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()
write_api = client.write_api(write_options=SYNCHRONOUS)

def create_bucket_if_not_exists(bucket_name, org):
    """Checks if a bucket exists and creates it if it doesn't."""
    try:
        buckets_api = client.buckets_api()
        # InfluxDB 2.x/3.x returns a list, so we check if our bucket is in there
        if not any(b.name == bucket_name for b in buckets_api.find_buckets().buckets):
            logging.info(f"Bucket '{bucket_name}' not found, creating it...")
            buckets_api.create_bucket(bucket_name=bucket_name, org=org)
            logging.info(f"Bucket '{bucket_name}' created successfully.")
    except ApiException as e:
        logging.error(f"Error managing buckets: {e}")
        # Handle cases where the token might not have permission to list/create buckets
        if e.status == 403:
            logging.error("Permission denied. Ensure the token has org-level permissions to create buckets.")


def calculate_rhr_trend():
    """Calculates the 7-day rolling average for Resting Heart Rate (RHR)."""
    logging.info("Running: Calculating RHR Trend...")
    try:
        flux_query = f'''
        from(bucket: "{RAW_BUCKET}")
          |> range(start: -30d)
          |> filter(fn: (r) => r["_measurement"] == "DailyStats" and r["_field"] == "restingHeartRate")
          |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
        '''
        df = query_api.query_data_frame(query=flux_query, org=INFLUXDB_ORG)

        if len(df) < 2:
            logging.warning(f"Not enough RHR data for trend (found {len(df)} points).")
            return

        df.rename(columns={'_time': 'time', '_value': 'rhr'}, inplace=True)
        df.set_index('time', inplace=True)

        # --- Enrichment Calculation ---
        df['rhr_7day_avg'] = df['rhr'].rolling(window='7d', min_periods=1).mean()
        
        enriched_df = df[['rhr_7day_avg']].dropna().reset_index()

        # --- Write data back to InfluxDB ---
        if not enriched_df.empty:
            write_api.write(
                bucket=ENRICHED_BUCKET,
                record=enriched_df,
                data_frame_measurement_name="wellness_trends",
                data_frame_timestamp_column="time"
            )
            logging.info(f"Successfully wrote {len(enriched_df)} RHR trend points.")

    except Exception as e:
        logging.error(f"Error calculating RHR trend: {e}")

if __name__ == "__main__":
    logging.info("Starting data enrichment service...")
    time.sleep(15) # Wait for InfluxDB to be fully ready
    
    # Ensure the target bucket for enriched data exists
    create_bucket_if_not_exists(ENRICHED_BUCKET, INFLUXDB_ORG)
    
    # Run once at startup
    calculate_rhr_trend()
    
    schedule.every(1).hour.do(calculate_rhr_trend)
    
    while True:
        schedule.run_pending()
        time.sleep(60)