# %%
import os
import sys
import time
import dotenv
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from influxdb_client_3 import InfluxDBClient3, InfluxDBError

# Import the list of all enrichment classes from your other file
from enrichments import ALL_ENRICHMENTS

# Load environment variables
dotenv.load_dotenv()
env_override = dotenv.load_dotenv("override-default-vars.env", override=True)
if env_override:
    logging.warning("System ENV variables are overridden with override-default-vars.env")

# %% ### CONFIGURATION ###
INFLUXDB_VERSION = os.getenv("INFLUXDB_VERSION", '3')
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST", 'your.influxdb.hostname')
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", 8181))
INFLUXDB_USERNAME = os.getenv("INFLUXDB_USERNAME", 'influxdb_username')
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD", 'influxdb_access_password')
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", 'GarminStats')
INFLUXDB_V3_ACCESS_TOKEN = os.getenv("INFLUXDB_V3_ACCESS_TOKEN", '')
INFLUXDB_ENDPOINT_IS_HTTP = os.getenv("INFLUXDB_ENDPOINT_IS_HTTP", "true").lower() in ['true', 't', 'yes', '1']

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
CHECK_INTERVAL_SECONDS = int(os.getenv("ENRICH_CHECK_INTERVAL_SECONDS", 300))
INITIAL_LOOKBACK_DAYS = int(os.getenv("ENRICH_INITIAL_LOOKBACK_DAYS", 30))

# %% ### LOGGING SETUP ###
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("EnricherService")

# %% ### INFLUXDB CLIENT INITIALIZATION ###
influxdbclient = None
try:
    protocol = "http" if INFLUXDB_ENDPOINT_IS_HTTP else "https"
    if INFLUXDB_VERSION == '1':
        influxdbclient = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_DATABASE, ssl=not INFLUXDB_ENDPOINT_IS_HTTP, verify_ssl=not INFLUXDB_ENDPOINT_IS_HTTP)
    else:
        influxdbclient = InfluxDBClient3(host=f"{protocol}://{INFLUXDB_HOST}:{INFLUXDB_PORT}", token=INFLUXDB_V3_ACCESS_TOKEN, database=INFLUXDB_DATABASE)
    logger.info(f"Successfully connected to InfluxDB v{INFLUXDB_VERSION} at {INFLUXDB_HOST}.")
except (InfluxDBClientError, InfluxDBError, Exception) as e:
    logger.error(f"Fatal: Unable to connect to InfluxDB. Please check your configuration. Error: {e}")
    sys.exit(1)

# %% ### GENERIC HELPER FUNCTIONS ###

def write_points_to_influxdb(points):
    if not points: return
    try:
        if INFLUXDB_VERSION == '1': influxdbclient.write_points(points)
        else: influxdbclient.write(record=points)
        logger.info(f"Successfully wrote {len(points)} enriched data point(s) to InfluxDB.")
    except (InfluxDBClientError, InfluxDBError) as e:
        logger.error(f"Failed to write points to InfluxDB: {e}")

def get_last_enriched_timestamp(measurement_name):
    try:
        query = f'SELECT * FROM "{measurement_name}" ORDER BY time DESC LIMIT 1'
        if INFLUXDB_VERSION == '1':
            result = list(influxdbclient.query(query).get_points())
            if result: return datetime.strptime(result[0]['time'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        else:
            result = influxdbclient.query(query=query, language="influxql").to_pylist()
            if result: return result[0]['time']
    except Exception:
        return None

def get_new_raw_data(measurement_name, since_timestamp):
    try:
        ts_str = since_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        query = f"SELECT * FROM \"{measurement_name}\" WHERE time > '{ts_str}'"
        if INFLUXDB_VERSION == '1':
            results = list(influxdbclient.query(query).get_points())
            for r in results: r['time'] = datetime.strptime(r['time'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            return results
        else:
            return influxdbclient.query(query=query, language="influxql").to_pylist()
    except (InfluxDBClientError, InfluxDBError) as e:
        logger.error(f"Error querying for new data from '{measurement_name}': {e}")
        return []

# %% ### MAIN EXECUTION ENGINE ###

def main():
    logger.info("Starting Enricher Service Engine...")

    # 1. Initialize and register all active enrichments
    enrichment_registry = defaultdict(list)
    for enrichment_class in ALL_ENRICHMENTS:
        try:
            instance = enrichment_class()
            if not instance.target_measurement or not instance.output_measurement:
                 logger.error(f"Enrichment '{instance.name}' is missing target_measurement or output_measurement. Skipping.")
                 continue
            enrichment_registry[instance.target_measurement].append(instance)
            logger.info(f"-> Registered enrichment '{instance.name}' for target '{instance.target_measurement}'")
        except Exception as e:
            logger.error(f"Failed to initialize enrichment {enrichment_class.__name__}: {e}", exc_info=True)
    
    if not enrichment_registry:
        logger.error("No valid enrichments were loaded. Please check enrichments.py. Exiting.")
        return

    # 2. Start the main processing loop
    while True:
        try:
            # Loop through each *type* of data we need to enrich (e.g., "SleepSummary")
            for target_measurement, enrichers in enrichment_registry.items():
                logger.debug(f"Checking for new data in '{target_measurement}'...")
                output_measurement = enrichers[0].output_measurement # All enrichers for a target must have the same output
                
                # a. Find where we left off for this specific measurement
                last_ts = get_last_enriched_timestamp(output_measurement)
                start_ts = last_ts if last_ts else datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)
                
                # b. Get new data
                new_data = get_new_raw_data(target_measurement, start_ts)

                if not new_data:
                    logger.debug(f"No new data found for '{target_measurement}'.")
                    continue

                logger.info(f"Found {len(new_data)} new record(s) in '{target_measurement}' to process.")
                
                # c. Process each new data point
                points_to_write = []
                for point in new_data:
                    all_new_fields = {}
                    # Apply every registered enrichment to this point
                    for enricher in enrichers:
                        try:
                            new_fields = enricher.enrich(point)
                            if new_fields:
                                all_new_fields.update(new_fields)
                        except Exception as e:
                            logger.error(f"Error running enrichment '{enricher.name}' on point {point.get('time')}: {e}")
                    
                    if all_new_fields:
                        enriched_point = {
                            "measurement": output_measurement,
                            "time": point['time'],
                            "tags": point['tags'],
                            "fields": all_new_fields
                        }
                        points_to_write.append(enriched_point)
                
                # d. Write the batch of new enriched points to InfluxDB
                write_points_to_influxdb(points_to_write)
                
            logger.info(f"All targets checked. Sleeping for {CHECK_INTERVAL_SECONDS} seconds.")
            time.sleep(CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Shutdown signal received. Exiting.")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
            logger.info("Restarting loop after a 60 second delay.")
            time.sleep(60)

if __name__ == "__main__":
    main()