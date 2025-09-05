import sys
import time
import os
from influxdb import InfluxDBClient
import pandas as pd
import numpy as np
from scipy.interpolate import griddata, NearestNDInterpolator
from datetime import datetime
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3

from src.tables.sleep import run_sleep_enricher
from src.tables.vo2 import run_vo2_enricher
def setup_connection(fetcher_database=True, dotenv_path:str=''):
    """
    Establishes a connection to an InfluxDB v3 instance using environment variables.
    """
    # Load the .env file from the specified path
    load_dotenv(dotenv_path=dotenv_path)    

    port = os.getenv("INFLUXDB_PORT") # Should be '8181'
    token = os.getenv("INFLUXDB_V3_ACCESS_TOKEN")
    if fetcher_database is True:
        database = os.getenv("INFLUXDB_DATABASE") # Database is the v3 term for bucket
    else:
        database = os.getenv("INFLUXDB_ENRICHER")
    full_host_url = f"http://localhost:{port}"


    print(f"--- Attempting Connection ---")
    print(f"Port: {port}")
    print(f"Database: {database}")
    print(f"Token: {'Set' if token else 'Not Set'}") # Avoid printing the token itself
    print(f"-----------------------------")

    # Check for missing required variables
    if not all([port, token, database]):
        print("🛑 Error: Missing one or more required environment variables.")
        print("Please set INFLUXDB_TOKEN, INFLUXDB_PORT and INFLUXDB_DATABASE.")
        return None

    try:
        # Use the InfluxDBClient3 for v3-specific features
        # The client will raise an exception on connection failure (e.g., bad auth, host not found)
        client = InfluxDBClient3(host=full_host_url, token=token, database=database)
        print(f"✅ Successfully connected to InfluxDB at {full_host_url}")
        return client
    except Exception as e:
        print(f"❌ Error connecting to InfluxDB: {e}")
        return None

client = setup_connection(fetcher_database=True, dotenv_path= './envs/.env.influx')

env_path = os.path.expanduser('./envs/.env.user')
print(env_path)
load_dotenv(dotenv_path=env_path)
sex = os.getenv('GENDER')
USER = os.getenv('GARMINCONNECT_EMAIL')
run_sleep_enricher(client, USER, sex)
run_vo2_enricher(client, USER, sex)
time.sleep(60*15)