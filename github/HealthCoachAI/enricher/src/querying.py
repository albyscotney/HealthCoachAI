from influxdb_client_3 import InfluxDBClient3
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv


def setup_connection():
    """
    Establishes a connection to an InfluxDB v3 instance using environment variables.
    """
    # load_dotenv() 
    # For InfluxDB v3, you typically need host, token, org, and database
    host = os.getenv("INFLUXDB_HOST", 'influxdb')
    token = os.getenv("INFLUXDB_V3_ACCESS_TOKEN")
    org = os.getenv("INFLUXDB3_ORG", 'HealthCoahAI')
    database = os.getenv("INFLUXDB_DATABASE", './garminconnect-tokens:/home/appuser/.garminconnect') # Database is the v3 term for bucket

    print(f"--- Attempting Connection ---")
    print(f"Host: {host}")
    print(f"Org: {org}")
    print(f"Database: {database}")
    print(f"Token: {'Set' if token else 'Not Set'}") # Avoid printing the token itself
    print(f"-----------------------------")

    # Check for missing required variables
    if not all([host, token, org, database]):
        print("🛑 Error: Missing one or more required environment variables.")
        print("Please set INFLUXDB_HOST, INFLUXDB_TOKEN, INFLUXDB_ORG, and INFLUXDB_DATABASE.")
        return None

    try:
        # Use the InfluxDBClient3 for v3-specific features
        # The client will raise an exception on connection failure (e.g., bad auth, host not found)
        client = InfluxDBClient3(host=host, token=token, org=org, database=database)
        print(f"✅ Successfully connected to InfluxDB at {host}")
        return client
    except Exception as e:
        print(f"❌ Error connecting to InfluxDB: {e}")
        return None


def query_garmin(client, query_string):
    """
    Executes an InfluxQL query and returns the result as a pandas DataFrame.
    """
    try:
        # Execute the query
        result = client.query(query_string)

        # Get the points from the result and convert to a DataFrame
        points = list(result.get_points())
        if not points:
            print(f"Warning: No data returned for query: {query_string}")
            return pd.DataFrame()

        df = pd.DataFrame(points).set_index('time')
        return df
    except Exception as e:
        print(f"Error querying InfluxDB: {e}")
        return pd.DataFrame()


def query_garmin_with_age_and_sex(client, query_string, sex):
    df = query_garmin(client, query_string)

    df.index = pd.to_datetime(df.index).normalize()

    query_age = """
        SELECT "chronologicalAge" as age FROM "FitnessAge"
        """

    age_df = query_garmin(client, query_age)

    age_df.index = pd.to_datetime(age_df.index)
    age_df = age_df.resample('D').last().ffill()

    age_df['sex'] = sex
    return age_df.join(df, how='left')


def fill_nulls(df):
    return (df.fillna(method='ffill')).fillna(method='bfill')