from influxdb_client_3 import InfluxDBClient3
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv


def setup_connection():
    """
    Establishes a connection to an InfluxDB v3 instance using environment variables.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to the project root and then to the .env.influx file
    # Adjust the number of '..' to match your folder structure.
    # In our example, we go up two levels from the script's location.
    dotenv_path = os.path.join(script_dir, '..', '..', '.env.influx')

    # Load the .env file from the specified path
    load_dotenv(dotenv_path=dotenv_path)    

    port = os.getenv("INFLUXDB_PORT") # Should be '8181'
    token = os.getenv("INFLUXDB_V3_ACCESS_TOKEN")
    database = os.getenv("INFLUXDB_DATABASE") # Database is the v3 term for bucket
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


def query_garmin(client, query_string):
    """
    Executes an InfluxQL query and returns the result as a pandas DataFrame.
    """
    try:
        # Execute the query
        result = client.query(query_string, mode="pandas")

        df = pd.DataFrame(result).set_index('time')
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