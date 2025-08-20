from influxdb import InfluxDBClient
import pandas as pd
from datetime import datetime

def setup_connection():
    # InfluxDB connection details from your compose file
    host = 'localhost' 
    port = 8086
    user = 'influxdb_user'
    password = 'influxdb_secret_password'
    dbname = 'GarminStats'
    sex = 'M'

    # Initialize the InfluxDB client
    client = InfluxDBClient(host=host, port=port, username=user, password=password, database=dbname)

    # Test the connection
    try:
        databases = client.get_list_database()
        print(f"Successfully connected to InfluxDB. Available databases: {databases}")
    except Exception as e:
        print(f"Error connecting to InfluxDB: {e}")

    return client

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