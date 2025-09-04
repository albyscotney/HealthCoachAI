import sys
import os
from influxdb import InfluxDBClient
import pandas as pd
import numpy as np
from scipy.interpolate import griddata, NearestNDInterpolator
from datetime import datetime
from dotenv import load_dotenv

current_dir = os.getcwd()

project_root = os.path.join(current_dir, '..', '..')

if project_root not in sys.path:
    sys.path.append(project_root)

from enricher.src.utils.querying import setup_connection, fill_nulls, query_garmin

client = setup_connection()

env_path = os.path.expanduser(project_root + '/.env.user')
load_dotenv(dotenv_path=env_path)
sex = os.getenv('GENDER')
USER = os.getenv('GARMINCONNECT_EMAIL')

def bilateral_interpolation(coordinates_df: pd.DataFrame, new_points_df: pd.DataFrame) -> pd.DataFrame:
    """
    Interpolates heart rate (hr) for new points, ensuring required columns
    are converted to float for accurate calculation.

    Args:
        coordinates_df: A pandas DataFrame with 'age', 'vo2', and 'hr' columns
                        representing the known grid points.
        new_points_df: A pandas DataFrame with 'age' and 'vo2' columns to
                       which the interpolated 'hr' column will be added.

    Returns:
        A new pandas DataFrame with the 'hr' column added.
    """
    result_df = new_points_df.copy()


    for col in ['age', 'vo2']:
        if col in result_df.columns:
            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')

    coordinates_df_copy = coordinates_df.copy()
    coordinates_df_copy['age'] = pd.to_numeric(coordinates_df_copy['age'], errors='coerce')
    coordinates_df_copy['vo2'] = pd.to_numeric(coordinates_df_copy['vo2'], errors='coerce')
    coordinates_df_copy['hr'] = pd.to_numeric(coordinates_df_copy['hr'], errors='coerce')

    result_df.dropna(subset=['age', 'vo2'], inplace=True)
    coordinates_df_copy.dropna(subset=['age', 'vo2', 'hr'], inplace=True)

    points = coordinates_df_copy[['age', 'vo2']].values
    values = coordinates_df_copy['hr'].values

    grid_points = result_df[['age', 'vo2']].values

    interpolated_hr_linear = griddata(points, values, grid_points, method='linear')

    nan_indices = np.isnan(interpolated_hr_linear)
    
    if np.any(nan_indices):
        nearest_interp = NearestNDInterpolator(points, values)
        interpolated_hr_nearest = nearest_interp(grid_points[nan_indices])

        interpolated_hr_linear[nan_indices] = interpolated_hr_nearest

    result_df['hr'] = interpolated_hr_linear

    return result_df


def run_vo2_enricher()
  query_vo2 = """
  SELECT
    F.time,
    V."VO2_max_value" as vo2,
    F."chronologicalAge" as age
  FROM "VO2_Max" AS V
  RIGHT JOIN "FitnessAge" AS F USING(time)
  ORDER BY time ASC
  """

  df_vo2 = fill_nulls(query_garmin(client=client, query_string=query_vo2))
  baseline = pd.read_csv('.../data/vo2_hr.csv')
  interpolated_df = bilateral_interpolation(baseline.loc[baseline['sex'] == sex], df_vo2)
  interpolated_df['email'] = USER

  try:
    client.write(
        record=interpolated_df,
        data_frame_measurement_name="VO2Daily",
        data_frame_tag_columns='email',
        database="Enricher" # <-- OVERRIDE HERE
      )
    print(f"Successfully wrote data for user {USER}.")
  except Exception as e:
    print(f"Failed to write to InfluxDB: {e}")
if __name__ == "__main__":
    run_vo2_enricher()