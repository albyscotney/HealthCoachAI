import pandas as pd
import numpy as np
from scipy.interpolate import griddata, NearestNDInterpolator

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
    sex = new_points_df.iloc[1]['sex']
    result_df = new_points_df.copy()

    result_df = result_df.loc[result_df['sex'] == sex]

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


