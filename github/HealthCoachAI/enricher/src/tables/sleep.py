from influxdb_client import InfluxDBClient
import pandas as pd
import numpy as np
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
from src.utils.querying import fill_nulls, query_garmin

def get_sleep_wake_times(df:pd.DataFrame) -> pd.DataFrame:
    df = df.sort_index()
    df = df.reset_index()
    df = df.rename(columns={'time': 'WakeTime'})
    df['WakeTime'] = pd.to_datetime(df['WakeTime'])
    df['SleepTime'] = df['WakeTime'] - pd.to_timedelta(df['sleepTimeSeconds'], unit='s')
    df['SleepDate'] = df['WakeTime'].dt.date - pd.Timedelta(days=1)
    df['SleepDuration'] = df['sleepTimeSeconds'] / 3600
    return df[['SleepDate', 'SleepTime', 'WakeTime', 'SleepDuration']]

def calculate_whoop_consistency(df_input):
    """
    Calculates a WHOOP-style consistency metric for a given DataFrame slice.
    
    This ROBUST version ignores rows with null SleepTime/WakeTime data
    within the input window.
    """
    # 1. --- KEY CHANGE ---
    # Drop any rows that have null sleep or wake times from the input window.
    df = df_input.copy().dropna(subset=['SleepTime', 'WakeTime'])

    # 2. Check if there are still at least 2 valid days left to compare.
    if len(df) < 2:
        return np.nan

    # --- The rest of the function is the same ---
    df['bedtime_minutes'] = df['SleepTime'].dt.hour * 60 + df['SleepTime'].dt.minute
    df['wake_up_minutes'] = df['WakeTime'].dt.hour * 60 + df['WakeTime'].dt.minute

    sleep_patterns = []
    for index, row in df.iterrows():
        daily_pattern = [0] * (24 * 60)
        bed_time = int(row['bedtime_minutes'])
        wake_time = int(row['wake_up_minutes'])

        if wake_time < bed_time: # Handle wrap-around midnight
            for minute in range(bed_time, 24 * 60):
                daily_pattern[minute] = 1
            for minute in range(0, wake_time):
                daily_pattern[minute] = 1
        else:
            for minute in range(bed_time, wake_time):
                daily_pattern[minute] = 1
        sleep_patterns.append(daily_pattern)

    num_days = len(sleep_patterns)
    if num_days < 2:
        return np.nan

    consistent_minutes = 0
    total_minutes = 24 * 60

    for minute_idx in range(total_minutes):
        states_at_minute = [pattern[minute_idx] for pattern in sleep_patterns]
        if len(set(states_at_minute)) == 1:
            consistent_minutes += 1

    consistency_score = (consistent_minutes / total_minutes) * 100
    return consistency_score

def interpolate_df(x_new, baseline_df, x_col, y_col):
    """
    Performs efficient linear interpolation on a DataFrame.

    Args:
        x_new (pd.Series or np.ndarray): A series of x-values for which to
                                         interpolate y-values.
        baseline_df (pd.DataFrame): A DataFrame with the known data points
                                    for interpolation.
        x_col (str): The name of the column in baseline_df representing the
                     x-coordinates (independent variable).
        y_col (str): The name of the column in baseline_df representing the
                     y-coordinates (dependent variable).
    """
    baseline_sorted = baseline_df.sort_values(by=x_col)
    
    interpolated_values = np.interp(
        x_new,
        baseline_sorted[x_col],
        baseline_sorted[y_col]
    )
    
    return interpolated_values

def get_enriched_sleep_data(df: pd.DataFrame, USER) -> pd.DataFrame:
    """
    Enriches sleep data with a rolling 4-day WHOOP-style consistency score (sci)
    and other interpolated HR metrics.
    """
    df_final = get_sleep_wake_times(df)

    whoop_consistency_scores = []
    for i in range(len(df_final)):
        start_index = max(0, i - 3)
        
        window_df = df_final.iloc[start_index:i+1]
        
        consistency_score = calculate_whoop_consistency(window_df)
        whoop_consistency_scores.append(consistency_score)

    df_final['SleepConsistencyIndex'] = whoop_consistency_scores


    baseline_data = pd.read_csv('enricher/data/sleep_hr.csv')

    df_final['SleepConsistencyIndexHR'] = interpolate_df(
        x_new=df_final['SleepConsistencyIndex'],
        baseline_df=baseline_data.query("Category == 'sci'"),
        x_col='Median',
        y_col='HR'
    )

    df_final['DurationHR'] = interpolate_df(
        x_new=df_final['SleepDuration'],
        baseline_df=baseline_data.query("Category == 'dur'"),
        x_col='Median',
        y_col='HR'
    )

    df_final['SleepHR'] = df_final['SleepConsistencyIndexHR'].fillna(1) * df_final['DurationHR'].fillna(1)
    df_final['time'] = pd.to_datetime(df_final['SleepDate']).dt.tz_localize('UTC')
    df_final = df_final.set_index('time')
    df_final = df_final.drop(columns=['SleepDate'])
    df_final['email'] = USER

    return df_final[['SleepDuration', 'SleepConsistencyIndex', 'SleepConsistencyIndexHR', 'DurationHR', 'SleepHR', 'email']]

def run_sleep_enricher(client, USER, sex):
    query_sleep = """
    SELECT "sleepTimeSeconds", "time" FROM "SleepSummary" ORDER BY time ASC
    """
    print("Getting Data")
    df = fill_nulls(query_garmin(client, query_sleep))

    print("Enriching Data")
    enriched_df = get_enriched_sleep_data(df, USER)

    print("Writing Data")
    try:
        client.write(
            record=enriched_df,
            data_frame_measurement_name="SleepDaily3",
            data_frame_tag_columns='email',
            database="Enricher"
        )
        print(f"Successfully wrote data for user {USER}.")
    except Exception as e:
        print(f"Failed to write to InfluxDB: {e}")
if __name__ == "__main__":
    run_sleep_enricher()