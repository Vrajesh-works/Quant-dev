import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def explore_data():
    
    df = pd.read_csv('data/l1_day.csv')
    
    print("Dataset Info:")
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print("\nFirst few rows:")
    print(df.head())
    
    df['ts_event'] = pd.to_datetime(df['ts_event'])
    df['ts_recv'] = pd.to_datetime(df['ts_recv'])
    
    print("\nTime range:")
    print(f"Start: {df['ts_event'].min()}")
    print(f"End: {df['ts_event'].max()}")
    
    start_time = df['ts_event'].dt.time >= pd.Timestamp('13:36:32').time()
    end_time = df['ts_event'].dt.time <= pd.Timestamp('13:45:14').time()
    filtered_df = df[start_time & end_time]
    
    print(f"\nFiltered dataset shape: {filtered_df.shape}")
    
    print("\nPublisher IDs (venues):")
    print(df['publisher_id'].value_counts())
    
    print("\nAsk price stats:")
    print(df['ask_px_00'].describe())
    
    print("\nAsk size stats:")
    print(df['ask_sz_00'].describe())
    
    return filtered_df

if __name__ == "__main__":
    filtered_data = explore_data()