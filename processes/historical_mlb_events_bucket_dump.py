import polars as pl
import os
import math
import sys
from dotenv import load_dotenv, find_dotenv
import b2sdk.v2 as b2
import pandas as pd 
from tqdm import tqdm
from utils.odds_api_accessor import OddsAccessor
from datetime import datetime
import pytz
from common_utils.upload_parquet_s3_backblaze import saved_chunked_parquet_b2_s3

# Load environment variables from .env file
load_dotenv(find_dotenv())

TIMEZONE = pytz.timezone('America/New_York')
oddsapi = OddsAccessor(os.getenv('_20K_API_KEY'))
date_range_list = pd.date_range(start='2025-04-01', end='2025-04-10')


# Main script execution
if __name__ == "__main__":
    # Get bucket name from environment or allow override
    bucket_name = "mlb-events"
    prefix = "parquet_chunks"
    target_size_gb = 1
    data_to_upload = []
    
    # Check if we have required env vars before proceeding
    if not all([os.getenv("B2_KEY_ID"), os.getenv("B2_APP_KEY")]):
        print("ERROR: Missing required B2 credentials. Please check your .env file.")
        sys.exit(1)
    
    if not bucket_name:
        print("WARNING: B2_BUCKET_NAME not set in environment variables.")
        bucket_name = input("Please enter the B2 bucket name: ")

    key_id = os.getenv('B2_KEY_ID')
    app_key = os.getenv('B2_APP_KEY')

    # Get data
    for date in tqdm(date_range_list):
        try:
            commence_after = datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=TIMEZONE).astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            commence_before = datetime(date.year, date.month, date.day, 23, 59, 59, tzinfo=TIMEZONE).astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            hist_events = oddsapi.get_historical_events('baseball_mlb', date=commence_before, commence_after=commence_after, commence_before=commence_before)
            
            # Check if there's data to process
            if not hist_events.get('data'):
                print(f"No data for {date}")
                continue
                
            df = pd.DataFrame(hist_events['data'])
            df.rename(columns={'id': 'event_id'}, inplace=True)
            df['commence_time'] = pd.to_datetime(df['commence_time']) 
            df['commence_time_ny'] = df['commence_time'].dt.tz_convert('America/New_York')
            df['commence_date'] = df['commence_time'].dt.date
            data_to_upload.append(df)
            
        except Exception as e:
            print(f"Error processing {date}: {e}")
            continue

    # Concatenate all dataframes and save to parquet
    final_df = pd.concat(data_to_upload, ignore_index=True)
    final_df.drop_duplicates(subset=['event_id'], keep='last', inplace=True)
    saved_chunked_parquet_b2_s3(key_id, app_key, final_df, bucket_name, prefix, target_size_gb=1)
