import duckdb 
import pandas as pd 
from tqdm import tqdm
from utils.odds_api_accessor import OddsAccessor, flatten_odds_data, flatten_mlb_historical_event_odds
from dotenv import load_dotenv, find_dotenv
import os
import pytz
from common_utils.upload_parquet_s3_backblaze import saved_chunked_parquet_b2_s3

load_dotenv(find_dotenv())

TIMEZONE = pytz.timezone('America/New_York')
oddsapi = OddsAccessor(os.getenv('_20K_API_KEY'))
date_range_list = pd.date_range(start='2025-04-01', end='2025-04-10')

if __name__ == "__main__":
    key_id = os.getenv('B2_KEY_ID')
    app_key = os.getenv('B2_APP_KEY')
    endpoint = os.getenv('B2_S3_ENDPOINT_URL', "s3.us-east-005.backblazeb2.com")
    bucket_name = "mlb-events-odds"
    prefix = "parquet_chunks"
    target_size_gb = 1
    data_to_upload = []

    conn = duckdb.connect(database="mlb.duckdb")
    mlb_events = conn.execute("SELECT * FROM mlb_events").fetch_df()

    for index, row in mlb_events.iterrows():
        try:
            event_id = row['event_id']
            commence_time = row['commence_time']

            time_to_pull = pd.to_datetime(commence_time) - pd.Timedelta(hours=1)
            time_to_pull_str = time_to_pull.tz_localize(TIMEZONE).tz_convert(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Get the closest snapshot to game time
            event_odds = oddsapi.get_historical_event_odds(
                sport='baseball_mlb', 
                event_id=event_id, 
                regions='us,eu', 
                markets=','.join(['batter_total_bases','batter_home_runs']), 
                date=time_to_pull_str
            )

            if event_odds is None:
                continue

            df = flatten_mlb_historical_event_odds(event_odds)
            data_to_upload.append(df)

        except Exception as e:
            print(f"Error processing {row}: {e}")
            continue

    # Concatenate all dataframes and save to parquet
    final_df = pd.concat(data_to_upload, ignore_index=True)
    final_df.drop_duplicates(inplace=True)
    saved_chunked_parquet_b2_s3(key_id, app_key, final_df, bucket_name, prefix, target_size_gb=1)
