import pandas as pd 
from tqdm import tqdm
from utils.odds_api_accessor import OddsAccessor
from database_config import current_config, DB_URI
from datetime import datetime
import pytz
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

TIMEZONE = pytz.timezone('America/New_York')
oddsapi = OddsAccessor(current_config.get('_20K_API_KEY'))
date_range_list = pd.date_range(start='2021-01-01', end='2025-03-30')
engine = create_engine(DB_URI)

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
        
        try:
            # Try to insert all records at once
            df.to_sql('mlb_events', engine, if_exists='append', index=False)
            #print(f"Added {len(df)} records for {date}")
        except IntegrityError:
            # If that fails, insert records one by one, skipping duplicates
            success_count = 0
            for _, row in df.iterrows():
                try:
                    pd.DataFrame([row]).to_sql('mlb_events', engine, if_exists='append', index=False)
                    success_count += 1
                except IntegrityError:
                    continue
            print(f"Added {success_count} new records for {date}")
            
    except Exception as e:
        print(f"Error on {date}: {e}")
        continue