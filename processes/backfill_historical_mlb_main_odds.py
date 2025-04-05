import pandas as pd 
from tqdm import tqdm
from utils.odds_api_accessor import OddsAccessor, flatten_odds_data
from database_config import current_config, DB_URI
from datetime import datetime, timedelta
import pytz
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

TIMEZONE = pytz.timezone('America/New_York')
oddsapi = OddsAccessor(current_config.get('_20K_API_KEY'))
date_range_list = pd.date_range(start='2021-01-01', end='2025-03-30')
engine = create_engine(DB_URI)

mlb_events = pd.read_sql_query("SELECT DISTINCT event_id, commence_time FROM mlb_events LIMIT 5", engine)

for index, row in mlb_events.iterrows():
    event_id = row['event_id']
    commence_time = (pd.to_datetime(row['commence_time'])
                      .tz_localize(TIMEZONE)  # First specify the original timezone
                      .tz_convert(pytz.UTC)   # Then convert to UTC
                      .strftime("%Y-%m-%dT%H:%M:%SZ"))

    # Get the closest snapshot to game time
    event_odds = oddsapi.get_historical_event_odds(
        sport='baseball_mlb', 
        event_id=event_id, 
        regions='us,eu', 
        markets=','.join(['totals', 'spreads', 'h2h']), 
        date=commence_time
    )

    if not event_odds.get('data'):
        continue

    df = flatten_odds_data(event_odds['data'])

    # Get the previous 9 hours snapshots, 3 hours apart

    for hours_before in [3, 6, 9]:
        snapshot_time = pd.to_datetime(commence_time) - pd.Timedelta(hours=hours_before)
        snapshot_time_str = snapshot_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Get the snapshot
        snapshot_odds = oddsapi.get_historical_odds(
            sport='baseball_mlb',
            regions='us,eu',
            markets=','.join(['totals', 'spreads', 'h2h']),
            date=snapshot_time_str
        )

        if not snapshot_odds.get('data'):
            continue

        snapshot_df = flatten_odds_data(snapshot_odds['data'])

        # Merge the snapshot with the main odds
        df = pd.concat([df, snapshot_df])

    # Push to the database
    try:
        df.to_sql('test_hist_mlb_main_odds', engine, if_exists='append', index=False)
    except IntegrityError:
        # Handle duplicates by inserting records one by one
        for _, odds_row in df.iterrows():
            try:
                pd.DataFrame([odds_row]).to_sql('test_hist_mlb_main_odds', engine, if_exists='append', index=False)
            except IntegrityError:
                continue
