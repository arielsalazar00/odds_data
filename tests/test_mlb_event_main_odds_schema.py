import pandas as pd 
from tqdm import tqdm
from utils.odds_api_accessor import OddsAccessor
from utils.odds_api_constants import BETTING_MARKETS
from database_config import current_config, DB_URI
from datetime import datetime
import pytz
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import random
import json
import numpy as np

TIMEZONE = pytz.timezone('America/New_York')
oddsapi = OddsAccessor(current_config.get('_20K_API_KEY'))
date_range_list = pd.date_range(start='2021-01-01', end='2025-03-30')
engine = create_engine(DB_URI)

date_random = np.random.choice(date_range_list, 1)

for date_ in tqdm(date_random):
    date = pd.to_datetime(date_)
    datetime_stamp = datetime(date.year, date.month, date.day, 11, 0, 0, tzinfo=TIMEZONE).astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    hist_results = oddsapi.get_historical_odds(sport='baseball_mlb', regions='us,eu', markets=','.join(['totals', 'spreads', 'h2h']), date=datetime_stamp)

    print(hist_results)

    if not hist_results.get('data'):
        print(f"No data for {date_}")
        continue

    with open('hist_main_odds_results.json', 'w') as f:
        json.dump(hist_results, f)

