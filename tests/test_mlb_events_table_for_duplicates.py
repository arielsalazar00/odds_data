import pandas as pd 
from database_config import DB_URI, current_config
from sqlalchemy import create_engine

engine = create_engine(DB_URI)

df = pd.read_sql_table('mlb_events', engine)

print(df.duplicated(subset=['event_id']).sum())