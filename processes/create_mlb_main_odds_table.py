from sqlalchemy import create_engine, text, MetaData, Table, Column, String, DateTime, Date, Integer, Float,ForeignKey, UniqueConstraint
from sqlalchemy.dialects.mysql import VARCHAR, DATETIME, DATE
from database_config import DB_URI

def create_mlb_main_odds_table(engine):
    metadata = MetaData()
    mlb_main_odds = Table(
        'hist_mlb_main_odds',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('timestamp', DATETIME, nullable=False),
        Column('previous_timestamp', DATETIME, nullable=False),
        Column('next_timestamp', DATETIME, nullable=False),
        Column('event_id', VARCHAR(36), nullable=False),
        Column('sport_key', VARCHAR(50), nullable=False),
        Column('sport_title', VARCHAR(50), nullable=False),
        Column('commence_time', DATETIME, nullable=False),
        Column('home_team', VARCHAR(50), nullable=False),
        Column('away_team', VARCHAR(50), nullable=False),
        Column('bookmaker_key', VARCHAR(50), nullable=False),
        Column('bookmaker_title', VARCHAR(50), nullable=False),
        Column('bookmaker_last_update', DATETIME, nullable=False),
        Column('market_key', VARCHAR(50), nullable=False),
        Column('market_last_update', DATETIME, nullable=False),
        Column('outcome_name', VARCHAR(50), nullable=False),
        Column('outcome_price', Float, nullable=False),
        Column('outcome_point', Float, nullable=False)
    )
    metadata.create_all(engine)

if __name__ == "__main__":
    engine = create_engine(DB_URI)
    create_mlb_main_odds_table(engine)