from sqlalchemy import create_engine, text, MetaData, Table, Column, String, DateTime, Date, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.mysql import VARCHAR, DATETIME, DATE
from database_config import DB_URI

def create_mlb_events_table(engine):
    metadata = MetaData()
    mlb_events = Table(
        'mlb_events',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('event_id', VARCHAR(36), nullable=False),
        Column('sport_key', VARCHAR(50), nullable=False),
        Column('sport_title', VARCHAR(50), nullable=False),
        Column('commence_time', DATETIME, nullable=False),
        Column('home_team', VARCHAR(100), nullable=False),
        Column('away_team', VARCHAR(100), nullable=False),
        Column('commence_date', DATE, nullable=False),
        Column('commence_time_ny', DATETIME, nullable=False),
        Column('created_at', DATETIME, server_default=text('CURRENT_TIMESTAMP')),
        Column('updated_at', DATETIME, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')),
        UniqueConstraint('event_id', name='unique_event_id')  # Add this line for the unique constraint
    )
    metadata.create_all(engine)

if __name__ == "__main__":
    engine = create_engine(DB_URI)
    create_mlb_events_table(engine)