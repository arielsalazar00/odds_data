import os
import duckdb
import b2sdk.v2 as b2
from dotenv import load_dotenv, find_dotenv
from common_utils.create_duckdb_from_backblaze_bucket import create_duckdb_with_b2_data


if __name__ == "__main__":
    bucket_name = "mlb-events"
    prefix = "parquet_chunks"
    db_path = "mlb.duckdb"

    table_name = "mlb_events"
    
    key_id = os.getenv('B2_KEY_ID')
    app_key = os.getenv('B2_APP_KEY')
    endpoint = os.getenv('B2_S3_ENDPOINT_URL', "s3.us-east-005.backblazeb2.com")

    schema = """
    CREATE OR REPLACE TABLE mlb_events (
        event_id VARCHAR(36),
        sport_key VARCHAR(50),
        sport_title VARCHAR(50),
        commence_time TIMESTAMP,
        home_team VARCHAR(100),
        away_team VARCHAR(100),
        commence_date DATE,
        commence_time_ny TIMESTAMP,
        index_number INTEGER
    );
    """

    db_file = create_duckdb_with_b2_data(key_id, app_key, endpoint, bucket_name, prefix, db_path, table_name, schema=schema)
    print(f"\nYou can now use the database with: duckdb.connect('{db_file}')")
    