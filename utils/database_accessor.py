from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import pandas as pd
from typing import Optional, List, Dict, Any
from database_config import DB_URI

class DatabaseAccessor:
    def __init__(self, db_uri: str, pool_size: int = 5, max_overflow: int = 10):
        """
        Initialize database connection pool and session factory.
        
        Args:
            db_uri: Database connection string
            pool_size: Number of connections to keep in the pool
            max_overflow: Maximum number of connections to allow above pool_size
        """
        self.engine = create_engine(
            db_uri,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=30,
            pool_recycle=3600  # Recycle connections after 1 hour
        )
        session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(session_factory)

