# database/connection.py
"""
Database connection management.
"""

import os
import logging
from contextlib import contextmanager
import psycopg2

log = logging.getLogger(__name__)


def get_db_connection():
    """
    Establishes connection to PostgreSQL database.
    
    Returns:
        psycopg2.connection: Database connection object
        
    Raises:
        ValueError: If DATABASE_URL is not set
        psycopg2.Error: If connection fails
    """
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        log.error("DATABASE_URL environment variable is not set")
        raise ValueError("DATABASE_URL must be set")
    
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        log.error(f"Failed to connect to database: {e}")
        raise


@contextmanager
def get_db_connection_context():
    """
    Context manager for database connections with automatic commit/rollback.
    
    Usage:
        with get_db_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
    
    Yields:
        psycopg2.connection: Database connection with auto-commit/rollback
    """
    conn = None
    try:
        conn = get_db_connection()
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        log.error(f"Database operation failed, rolled back: {e}")
        raise
    finally:
        if conn:
            conn.close()