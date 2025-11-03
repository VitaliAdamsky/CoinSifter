# database/__init__.py
"""
Database module for CoinSifter.
Provides clean API for database operations.
"""

from .connection import get_db_connection, get_db_connection_context
from .schema import setup_database_tables
from .coins import (
    save_coins_to_db,
    fetch_all_coins_from_db,
    clear_existing_data,
    fetch_last_analysis_timestamp
)
from .logs import (
    create_log_entry,
    update_log_status,
    get_log_by_id,
    fetch_logs_from_db,
    clear_logs_in_db,
    clear_logs  # Alias for backward compatibility
)

__all__ = [
    # Connection
    'get_db_connection',
    'get_db_connection_context',
    
    # Schema
    'setup_database_tables',
    
    # Coins
    'save_coins_to_db',
    'fetch_all_coins_from_db',
    'clear_existing_data',
    'fetch_last_analysis_timestamp',
    
    # Logs
    'create_log_entry',
    'update_log_status',
    'get_log_by_id',
    'fetch_logs_from_db',
    'clear_logs_in_db',
    'clear_logs',
]