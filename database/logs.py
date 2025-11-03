# database/logs.py
"""
CRUD operations for logs (script_run_logs table).
"""

import logging
import datetime
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from .connection import get_db_connection

log = logging.getLogger(__name__)


def create_log_entry(status: str, details: str = "") -> int:
    """
    Creates a new log entry and returns its ID.
    
    Args:
        status: Status of the run (e.g., "Running", "Completed")
        details: Additional details about the run
        
    Returns:
        ID of the created log entry, or None if failed
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            INSERT INTO script_run_logs (status, details, start_time, coins_saved) 
            VALUES (%s, %s, %s, 0) 
            RETURNING id;
        """
        start_time = datetime.datetime.now(datetime.timezone.utc)
        cursor.execute(query, (status, details, start_time))
        
        log_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        
        log.debug(f"Created log entry with ID: {log_id}")
        return log_id
        
    except Exception as e:
        log.error(f"❌ Error creating log entry: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return None
        
    finally:
        if conn:
            conn.close()


def update_log_status(log_id: int, status: str, details: str = "", coins_saved: int = None):
    """
    Updates the status of a log entry.
    
    Args:
        log_id: ID of the log entry to update
        status: New status
        details: Additional details
        coins_saved: Number of coins saved (optional)
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build dynamic query
        set_parts = [
            sql.SQL("status = %s"),
            sql.SQL("details = %s")
        ]
        params = [status, details]
        
        # Add end_time for final statuses
        if status in ("Завершено", "Completed", "Критическая ошибка", "Ошибка", "Error"):
            set_parts.append(sql.SQL("end_time = %s"))
            params.append(datetime.datetime.now(datetime.timezone.utc))
        
        # Add coins_saved if provided
        if coins_saved is not None:
            set_parts.append(sql.SQL("coins_saved = %s"))
            params.append(coins_saved)
        
        params.append(log_id)
        
        query = sql.SQL("UPDATE script_run_logs SET {} WHERE id = %s").format(
            sql.SQL(', ').join(set_parts)
        )
        
        cursor.execute(query, tuple(params))
        conn.commit()
        cursor.close()
        
        log.debug(f"Updated log {log_id}: status='{status}', coins_saved={coins_saved}")
        
    except Exception as e:
        log.error(f"❌ Error updating log {log_id}: {e}")
        if conn:
            conn.rollback()
            
    finally:
        if conn:
            conn.close()


def get_log_by_id(log_id: int) -> dict:
    """
    Retrieves a specific log entry by ID.
    
    Args:
        log_id: ID of the log entry
        
    Returns:
        Dictionary with log data, or None if not found
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("SELECT * FROM script_run_logs WHERE id = %s", (log_id,))
        log_entry = cursor.fetchone()
        cursor.close()
        
        return dict(log_entry) if log_entry else None
        
    except Exception as e:
        log.error(f"❌ Error fetching log by ID {log_id}: {e}")
        return None
        
    finally:
        if conn:
            conn.close()


def fetch_logs_from_db(limit: int = 50) -> list:
    """
    Retrieves the last N log entries.
    
    Args:
        limit: Maximum number of logs to retrieve
        
    Returns:
        List of log dictionaries, ordered by start_time descending
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            "SELECT * FROM script_run_logs ORDER BY start_time DESC LIMIT %s",
            (limit,)
        )
        logs = cursor.fetchall()
        cursor.close()
        
        return [dict(log_entry) for log_entry in logs]
        
    except Exception as e:
        log.error(f"❌ Error fetching logs from database: {e}")
        return []
        
    finally:
        if conn:
            conn.close()


def clear_logs_in_db():
    """
    Clears the script_run_logs table.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("TRUNCATE TABLE script_run_logs RESTART IDENTITY;")
        conn.commit()
        cursor.close()
        
        log.info("✅ Logs table cleared successfully")
        
    except Exception as e:
        log.error(f"❌ Error clearing logs: {e}")
        if conn:
            conn.rollback()
            
    finally:
        if conn:
            conn.close()


# Alias for backward compatibility
clear_logs = clear_logs_in_db