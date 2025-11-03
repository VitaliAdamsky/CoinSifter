# metrics/ranking.py

import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, RealDictCursor

import config
# --- ИЗМЕНЕНИЕ ---
from database import get_db_connection
# --- КОНЕЦ ИЗМЕНЕНИЯ ---

log = logging.getLogger(__name__)


def update_volume_categories(table_name="monthly_coin_selection", log_prefix=""):
    """
    Calculate and update volume categories (ranks 1-6) based on 24h volume.
    
    Higher rank = higher volume:
    - Category 6: Top 16.7% by volume (highest)
    - Category 5: Next 16.7%
    - Category 4: Next 16.7%
    - Category 3: Next 16.7%
    - Category 2: Next 16.7%
    - Category 1: Bottom 16.7% (lowest)
    
    Args:
        table_name: Database table name
        log_prefix: Prefix for log messages
    """
    log.info(f"{log_prefix}--- Stage 4: Calculating Volume Categories (Ranks 1-6) ---")
    
    conn = None
    try:
        # --- ИЗМЕНЕНИЕ ---
        conn = get_db_connection()
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Fetch volume data
        cursor.execute(f"SELECT full_symbol, volume_24h_usd FROM {table_name}")
        data = cursor.fetchall()
        
        if not data:
            log.warning(f"{log_prefix}No coins in database for category calculation")
            return
        
        df = pd.DataFrame(data)
        
        # Calculate ranks (1-6) using quantile-based ranking
        # duplicates='drop' handles coins with identical volume
        df['rank'] = pd.qcut(
            df['volume_24h_usd'],
            q=6,
            labels=False,
            duplicates='drop'
        ) + 1  # pd.qcut uses 0-5, we need 1-6
        
        # Prepare data for bulk update
        update_data = list(df[['rank', 'full_symbol']].itertuples(index=False, name=None))
        
        # Execute bulk UPDATE
        query = sql.SQL("""
            UPDATE {table} SET
                category = data.rank
            FROM (VALUES %s) AS data (rank, symbol)
            WHERE {table}.full_symbol = data.symbol;
        """).format(
            table=sql.Identifier(table_name)
        )
        
        execute_values(cursor, query, update_data)
        conn.commit()
        
        log.info(f"{log_prefix}✅ Volume categories (ranks 1-6) successfully calculated and saved for {len(df)} coins.")
    
    except Exception as e:
        log.error(f"{log_prefix}❌ Error calculating volume categories: {e}", exc_info=True)
        if conn:
            conn.rollback()
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()