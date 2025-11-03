# database/schema.py
"""
Database schema management and DDL operations.
"""

import logging
from psycopg2 import sql

from config import DATABASE_SCHEMA
from .connection import get_db_connection

log = logging.getLogger(__name__)

# --- (НОВОЕ) Глобальные константы ---
# (Вынесены из setup_database_tables, чтобы их можно было импортировать)
TABLE_NAME = "monthly_coin_selection"
LOGS_TABLE_NAME = "script_run_logs"
# --- Конец НОВОГО блока ---


def setup_database_tables():
    """
    Checks and creates all necessary tables in the database.
    ...
    """
    conn = None
    # (ИЗМЕНЕНО) Локальные переменные удалены,
    # теперь функция использует глобальные константы TABLE_NAME и LOGS_TABLE_NAME
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # --- 1. Create/verify monthly_coin_selection table ---
        log.info(f"Checking table '{TABLE_NAME}'...")
        
        # Build column definitions with proper SQL quoting
        columns_sql_objects = []
        for col_name, col_type in DATABASE_SCHEMA.items():
            columns_sql_objects.append(
                sql.SQL("{} {}").format(
                    sql.Identifier(col_name),  # Preserves case with quotes
                    sql.SQL(col_type)           # Raw SQL type
                )
            )
        
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                {columns}
            );
        """).format(
            table=sql.Identifier(TABLE_NAME), # (ИЗМЕНЕНО)
            columns=sql.SQL(', ').join(columns_sql_objects)
        )
        
        cursor.execute(create_table_query)
        
        # Check for missing columns and add them
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s
        """, (TABLE_NAME,)) # (ИЗМЕНЕНО)
        
        existing_columns = {row[0] for row in cursor.fetchall()}
        
        for col_name, col_type in DATABASE_SCHEMA.items():
            if col_name not in existing_columns:
                log.warning(f"Adding missing column '{col_name}' to {TABLE_NAME}") # (ИЗМЕНЕНО)
                alter_query = sql.SQL("ALTER TABLE {table} ADD COLUMN {col} {type}").format(
                    table=sql.Identifier(TABLE_NAME), # (ИЗМЕНЕНО)
                    col=sql.Identifier(col_name),
                    type=sql.SQL(col_type)
                )
                cursor.execute(alter_query)

        # --- 2. Create/verify script_run_logs table ---
        log.info(f"Checking table '{LOGS_TABLE_NAME}'...") # (ИЗМЕНЕНО)
        
        cursor.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP WITH TIME ZONE,
                status VARCHAR(50) NOT NULL,
                details TEXT,
                coins_saved INT DEFAULT 0
            );
        """).format(table=sql.Identifier(LOGS_TABLE_NAME))) # (ИЗМЕНЕНО)
        
        # Check for coins_saved column (for legacy databases)
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name = 'coins_saved'
        """, (LOGS_TABLE_NAME,)) # (ИЗМЕНЕНО)
        
        if not cursor.fetchone():
            log.warning(f"Adding 'coins_saved' column to {LOGS_TABLE_NAME}") # (ИЗМЕНЕНО)
            cursor.execute(sql.SQL(
                "ALTER TABLE {table} ADD COLUMN coins_saved INT DEFAULT 0"
            ).format(table=sql.Identifier(LOGS_TABLE_NAME))) # (ИЗМЕНЕНО)
        
        conn.commit()
        cursor.close()
        log.info("✅ Database tables setup completed successfully")

    except Exception as e:
        log.error(f"❌ Error setting up database tables: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def add_column_if_missing(table_name: str, column_name: str, column_type: str):
    """
    Safely adds a column to a table if it doesn't exist.
    ...
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name = %s
        """, (table_name, column_name))
        
        if not cursor.fetchone():
            log.info(f"Adding column '{column_name}' to table '{table_name}'")
            cursor.execute(sql.SQL("ALTER TABLE {table} ADD COLUMN {col} {type}").format(
                table=sql.Identifier(table_name),
                col=sql.Identifier(column_name),
                type=sql.SQL(column_type)
            ))
            conn.commit()
            log.info(f"✅ Column '{column_name}' added successfully")
        else:
            log.debug(f"Column '{column_name}' already exists in '{table_name}'")
        
        cursor.close()
    except Exception as e:
        log.error(f"❌ Error adding column '{column_name}': {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()