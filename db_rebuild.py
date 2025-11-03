#!/usr/bin/env python3
"""
Database Rebuild Script - Complete Table Recreation

This script performs a DESTRUCTIVE operation:
- Drops all existing tables (monthly_coin_selection, script_run_logs)
- Recreates them from scratch using the schema from config.py
- All data will be PERMANENTLY DELETED

Use cases:
- Schema migration after major changes
- Fixing corrupt table structures
- Clean slate after testing
- Resolving type conflicts

Usage:
    python db_rebuild.py

Safety:
- 5 second countdown before execution
- Requires manual confirmation (Ctrl+C to abort)
- Logs all operations
"""

import logging
import time
import sys
from dotenv import load_dotenv
from psycopg2 import sql

# Import from new database module
from database import get_db_connection, setup_database_tables
import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)
log = logging.getLogger(__name__)


def print_warning_banner():
    """Display a scary warning banner."""
    banner = """
    ╔════════════════════════════════════════════════════════════╗
    ║                    ⚠️  DANGER ZONE  ⚠️                     ║
    ╠════════════════════════════════════════════════════════════╣
    ║                                                            ║
    ║  This script will PERMANENTLY DELETE all data in:         ║
    ║                                                            ║
    ║  • monthly_coin_selection (all analyzed coins)            ║
    ║  • script_run_logs (all execution history)                ║
    ║                                                            ║
    ║  ⚠️  THIS OPERATION CANNOT BE UNDONE! ⚠️                   ║
    ║                                                            ║
    ║  Make sure you have:                                       ║
    ║  ✓ Backed up your database                                ║
    ║  ✓ Confirmed this is what you want                        ║
    ║                                                            ║
    ╚════════════════════════════════════════════════════════════╝
    """
    print(banner)


def drop_all_tables():
    """
    Drops all tables that will be recreated.
    
    Tables dropped:
    - monthly_coin_selection: Main coins data
    - script_run_logs: Execution logs
    
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    tables_to_drop = [
        "monthly_coin_selection",
        "script_run_logs"
    ]
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        log.info("=" * 60)
        log.info("PHASE 1: DROPPING EXISTING TABLES")
        log.info("=" * 60)
        
        for table_name in tables_to_drop:
            log.warning(f"🗑️  Dropping table: {table_name}...")
            
            drop_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                sql.Identifier(table_name)
            )
            cursor.execute(drop_query)
            
            log.info(f"✅ Table '{table_name}' dropped successfully")
        
        conn.commit()
        cursor.close()
        
        log.info("=" * 60)
        log.info("✅ ALL TABLES DROPPED SUCCESSFULLY")
        log.info("=" * 60)
        
        return True
        
    except Exception as e:
        log.error(f"❌ Error dropping tables: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
        
    finally:
        if conn:
            conn.close()


def recreate_tables():
    """
    Recreates all tables using the schema from config.py.
    
    Uses the setup_database_tables() function from the database module,
    which handles:
    - Table creation with proper types
    - Column quoting (preserves case)
    - All constraints and indexes
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        log.info("")
        log.info("=" * 60)
        log.info("PHASE 2: RECREATING TABLES FROM SCHEMA")
        log.info("=" * 60)
        
        log.info("Calling setup_database_tables()...")
        setup_database_tables()
        
        log.info("=" * 60)
        log.info("✅ ALL TABLES RECREATED SUCCESSFULLY")
        log.info("=" * 60)
        
        return True
        
    except Exception as e:
        log.error(f"❌ Error recreating tables: {e}", exc_info=True)
        return False


def verify_tables():
    """
    Verifies that tables were created correctly.
    
    Checks:
    - Tables exist
    - Have correct columns
    - Proper types (especially category: SMALLINT)
    
    Returns:
        bool: True if verification passed, False otherwise
    """
    conn = None
    try:
        log.info("")
        log.info("=" * 60)
        log.info("PHASE 3: VERIFYING TABLE STRUCTURE")
        log.info("=" * 60)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check monthly_coin_selection
        log.info("Checking 'monthly_coin_selection'...")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'monthly_coin_selection'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        if not columns:
            log.error("❌ Table 'monthly_coin_selection' has no columns!")
            return False
        
        log.info(f"✅ Found {len(columns)} columns")
        
        # Verify critical columns
        column_dict = {col[0]: col[1] for col in columns}
        
        critical_checks = [
            ('symbol', 'character varying'),
            ('full_symbol', 'character varying'),
            ('category', 'smallint'),  # Should be SMALLINT now!
            ('volume_24h_usd', 'double precision'),
        ]
        
        all_passed = True
        for col_name, expected_type in critical_checks:
            if col_name in column_dict:
                actual_type = column_dict[col_name]
                if expected_type in actual_type.lower():
                    log.info(f"  ✅ {col_name}: {actual_type}")
                else:
                    log.error(f"  ❌ {col_name}: Expected '{expected_type}', got '{actual_type}'")
                    all_passed = False
            else:
                log.error(f"  ❌ {col_name}: Column missing!")
                all_passed = False
        
        # Check script_run_logs
        log.info("")
        log.info("Checking 'script_run_logs'...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = 'script_run_logs'
        """)
        
        log_columns_count = cursor.fetchone()[0]
        if log_columns_count > 0:
            log.info(f"✅ Found {log_columns_count} columns in script_run_logs")
        else:
            log.error("❌ Table 'script_run_logs' has no columns!")
            all_passed = False
        
        cursor.close()
        
        if all_passed:
            log.info("=" * 60)
            log.info("✅ VERIFICATION PASSED")
            log.info("=" * 60)
        else:
            log.error("=" * 60)
            log.error("❌ VERIFICATION FAILED - CHECK ERRORS ABOVE")
            log.error("=" * 60)
        
        return all_passed
        
    except Exception as e:
        log.error(f"❌ Error verifying tables: {e}", exc_info=True)
        return False
        
    finally:
        if conn:
            conn.close()


def wipe_and_rebuild_tables():
    """
    Main function: Complete database rebuild.
    
    Steps:
    1. Drop all existing tables
    2. Recreate tables from config.py schema
    3. Verify table structure
    
    Returns:
        bool: True if all steps successful, False otherwise
    """
    log.info("╔" + "=" * 58 + "╗")
    log.info("║" + " " * 15 + "DATABASE REBUILD STARTED" + " " * 19 + "║")
    log.info("╚" + "=" * 58 + "╝")
    log.info("")
    
    # Step 1: Drop tables
    if not drop_all_tables():
        log.error("⛔ Failed to drop tables. Aborting.")
        return False
    
    # Step 2: Recreate tables
    if not recreate_tables():
        log.error("⛔ Failed to recreate tables. Database may be in inconsistent state!")
        return False
    
    # Step 3: Verify
    if not verify_tables():
        log.warning("⚠️  Tables recreated but verification failed. Check manually.")
        return False
    
    # Success!
    log.info("")
    log.info("╔" + "=" * 58 + "╗")
    log.info("║" + " " * 10 + "🎉 DATABASE REBUILD COMPLETED! 🎉" + " " * 13 + "║")
    log.info("╠" + "=" * 58 + "╣")
    log.info("║" + " " * 58 + "║")
    log.info("║  Tables recreated with clean schema from config.py        ║")
    log.info("║  All old data has been permanently deleted                ║")
    log.info("║  Ready for fresh analysis run                             ║")
    log.info("║" + " " * 58 + "║")
    log.info("╚" + "=" * 58 + "╝")
    
    return True


def main():
    """Entry point with safety checks."""
    # Load environment variables
    load_dotenv()
    
    # Display warning
    print_warning_banner()
    
    # Countdown
    countdown_seconds = 5
    log.warning(f"⏱️  You have {countdown_seconds} seconds to press Ctrl+C to abort...")
    
    try:
        for i in range(countdown_seconds, 0, -1):
            log.warning(f"   {i}...")
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("")
        log.info("⛔ ABORTED by user. No changes made.")
        sys.exit(0)
    
    log.info("")
    log.info("⏰ Time's up! Starting rebuild...")
    log.info("")
    
    # Execute rebuild
    success = wipe_and_rebuild_tables()
    
    if success:
        sys.exit(0)
    else:
        log.error("⛔ Rebuild failed. Check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()