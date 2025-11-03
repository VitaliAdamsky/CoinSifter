import logging
import pandas as pd
from typing import Dict, Any, List
from psycopg2 import sql, extras

from .models import CoinQueryParams, CoinsFilteredResponse
from .connection import get_db_connection, get_db_connection_context
from .schema import TABLE_NAME
from .utils import build_query_with_filters, validate_coin_data, prepare_coin_row

from config import DATABASE_SCHEMA

log = logging.getLogger(__name__)

# --- (Функция clear_existing_data) ---
def clear_existing_data(log_prefix="[DB.Clear]"):
    """
    (V3) Очищает таблицу 'monthly_coin_selection' перед новой вставкой.
    """
    log.info(f"{log_prefix} Очистка таблицы '{TABLE_NAME}'...")
    conn = None
    try:
        conn = get_db_connection() 
        with conn.cursor() as cursor:
            truncate_query = sql.SQL("TRUNCATE TABLE {table} RESTART IDENTITY;").format(
                table=sql.Identifier(TABLE_NAME)
            )
            cursor.execute(truncate_query)
        conn.commit()
        log.info(f"{log_prefix} ✅ Таблица '{TABLE_NAME}' успешно очищена.")
        
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при очистке таблицы: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# --- (Функция save_coins_to_db) ---
def save_coins_to_db(coins: List[Dict[str, Any]], log_prefix="[DB.Save]"):
    """
    (V3) Сохраняет список монет (dict) в базу данных.
    """
    if not coins:
        log.warning(f"{log_prefix} Нет данных для сохранения.")
        return 0
        
    log.info(f"{log_prefix} Попытка сохранения {len(coins)} монет в БД...")
    
    conn = None
    try:
        conn = get_db_connection() 
        with conn.cursor() as cursor:
            
            schema_columns = list(DATABASE_SCHEMA.keys())
            
            rows_to_insert = []
            invalid_count = 0
            
            for coin in coins:
                is_valid, error_msg = validate_coin_data(coin, DATABASE_SCHEMA)
                if not is_valid:
                    log.warning(f"{log_prefix} Невалидные данные монеты: {error_msg}. Пропуск.")
                    invalid_count += 1
                    continue
                
                row_tuple = prepare_coin_row(coin, schema_columns)
                rows_to_insert.append(row_tuple)

            if not rows_to_insert:
                log.warning(f"{log_prefix} Нет валидных строк для вставки (Всего: {len(coins)}, Невалидных: {invalid_count}).")
                return 0

            cols_sql = sql.SQL(', ').join(map(sql.Identifier, schema_columns))
            placeholders = sql.SQL(', ').join([sql.SQL('%s')] * len(schema_columns))
            
            insert_query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES %s
            """).format(
                table=sql.Identifier(TABLE_NAME),
                columns=cols_sql
            )
            
            extras.execute_values(
                cursor,
                insert_query,
                rows_to_insert,
                template=None,
                page_size=200 
            )
            
        conn.commit()
        saved_count = len(rows_to_insert)
        log.info(f"{log_prefix} ✅ Успешно сохранено {saved_count} монет (Пропущено невалидных: {invalid_count}).")
        return saved_count

    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при сохранении в PostgreSQL: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# --- (Функция fetch_all_coins_from_db) ---
def fetch_all_coins_from_db(log_prefix="[DB.FetchAll]") -> List[Dict[str, Any]]:
    """
    (V3) Загружает ВСЕ монеты из 'monthly_coin_selection' как list[dict].
    """
    log.info(f"{log_prefix} Загрузка ВСЕХ монет из '{TABLE_NAME}' (list[dict])...")
    conn = None
    try:
        conn = get_db_connection() 
        if conn is None:
            return []
            
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            query = sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(TABLE_NAME))
            cursor.execute(query)
            rows = cursor.fetchall()
            log.info(f"{log_prefix} ✅ Загружено {len(rows)} монет.")
            return rows 
            
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при чтении из PostgreSQL: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

# --- (Функция fetch_filtered_coins) ---
def fetch_filtered_coins(params: CoinQueryParams, log_prefix="") -> CoinsFilteredResponse:
    """
    (V3) Загружает монеты из БД с учетом фильтрации, сортировки и пагинации.
    """
    log_prefix = f"{log_prefix} [DB.FetchFiltered]"
    log.info(f"{log_prefix} Загрузка монет из БД (Page: {params.page}, Limit: {params.limit})...")
    
    conn = None
    try:
        conn = get_db_connection() 
        if conn is None:
            return CoinsFilteredResponse(total=0, data=[])
            
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            base_query = f"FROM {TABLE_NAME}"
            query, count_query, query_params = build_query_with_filters(
                base_query, params, log_prefix
            )
            
            cursor.execute(count_query, query_params)
            total_count = cursor.fetchone()['count']
            
            if total_count == 0:
                log.info(f"{log_prefix} Монеты по фильтрам не найдены.")
                return CoinsFilteredResponse(total=0, data=[])
            
            cursor.execute(query, query_params)
            rows = cursor.fetchall()
            
            log.info(f"{log_prefix} ✅ Найдено: {total_count} (Отдано: {len(rows)})")
            return CoinsFilteredResponse(total=total_count, data=rows)
            
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при чтении из PostgreSQL: {e}", exc_info=True)
        return CoinsFilteredResponse(total=0, data=[])
    finally:
        if conn:
            conn.close()

# --- (Функция fetch_filtered_coins_dataframe) ---
def fetch_filtered_coins_dataframe(log_prefix="") -> pd.DataFrame | None:
    """
    Загружает ВСЮ таблицу 'filtered_coins' напрямую в pandas.DataFrame.
    """
    log_prefix = f"{log_prefix} [DB.FetchAllDF]"
    log.info(f"{log_prefix} Попытка загрузки ВСЕХ монет из '{TABLE_NAME}' в DataFrame...")
    
    conn = None
    try:
        conn = get_db_connection() 
        if conn is None:
            log.error(f"{log_prefix} Не удалось получить соединение с БД.")
            return None

        query = sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(TABLE_NAME))
        df = pd.read_sql_query(query, conn)
        
        log.info(f"{log_prefix} ✅ Успешно загружено {len(df)} монет в DataFrame.")
        return df
        
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при чтении из PostgreSQL в DataFrame: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def fetch_last_analysis_timestamp(log_prefix: str = ""):
    """
    (НОВОЕ) Получает 'analyzed_at' из первой (LIMIT 1) записи в БД.
    """
    log.info(f"{log_prefix} ⏱️ Запрос времени последнего анализа (LIMIT 1)...")
    
    query = sql.SQL("SELECT analyzed_at FROM {table} ORDER BY id ASC LIMIT 1").format(
        table=sql.Identifier(TABLE_NAME)
    )

    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                
        if result:
            timestamp = result[0]
            log.info(f"{log_prefix} ✅ Найдено время: {timestamp}")
            return timestamp
        else:
            log.warning(f"{log_prefix} ⚠️ Таблица пуста. Время анализа не найдено.")
            return None

    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при запросе времени анализа: {e}", exc_info=True)
        raise