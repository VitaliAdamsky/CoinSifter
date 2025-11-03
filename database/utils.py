# database/utils.py
"""
Utility functions for database operations.
"""

import logging
import numpy as np
import pandas as pd
from typing import Any, Dict, Tuple
from psycopg2 import sql

# (НОВОЕ) Импортируем Pydantic-модель, которая нужна новой функции
from .models import CoinQueryParams

log = logging.getLogger(__name__)


# --- (СУЩЕСТВУЮЩАЯ ФУНКЦИЯ - НЕ ИЗМЕНЕНА) ---
def convert_value_for_db(value: Any, field_name: str = "unknown") -> Any:
    """
    Converts Python/NumPy/Pandas types to PostgreSQL-compatible types.
    ...
    """
    # Handle None first
    if value is None:
        return None
    
    # Handle lists/arrays EARLY (before any type checks that might fail)
    if isinstance(value, (list, np.ndarray)):
        # Convert numpy array to list
        if isinstance(value, np.ndarray):
            return value.tolist()
        return list(value)
    
    # Handle NaN (from NumPy or Pandas)
    try:
        if isinstance(value, float) and np.isnan(value):
            return None
    except (TypeError, ValueError):
        # np.isnan() can raise TypeError for non-numeric types
        pass
    
    # Handle Pandas NaT (Not a Time)
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    
    # Convert NumPy/Pandas integers to Python int first
    if isinstance(value, (np.int8, np.int16, np.int32, np.int64, 
                         np.uint8, np.uint16, np.uint32, np.uint64)):
        value = int(value)
    
    # Convert integers to float for safe storage
    # (PostgreSQL DOUBLE PRECISION can handle large numbers)
    if isinstance(value, int):
        # Check for reasonable range
        if abs(value) > 1e15:  # Очень большое число
            log.warning(f"Field '{field_name}' has very large value: {value}. Converting to float.")
        return float(value)
    
    # Convert NumPy floats to Python float
    if isinstance(value, (np.float16, np.float32, np.float64)):
        return float(value)
    
    # Convert Pandas Timestamp to Python datetime
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    
    # Handle NumPy bool
    if isinstance(value, np.bool_):
        return bool(value)
    
    # Return as-is for strings, dates, etc.
    return value


# --- (СУЩЕСТВУЮЩАЯ ФУНКЦИЯ - НЕ ИЗМЕНЕНА) ---
def validate_coin_data(coin: dict, schema: dict) -> tuple[bool, str]:
    """
    Validates coin data before insertion.
    ...
    """
    required_fields = ['symbol', 'full_symbol']
    
    for field in required_fields:
        if field not in coin or coin[field] is None:
            return False, f"Missing required field: {field}"
    
    # Check for invalid types in critical fields
    if 'category' in coin and coin['category'] is not None:
        try:
            cat_val = int(coin['category'])
            if cat_val < 1 or cat_val > 6:
                return False, f"Invalid category value: {cat_val} (must be 1-6)"
        except (ValueError, TypeError):
            return False, f"Invalid category type: {coin['category']}"
    
    return True, ""


# --- (СУЩЕСТВУЮЩАЯ ФУНКЦИЯ - НЕ ИЗМЕНЕНА) ---
def prepare_coin_row(coin: dict, schema_columns: list) -> tuple:
    """
    Prepares a single coin row for database insertion.
    ...
    """
    row = []
    for col_name in schema_columns:
        value = coin.get(col_name, None)
        converted_value = convert_value_for_db(value, col_name)
        row.append(converted_value)
    return tuple(row)


# --- (НОВАЯ ФУНКЦИЯ - ДОБАВЛЕНА ДЛЯ ИСПРАВЛЕНИЯ IMPORT_ERROR) ---
def build_query_with_filters(
    base_query: str, 
    params: CoinQueryParams, 
    log_prefix: str = ""
) -> Tuple[sql.SQL, sql.SQL, Dict[str, Any]]:
    """
    Строит SQL-запрос (для данных и для подсчета) с учетом фильтров, 
    сортировки и пагинации.
    """
    query_params = {}
    where_conditions = []

    # --- 1. Фильтры (WHERE) ---
    if params.filters:
        log.debug(f"{log_prefix} Применение фильтров: {params.filters}")
        for key, value in params.filters.items():
            if value is None:
                continue
            
            # (Безопасная вставка имени колонки)
            col_sql = sql.Identifier(key)
            
            # (Безопасная вставка значения)
            # Мы используем %(param_name)s синтаксис для psycopg2
            param_name = f"filter_{key}"
            
            if isinstance(value, list):
                # Фильтр "IN (...)"
                where_conditions.append(sql.SQL("{} IN %s").format(col_sql))
                query_params[param_name] = tuple(value)
            elif isinstance(value, dict):
                # Фильтры диапазона (например, {"min": 100, "max": 1000})
                if "min" in value:
                    min_param = f"{param_name}_min"
                    where_conditions.append(sql.SQL("{} >= %({})s").format(col_sql, sql.SQL(min_param)))
                    query_params[min_param] = value["min"]
                if "max" in value:
                    max_param = f"{param_name}_max"
                    where_conditions.append(sql.SQL("{} <= %({})s").format(col_sql, sql.SQL(max_param)))
                    query_params[max_param] = value["max"]
            else:
                # Фильтр "column = value"
                where_conditions.append(sql.SQL("{} = %({})s").format(col_sql, sql.SQL(param_name)))
                query_params[param_name] = value

    where_clause = sql.SQL("")
    if where_conditions:
        where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_conditions)

    # --- 2. Запрос для подсчета (Count Query) ---
    count_query = sql.SQL("SELECT COUNT(*) {base} {where}").format(
        base=sql.SQL(base_query),
        where=where_clause
    )

    # --- 3. Сортировка (ORDER BY) ---
    # (Безопасная валидация)
    sort_order_sql = sql.SQL("DESC") if params.sort_order.lower() == 'desc' else sql.SQL("ASC")
    # (Безопасная вставка имени колонки)
    sort_by_sql = sql.Identifier(params.sort_by)
    
    order_by_clause = sql.SQL("ORDER BY {sort_by} {sort_order}, id ASC").format(
        sort_by=sort_by_sql,
        sort_order=sort_order_sql
    )

    # --- 4. Пагинация (OFFSET / LIMIT) ---
    offset = (params.page - 1) * params.limit
    pagination_clause = sql.SQL("LIMIT %(limit)s OFFSET %(offset)s")
    query_params["limit"] = params.limit
    query_params["offset"] = offset

    # --- 5. Финальный запрос (Query) ---
    final_query = sql.SQL("SELECT * {base} {where} {order_by} {pagination}").format(
        base=sql.SQL(base_query),
        where=where_clause,
        order_by=order_by_clause,
        pagination=pagination_clause
    )

    return final_query, count_query, query_params