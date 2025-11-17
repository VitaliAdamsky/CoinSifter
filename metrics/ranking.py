# metrics/ranking.py
"""
(МИГРАЦИЯ НА MONGO)
Этот модуль рассчитывает категории (ранги) объема.
Функция update_volume_categories (которая работала с PostgreSQL) 
заменена на calculate_volume_categories, которая работает 
в памяти со списком словарей.
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any

# (ИЗМЕНЕНИЕ) Импорты PostgreSQL удалены
# import psycopg2
# from psycopg2 import sql
# from psycopg2.extras import execute_values, RealDictCursor
# from database import get_db_connection

import config

log = logging.getLogger(__name__)


def calculate_volume_categories(
    coins: List[Dict[str, Any]], 
    log_prefix=""
) -> Dict[str, int]:
    """
    (ИЗМЕНЕНО) Рассчитывает категории объема (Ранги 1-6) на основе 
    списка монет (dict) в памяти.
    
    Возвращает словарь-карту: {full_symbol: rank}
    """
    log.info(f"{log_prefix}--- Расчет Категорий Объема (Ранги 1-6) в памяти ---")
    
    if not coins:
        log.warning(f"{log_prefix}Нет монет для расчета категорий.")
        return {}

    try:
        # 1. Загружаем данные в DataFrame
        # Нам нужны только 'full_symbol' и 'volume_24h_usd'
        df = pd.DataFrame(
            [
                {
                    "full_symbol": c.get("full_symbol"), 
                    "volume_24h_usd": c.get("volume_24h_usd")
                } 
                for c in coins
            ]
        )
        
        if df.empty:
            log.warning(f"{log_prefix}DataFrame пуст после извлечения данных.")
            return {}
            
        # 2. Рассчитываем ранги (1-6)
        # (Логика pd.qcut сохранена из оригинальной функции)
        # duplicates='drop' обрабатывает монеты с идентичным объемом
        df['rank'] = pd.qcut(
            df['volume_24h_usd'],
            q=6,
            labels=False,
            duplicates='drop'
        ) + 1  # pd.qcut использует 0-5, нам нужно 1-6
        
        # 3. Преобразуем в словарь для быстрого поиска
        # {full_symbol: rank}
        rank_map = pd.Series(
            df['rank'].values, 
            index=df['full_symbol']
        ).to_dict()
        
        log.info(f"{log_prefix}✅ Категории (ранги 1-6) успешно рассчитаны для {len(rank_map)} монет.")
        
        return rank_map

    except Exception as e:
        log.error(f"{log_prefix}❌ Ошибка при расчете категорий объема: {e}", exc_info=True)
        return {}