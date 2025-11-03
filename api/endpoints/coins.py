# api/endpoints/coins.py

import logging
import io
import pandas as pd
from fastapi import APIRouter, HTTPException, Depends, Response # (ИЗМЕНЕНИЕ №1)
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder

# Import project modules
import config
import services 
# (ИЗМЕНЕНО) Импорт fetch_all_coins_from_db из database больше не нужен

# Import our security module
from api.security import verify_token

# --- Setup ---
log = logging.getLogger(__name__)
# (ИЗМЕНЕНО) Переименовано для соответствия __init__.py
coins_router = APIRouter()


# ============================================================================
# === _extract_base_symbol_from_full (НОВОЕ) ===
# ============================================================================
def _extract_base_symbol_from_full(full_symbol: str) -> str:
    """
    (ИЗМЕНЕНИЕ №1) Извлекает базовый символ из полного формата (e.g., 'SOL/USDT:USDT' -> 'SOL').
    Это обеспечивает единую логику сравнения с Черным списком.
    """
    if not full_symbol:
        return ""
    # Базовый символ - это часть до первого слэша (/)
    # (e.g., SOL/USDT:USDT -> SOL/USDT)
    ccxt_symbol = full_symbol.split(':')[0] 
    # (e.g., SOL/USDT -> SOL)
    return ccxt_symbol.split('/')[0]


# (ИЗМЕНЕНО) Используем новое имя переменной
@coins_router.get("/coins/filtered", dependencies=[Depends(verify_token)])
async def get_filtered_coins():
    """
    (РЕФАКТОРИНГ) "ПЛАН ЧИСТОГАН".
    Возвращает JSON со ВСЕМИ монетами (из кэша),
    кроме тех, что в Черном списке.
    """
    log_prefix = "[API /coins/filtered] "
    try:
        log.info(f"{log_prefix} Request 'ПЛАН ЧИСТОГАН' (из кэша).")

        # 1. (БЕЗ ИЗМЕНЕНИЙ) Загрузка Blacklist из MongoDB
        blacklist = await services.load_blacklist_from_mongo_async(log_prefix)
        log.info(f"{log_prefix} Loaded Blacklist (MongoDB): {len(blacklist)} coins.")

        # 2. (ИЗМЕНЕНО) Загрузка ВСЕХ монет (из кэша)
        all_coins = await services.get_cached_coins_data(
            log_prefix=f"{log_prefix} [Cache]"
        )
        
        if not all_coins:
            log.warning(f"{log_prefix} Данные не найдены (кэш пуст).")
            return JSONResponse(content={"count": 0, "coins": []})

        # --- ИЗМЕНЕНИЕ №2: ИСПРАВЛЕНИЕ ФИЛЬТРАЦИИ И ЛОГИРОВАНИЕ (JSON) ---
        coins_filtered_by_blacklist = 0
        
        filtered_coins = []
        for coin in all_coins:
            base_symbol = _extract_base_symbol_from_full(coin['full_symbol'])
            if base_symbol not in blacklist:
                filtered_coins.append(coin)
            else:
                coins_filtered_by_blacklist += 1
        
        count_before = len(all_coins)
        count_after = len(filtered_coins)
        
        log.info(f"{log_prefix} Blacklist filtering: {count_before} -> {count_after} coins.")
        if coins_filtered_by_blacklist > 0:
            log.warning(f"{log_prefix} 🚫 Отсеяно по Черному списку: {coins_filtered_by_blacklist} монет.")
        
        log.info(f"{log_prefix} Success. Returning {count_after} coins.")
        # --- КОНЕЦ ИЗМЕНЕНИЯ №2 ---
        
        # (БЕЗ ИЗМЕНЕНИЙ) Обертка для дат
        return JSONResponse(content=jsonable_encoder({
            "count": count_after,
            "coins": filtered_coins 
        }))

    except Exception as e:
        log.error(f"{log_prefix} Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# (ИЗМЕНЕНО) Используем новое имя переменной
@coins_router.get("/coins/filtered/csv", dependencies=[Depends(verify_token)])
async def get_filtered_coins_csv():
    """
    (РЕФАКТОРИНГ) "ПЛАН ЧИСТОГАН" (CSV).
    Возвращает CSV со ВСЕМИ монетами (из кэша),
    кроме тех, что в Черном списке.
    """
    log_prefix = "[API /coins/filtered/csv] "
    try:
        log.info(f"{log_prefix} CSV-Request 'ПЛАН ЧИСТОГАН' (из кэша).")

        # 1. (БЕЗ ИЗМЕНЕНИЙ) Загрузка Blacklist из MongoDB
        blacklist = await services.load_blacklist_from_mongo_async(log_prefix)
        log.info(f"{log_prefix} Loaded Blacklist (MongoDB): {len(blacklist)} coins.")

        # 2. (ИЗМЕНЕНО) Загрузка ВСЕХ монет (из кэша)
        all_coins = await services.get_cached_coins_data(
            log_prefix=f"{log_prefix} [Cache]"
        )
        
        if not all_coins:
            log.warning(f"{log_prefix} Данные не найдены (кэш пуст).")
            # (ИЗМЕНЕНИЕ №1) Исправлен возврат 404
            return Response(content="No data found", status_code=404, media_type="text/plain")

        # --- ИЗМЕНЕНИЕ №3: ИСПРАВЛЕНИЕ ФИЛЬТРАЦИИ И ЛОГИРОВАНИЕ (CSV) ---
        coins_filtered_by_blacklist = 0
        
        filtered_coins = []
        for coin in all_coins:
            base_symbol = _extract_base_symbol_from_full(coin['full_symbol'])
            if base_symbol not in blacklist:
                filtered_coins.append(coin)
            else:
                coins_filtered_by_blacklist += 1
        
        count_after = len(filtered_coins)
        
        log.info(f"{log_prefix} Blacklist filtering: {len(all_coins)} -> {count_after} coins.")
        if coins_filtered_by_blacklist > 0:
            log.warning(f"{log_prefix} 🚫 Отсеяно по Черному списку: {coins_filtered_by_blacklist} монет.")
        
        if not filtered_coins: 
            log.warning(f"{log_prefix} No data after filtering.")
            # (ИЗМЕНЕНИЕ №1) Исправлен возврат 404
            return Response(content="No data found after filtering", status_code=404, media_type="text/plain")
        # --- КОНЕЦ ИЗМЕНЕНИЯ №3 ---

        # 4. (БЕЗ ИЗМЕНЕНИЙ) Конвертация в DataFrame
        df = pd.DataFrame(filtered_coins) 
        
        columns_in_order = [col for col in config.DATABASE_SCHEMA.keys() if col in df.columns]
        df = df[columns_in_order]

        log.info(f"{log_prefix} DataFrame created. {df.shape[0]} rows. Sending CSV.")
        
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        
        response = StreamingResponse(
            iter([stream.getvalue()]), 
            media_type="text/csv"
        )
        response.headers["Content-Disposition"] = "attachment; filename=coins_export.csv"
        return response

    except Exception as e:
        log.error(f"{log_prefix} Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))