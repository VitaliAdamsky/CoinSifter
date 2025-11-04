# api/endpoints/coins.py

import logging
import io
import pandas as pd
from fastapi import APIRouter, HTTPException, Depends, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder

# Import project modules
import config
import services 

# Import our security module
from api.security import verify_token

# --- Setup ---
log = logging.getLogger(__name__)
coins_router = APIRouter()


# ============================================================================
# === _extract_base_symbol_from_full ===
# ============================================================================
def _extract_base_symbol_from_full(full_symbol: str) -> str:
    """
    Извлекает базовый символ из полного формата (e.g., 'SOL/USDT:USDT' -> 'SOL').
    Это обеспечивает единую логику сравнения с Черным списком.
    """
    if not full_symbol:
        return ""
    # Базовый символ - это часть до первого слэша (/)
    ccxt_symbol = full_symbol.split(':')[0] 
    return ccxt_symbol.split('/')[0]


# ============================================================================
# === ЗАЩИЩЁННЫЙ ЭНДПОИНТ (JSON) ===
# ============================================================================
@coins_router.get("/coins/filtered", dependencies=[Depends(verify_token)])
async def get_filtered_coins():
    """
    (РЕФАКТОРИНГ) "ПЛАН ЧИСТОГАН".
    Возвращает JSON со ВСЕМИ монетами (из кэша),
    кроме тех, что в Черном списке.
    
    🔒 ЗАЩИЩЁН ТОКЕНОМ
    """
    log_prefix = "[API /coins/filtered] "
    try:
        log.info(f"{log_prefix} Request 'ПЛАН ЧИСТОГАН' (из кэша).")

        # 1. Загрузка Blacklist из MongoDB
        blacklist = await services.load_blacklist_from_mongo_async(log_prefix)
        log.info(f"{log_prefix} Loaded Blacklist (MongoDB): {len(blacklist)} coins.")

        # 2. Загрузка ВСЕХ монет (из кэша)
        all_coins = await services.get_cached_coins_data(
            log_prefix=f"{log_prefix} [Cache]"
        )
        
        if not all_coins:
            log.warning(f"{log_prefix} Данные не найдены (кэш пуст).")
            return JSONResponse(content={"count": 0, "coins": []})

        # 3. Фильтрация по Blacklist
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
        
        # 4. Обертка для дат
        return JSONResponse(content=jsonable_encoder({
            "count": count_after,
            "coins": filtered_coins 
        }))

    except Exception as e:
        log.error(f"{log_prefix} Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# === ПУБЛИЧНЫЙ ЭНДПОИНТ (CSV) ===
# ============================================================================
@coins_router.get("/coins/filtered/csv")
async def get_filtered_coins_csv():
    """
    (ПУБЛИЧНЫЙ) "ПЛАН ЧИСТОГАН" (CSV).
    Возвращает CSV со ВСЕМИ монетами (из кэша),
    кроме тех, что в Черном списке.
    
    🌐 ПУБЛИЧНЫЙ (без токена)
    """
    log_prefix = "[API /coins/filtered/csv] "
    try:
        log.info(f"{log_prefix} CSV-Request 'ПЛАН ЧИСТОГАН' (из кэша). PUBLIC ACCESS.")

        # 1. Загрузка Blacklist из MongoDB
        blacklist = await services.load_blacklist_from_mongo_async(log_prefix)
        log.info(f"{log_prefix} Loaded Blacklist (MongoDB): {len(blacklist)} coins.")

        # 2. Загрузка ВСЕХ монет (из кэша)
        all_coins = await services.get_cached_coins_data(
            log_prefix=f"{log_prefix} [Cache]"
        )
        
        if not all_coins:
            log.warning(f"{log_prefix} Данные не найдены (кэш пуст).")
            return Response(content="No data found", status_code=404, media_type="text/plain")

        # 3. Фильтрация по Blacklist
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
            return Response(content="No data found after filtering", status_code=404, media_type="text/plain")

        # 4. Конвертация в DataFrame
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