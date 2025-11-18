# api/endpoints/coins.py

import logging
import io
import pandas as pd
from fastapi import APIRouter, HTTPException, Depends, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder

# --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
import config
# (БЫЛО) import services 
# (СТАЛО) Импортируем НАПРЯМУЮ
from services.data_cache_service import get_cached_coins_data
from services.mongo_service import load_blacklist_from_mongo_async
# --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

# Import our security module
from api.security import verify_token

# --- Setup ---
log = logging.getLogger(__name__)
coins_router = APIRouter()


# ============================================================================\r
# === _extract_base_symbol_from_full ===\r
# ============================================================================
def _extract_base_symbol_from_full(full_symbol: str) -> str:
    """
    Извлекает базовый символ из полного формата (e.g., 'SOL/USDT:USDT' -> 'SOL').
    Это обеспечивает единую логику сравнения с Черным списком.
    """
    if not full_symbol:
        return ""
    # Базовый символ - это часть до первого слэша (/)\r
    ccxt_symbol = full_symbol.split(':')[0] 
    return ccxt_symbol.split('/')[0]


# ============================================================================\r
# === ЗАЩИЩЁННЫЙ ЭНДПОИНТ (JSON) ===\r
# ============================================================================
@coins_router.get("/coins/filtered", dependencies=[Depends(verify_token)])
async def get_filtered_coins():
    """
    (V3) Возвращает ВСЕ отфильтрованные монеты из КЭША (MongoDB).
    (ИЗМЕНЕНО) Удалена вся логика фильтрации - она теперь в КЭШЕ.
    """
    log_prefix = "[API /coins/filtered GET]"
    log.info(f"{log_prefix} Запрошены монеты (JSON) из кэша...")
    
    try:
        # --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
        # (БЫЛО) data = await services.get_cached_coins_data(...)
        # (СТАЛО)
        data = await get_cached_coins_data(
            force_reload=False, 
            log_prefix=f"{log_prefix} [Cache]"
        )
        # --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---
        
        if not data:
            log.warning(f"{log_prefix} Кэш пуст.")
            raise HTTPException(status_code=404, detail="No data available in cache.")
            
        log.info(f"{log_prefix} ✅ Успешно. Возвращаем {len(data)} монет из кэша.")
        
        return JSONResponse(content=jsonable_encoder({
            "count": len(data),
            "data": data
        }))
        
    except HTTPException:
        raise 
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================\r
# === ПУБЛИЧНЫЙ ЭНДПОИНТ (CSV) ===\r
# ============================================================================
@coins_router.get("/coins/filtered/csv")
async def get_filtered_coins_csv():
    """
    (V3) Возвращает ВСЕ монеты из КЭША (MongoDB) в CSV формате.
    """
    log_prefix = "[API /coins/filtered/csv GET]"
    log.info(f"{log_prefix} Запрошены монеты (CSV)...")

    try:
        # 1. Получаем данные из кэша
        # --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
        # (БЫЛО) all_coins = await services.get_cached_coins_data(...)
        # (СТАЛО)
        all_coins = await get_cached_coins_data(
            force_reload=False, 
            log_prefix=f"{log_prefix} [Cache]"
        )
        # --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

        # 2. Получаем Черный список (для фильтрации на лету)
        # --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
        # (БЫЛО) blacklist = await services.load_blacklist_from_mongo_async(...)
        # (СТАЛО)
        blacklist = await load_blacklist_from_mongo_async(
            log_prefix=f"{log_prefix} [Blacklist]"
        )
        # --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---
        
        if not all_coins:
            log.warning(f"{log_prefix} Кэш пуст.")
            return Response(content="No data available in cache", status_code=404, media_type="text/plain")

        # 3. Фильтруем по Blacklist
        filtered_coins = []
        coins_filtered_by_blacklist = 0
        
        for coin in all_coins:
            base_symbol = _extract_base_symbol_from_full(coin['symbol'])
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
        response.headers["Content-Disposition"] = "attachment; filename=coins_data.csv"
        
        return response

    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")