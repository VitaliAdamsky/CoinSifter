# api/endpoints/coins.py

import logging
import io
import pandas as pd
from fastapi import APIRouter, HTTPException, Depends, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder

import config
# Импортируем сервисы напрямую
from services.data_cache_service import get_cached_coins_data
from services.mongo_service import load_blacklist_from_mongo_async

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
    (V3) Возвращает ВСЕ отфильтрованные монеты из КЭША (MongoDB).
    Фильтрация:
    1. Blacklist (Черный список)
    2. BTC Correlation < 0.4 (Слабая корреляция с битком)
    """
    log_prefix = "[API /coins/filtered GET]"
    log.info(f"{log_prefix} Запрошены монеты (JSON) из кэша...")
    
    try:
        # 1. Получаем данные из кэша
        all_coins = await get_cached_coins_data(
            force_reload=False, 
            log_prefix=f"{log_prefix} [Cache]"
        )
        
        # 2. Получаем Черный список
        blacklist = await load_blacklist_from_mongo_async(
            log_prefix=f"{log_prefix} [Blacklist]"
        )
        
        if not all_coins:
            log.warning(f"{log_prefix} Кэш пуст.")
            raise HTTPException(status_code=404, detail="No data available in cache.")
            
        # 3. Фильтрация
        filtered_coins = []
        stats = {
            "blacklist": 0,
            "low_correlation": 0
        }
        
        for coin in all_coins:
            # --- ПРОВЕРКА 1: Blacklist ---
            base_symbol = _extract_base_symbol_from_full(coin.get('symbol', ''))
            if base_symbol in blacklist:
                stats["blacklist"] += 1
                continue

            # --- ПРОВЕРКА 2: BTC Correlation < 0.4 ---
            # (Метрика из calculator.py: 'btc_corr_1d_w30')
            btc_corr = coin.get('btc_corr_1d_w30')
            
            # Если корреляции нет (None) или она меньше 0.4 -> пропускаем
            if btc_corr is None or btc_corr < 0.4:
                stats["low_correlation"] += 1
                continue

            # Если всё ок -> добавляем
            filtered_coins.append(coin)
        
        count_after = len(filtered_coins)

        log.info(f"{log_prefix} Filtering result: {len(all_coins)} -> {count_after} coins.")
        if stats["blacklist"] > 0:
            log.warning(f"{log_prefix} 🚫 Отсеяно по Черному списку: {stats['blacklist']}")
        if stats["low_correlation"] > 0:
            log.warning(f"{log_prefix} 📉 Отсеяно по Correlation (<0.4): {stats['low_correlation']}")
            
        log.info(f"{log_prefix} ✅ Успешно. Возвращаем {count_after} монет.")
        
        return JSONResponse(content=jsonable_encoder({
            "count": count_after,
            "data": filtered_coins
        }))
        
    except HTTPException:
        raise 
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# === ПУБЛИЧНЫЙ ЭНДПОИНТ (CSV) ===
# ============================================================================
@coins_router.get("/coins/filtered/csv")
async def get_filtered_coins_csv():
    """
    (V3) Возвращает ВСЕ монеты из КЭША (MongoDB) в CSV формате.
    Фильтрация:
    1. Blacklist
    2. BTC Correlation < 0.4
    """
    log_prefix = "[API /coins/filtered/csv GET]"
    log.info(f"{log_prefix} Запрошены монеты (CSV)...")

    try:
        # 1. Получаем данные из кэша
        all_coins = await get_cached_coins_data(
            force_reload=False, 
            log_prefix=f"{log_prefix} [Cache]"
        )

        # 2. Получаем Черный список
        blacklist = await load_blacklist_from_mongo_async(
            log_prefix=f"{log_prefix} [Blacklist]"
        )
        
        if not all_coins:
            log.warning(f"{log_prefix} Кэш пуст.")
            return Response(content="No data available in cache", status_code=404, media_type="text/plain")

        # 3. Фильтрация
        filtered_coins = []
        stats = {
            "blacklist": 0,
            "low_correlation": 0
        }
        
        for coin in all_coins:
            # --- ПРОВЕРКА 1: Blacklist ---
            base_symbol = _extract_base_symbol_from_full(coin.get('symbol', ''))
            if base_symbol in blacklist:
                stats["blacklist"] += 1
                continue
            
            # --- ПРОВЕРКА 2: BTC Correlation < 0.4 ---
            btc_corr = coin.get('btc_corr_1d_w30')
            if btc_corr is None or btc_corr < 0.4:
                stats["low_correlation"] += 1
                continue
                
            filtered_coins.append(coin)
        
        count_after = len(filtered_coins)
        
        log.info(f"{log_prefix} Filtering result: {len(all_coins)} -> {count_after} coins.")
        if stats["blacklist"] > 0:
            log.warning(f"{log_prefix} 🚫 Отсеяно по Черному списку: {stats['blacklist']}")
        if stats["low_correlation"] > 0:
            log.warning(f"{log_prefix} 📉 Отсеяно по Correlation (<0.4): {stats['low_correlation']}")
        
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