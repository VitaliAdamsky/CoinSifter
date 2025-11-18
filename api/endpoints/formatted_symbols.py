# api/endpoints/formatted_symbols.py

import logging
from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import JSONResponse
from typing import List, Dict, Any

# --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
# (БЫЛО) import services 
# (СТАЛО) Импортируем НАПРЯМУЮ
from services.data_cache_service import get_cached_coins_data
from services.mongo_service import load_blacklist_from_mongo_async
# --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

from api.security import verify_token

# --- Setup ---
log = logging.getLogger(__name__)
formatted_symbols_router = APIRouter()

# ============================================================================\r
# === Вспомогательные функции (Логика) ===\r
# ============================================================================

def _extract_base_symbol_from_full(full_symbol: str) -> str:
    """
    Извлекает базовый символ из полного формата (e.g., 'SOL/USDT:USDT' -> 'SOL').
    (Скопировано из api/endpoints/coins.py)
    """
    if not full_symbol:
        return ""
    ccxt_symbol = full_symbol.split(':')[0] 
    return ccxt_symbol.split('/')[0]


def _format_tv_symbol(full_tv_symbol: str) -> str:
    """
    (ИЗМЕНЕНИЕ №1)
    Преобразует ПОЛНЫЙ символ (e.g., "BTC/USDT:USDT") 
    в формат TradingView (e.g., "BTCUSDT.P" -> "BTCUSDT").
    """
    # 1. Убираем ':USDT'
    ccxt_symbol = full_tv_symbol.split(':')[0] # "BTC/USDT"
    
    # 2. Убираем '/'
    tv_symbol = ccxt_symbol.replace('/', '') # "BTCUSDT"
    
    # 3. (ИЗМЕНЕНИЕ №1) Убираем ".P" (Bybit)
    if tv_symbol.endswith('.P'):
        tv_symbol = tv_symbol[:-2]
        
    return tv_symbol

def _format_tv_exchange(exchange_id: str) -> str:
    """
    Преобразует ID биржи (e.g., 'binanceusdm') 
    в формат TradingView (e.g., 'BINANCE').
    """
    if 'binance' in exchange_id:
        return 'BINANCE'
    elif 'bybit' in exchange_id:
        return 'BYBIT'
    # Добавьте другие биржи здесь, если нужно
    return exchange_id.upper()


# ============================================================================\r
# === Эндпоинт (Formatted Symbols) ===\r
# ============================================================================

@formatted_symbols_router.get(
    "/coins/formatted-symbols", 
    dependencies=[Depends(verify_token)]
)
async def get_formatted_symbols():
    """
    (V3) Возвращает монеты из КЭША (MongoDB) в 
    специальном формате для TradingView.
    """
    log_prefix = "[API /coins/formatted-symbols GET]"
    log.info(f"{log_prefix} Запрошены монеты (формат TradingView)...")
    
    try:
        # Шаг 1: Получаем данные из кэша
        # --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
        # (БЫЛО) all_coins = await services.get_cached_coins_data(...)
        # (СТАЛО)
        all_coins = await get_cached_coins_data(
            force_reload=False, 
            log_prefix=f"{log_prefix} [Cache]"
        )
        # --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

        # Шаг 2: Получаем Черный список
        # --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
        # (БЫЛО) blacklist = await services.load_blacklist_from_mongo_async(...)
        # (СТАЛО)
        blacklist = await load_blacklist_from_mongo_async(
            log_prefix=f"{log_prefix} [Blacklist]"
        )
        # --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---
        
        if not all_coins:
            log.warning(f"{log_prefix} Кэш пуст.")
            raise HTTPException(status_code=404, detail="No data available in cache.")
            
        # Шаг 3: Обработка и форматирование
        formatted_list = []
        coins_filtered_by_blacklist = 0
        
        for coin in all_coins:
            # (ИЗМЕНЕНИЕ №1) 'symbol' в MongoDB - это 'full_tv_symbol'
            full_tv_symbol = coin.get('symbol') 
            exchanges = coin.get('exchanges', [])
            
            if not full_tv_symbol:
                continue

            # --- ИЗМЕНЕНИЕ: Шаг 3.1. Проверка по Blacklist ---
            base_symbol = _extract_base_symbol_from_full(full_tv_symbol)
            
            if base_symbol in blacklist:
                coins_filtered_by_blacklist += 1
                continue
            # --- Конец Изменения ---

            # Шаг 3.2. Форматирование (только для прошедших)
            formatted_symbol = _format_tv_symbol(full_tv_symbol)
            
            formatted_exchanges = [
                _format_tv_exchange(ex) for ex in exchanges
            ]
            
            formatted_list.append({
                "symbol": formatted_symbol,
                "exchanges": formatted_exchanges
            })

        if coins_filtered_by_blacklist > 0:
            log.warning(f"{log_prefix} 🚫 Отсеяно по Черному списку: {coins_filtered_by_blacklist} монет.")

        log.info(f"{log_prefix} Успешно. Возвращаем {len(formatted_list)} символов.")
        
        return JSONResponse(content={
            "count": len(formatted_list),
            "data": formatted_list
        })
        
    except HTTPException:
        raise 
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")