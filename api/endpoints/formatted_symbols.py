# api/endpoints/formatted_symbols.py

import logging
from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import JSONResponse
from typing import List, Dict, Any

from services.data_cache_service import get_cached_coins_data
from services.mongo_service import load_blacklist_from_mongo_async
from api.security import verify_token

# --- Setup ---
log = logging.getLogger(__name__)
formatted_symbols_router = APIRouter()

# ============================================================================
# === Вспомогательные функции (Логика) ===
# ============================================================================

def _extract_base_symbol_from_full(full_symbol: str) -> str:
    if not full_symbol:
        return ""
    ccxt_symbol = full_symbol.split(':')[0] 
    return ccxt_symbol.split('/')[0]


def _format_tv_symbol(full_tv_symbol: str) -> str:
    ccxt_symbol = full_tv_symbol.split(':')[0] # "BTC/USDT"
    tv_symbol = ccxt_symbol.replace('/', '') # "BTCUSDT"
    if tv_symbol.endswith('.P'):
        tv_symbol = tv_symbol[:-2]
    return tv_symbol

def _format_tv_exchange(exchange_id: str) -> str:
    if 'binance' in exchange_id:
        return 'BINANCE'
    elif 'bybit' in exchange_id:
        return 'BYBIT'
    return exchange_id.upper()


# ============================================================================
# === Эндпоинт (Formatted Symbols) ===
# ============================================================================

@formatted_symbols_router.get(
    "/coins/formatted-symbols", 
    dependencies=[Depends(verify_token)]
)
async def get_formatted_symbols():
    """
    (V3) Возвращает монеты из КЭША (MongoDB) в 
    специальном формате для TradingView.
    Фильтрация:
    1. Blacklist
    2. BTC Correlation < 0.4
    """
    log_prefix = "[API /coins/formatted-symbols GET]"
    log.info(f"{log_prefix} Запрошены монеты (формат TradingView)...")
    
    try:
        # Шаг 1: Получаем данные из кэша
        all_coins = await get_cached_coins_data(
            force_reload=False, 
            log_prefix=f"{log_prefix} [Cache]"
        )

        # Шаг 2: Получаем Черный список
        blacklist = await load_blacklist_from_mongo_async(
            log_prefix=f"{log_prefix} [Blacklist]"
        )
        
        if not all_coins:
            log.warning(f"{log_prefix} Кэш пуст.")
            raise HTTPException(status_code=404, detail="No data available in cache.")
            
        # Шаг 3: Обработка и фильтрация
        formatted_list = []
        stats = {
            "blacklist": 0,
            "low_correlation": 0
        }
        
        for coin in all_coins:
            full_tv_symbol = coin.get('symbol') 
            exchanges = coin.get('exchanges', [])
            
            if not full_tv_symbol:
                continue

            # --- ПРОВЕРКА 1: Blacklist ---
            base_symbol = _extract_base_symbol_from_full(full_tv_symbol)
            if base_symbol in blacklist:
                stats["blacklist"] += 1
                continue
            
            # --- ПРОВЕРКА 2: BTC Correlation < 0.4 ---
            # (Добавляем ту же логику, что и в coins.py)
            btc_corr = coin.get('btc_corr_1d_w30')
            if btc_corr is None or btc_corr < 0.4:
                stats["low_correlation"] += 1
                continue

            # Шаг 3.2. Форматирование (только для прошедших фильтры)
            formatted_symbol = _format_tv_symbol(full_tv_symbol)
            
            formatted_exchanges = [
                _format_tv_exchange(ex) for ex in exchanges
            ]
            
            formatted_list.append({
                "symbol": formatted_symbol,
                "exchanges": formatted_exchanges,
                "category": coin.get("category")
            })

        # Логирование результатов фильтрации
        log.info(f"{log_prefix} Filtering result: {len(all_coins)} -> {len(formatted_list)} coins.")
        if stats["blacklist"] > 0:
            log.warning(f"{log_prefix} 🚫 Отсеяно по Черному списку: {stats['blacklist']}")
        if stats["low_correlation"] > 0:
            log.warning(f"{log_prefix} 📉 Отсеяно по Correlation (<0.4): {stats['low_correlation']}")

        log.info(f"{log_prefix} Успешно. Возвращаем {len(formatted_list)} символов.")
        
        return JSONResponse(content={
            "count": len(formatted_list),
            "symbols": formatted_list
        })
        
    except HTTPException:
        raise 
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")