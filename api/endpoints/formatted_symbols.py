# api/endpoints/formatted_symbols.py

import logging
from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import JSONResponse
from typing import List, Dict, Any

# Import project modules
import services 
from api.security import verify_token

# --- Setup ---
log = logging.getLogger(__name__)
formatted_symbols_router = APIRouter()

# ============================================================================
# === Вспомогательные функции (Логика) ===
# ============================================================================

def _format_tv_symbol(full_tv_symbol: str) -> str:
    """
    (ИЗМЕНЕНИЕ №1)
    Преобразует ПОЛНЫЙ символ (напр., 'BTC/USDT:USDT') в формат 
    TradingView (напр., 'BTCUSDT').
    """
    if not full_tv_symbol:
        return ""
    
    # 1. Отсекаем :USDT (или :BTC и т.д.)
    # 'BTC/USDT:USDT' -> 'BTC/USDT'
    ccxt_symbol = full_tv_symbol.split(':')[0]
    
    # 2. Убираем '/'
    # 'BTC/USDT' -> 'BTCUSDT'
    return ccxt_symbol.replace("/", "")

def _format_tv_exchange(exchange_id: str) -> str:
    """
    Преобразует ID биржи (напр., 'binanceusdm') в формат 
    TradingView (напр., 'binance').
    """
    if exchange_id == "binanceusdm":
        return "binance"
    return exchange_id

# ============================================================================
# === Эндпоинт ===
# ============================================================================

@formatted_symbols_router.get("/coins/formatted-symbols", dependencies=[Depends(verify_token)])
async def get_formatted_symbols():
    """
    Возвращает отформатированный список монет и бирж 
    для использования в TradingView.
    """
    log_prefix = "[API /coins/formatted-symbols] "
    log.info(f"{log_prefix} Запрос отформатированного списка...")
    
    try:
        all_coins = await services.get_cached_coins_data(
            log_prefix=f"{log_prefix} [Cache]"
        )
        
        if not all_coins:
            log.warning(f"{log_prefix} Кэш пуст.")
            return JSONResponse(content={"count": 0, "symbols": []})

        formatted_list = []
        for coin in all_coins:
            
            # (ИЗМЕНЕНИЕ №1) Мы читаем поле 'symbol', 
            # которое (как вы указали) содержит 'BTC/USDT:USDT'
            full_tv_symbol = coin.get('symbol') 
            exchanges = coin.get('exchanges', [])
            
            if not full_tv_symbol:
                continue

            # Используем ИСПРАВЛЕННЫЙ хелпер
            formatted_symbol = _format_tv_symbol(full_tv_symbol)
            
            formatted_exchanges = [
                _format_tv_exchange(ex) for ex in exchanges
            ]
            
            formatted_list.append({
                "symbol": formatted_symbol,
                "exchanges": formatted_exchanges
            })

        log.info(f"{log_prefix} Успешно. Возвращаем {len(formatted_list)} символов.")
        
        return JSONResponse(content={
            "count": len(formatted_list),
            "symbols": formatted_list
        })

    except Exception as e:
        log.error(f"{log_prefix} Ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))