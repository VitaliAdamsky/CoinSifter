# services/exchange_api.py 

import logging
import asyncio
import ccxt.pro as ccxt
from typing import List, Optional, Dict, Any
from datetime import datetime

from .exchange_utils import retry_on_network_error 

log = logging.getLogger(__name__)

# ============================================================================
# 1. fetch_markets
# ============================================================================

@retry_on_network_error()
async def fetch_markets(exchange: ccxt.Exchange, quote_currencies: List[str], log_prefix: str) -> Dict[str, Any]:
    """
    Загружает все рынки биржи и фильтрует их по валюте.
    """
    try:
        log.info(f"{log_prefix} 🔄 Загрузка рынков для {exchange.id} (Quote: {', '.join(quote_currencies)})...")
        markets = await exchange.load_markets()
        
        filtered_markets = {}
        for symbol, market in markets.items():
            if market['active'] and market['type'] in ['future', 'swap'] and market['quote'] in quote_currencies:
                filtered_markets[symbol] = market
        
        log.info(f"{log_prefix} ✅ Найдено {len(filtered_markets)} активных фьючерсных рынков.")
        return filtered_markets
    
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка загрузки рынков: {e}", exc_info=True)
        return {}


# ============================================================================
# 2. fetch_tickers
# ============================================================================

@retry_on_network_error()
async def fetch_tickers(exchange: ccxt.Exchange, log_prefix: str) -> Dict[str, Any]:
    """
    Загружает тикеры (цены и объем) для всех рынков.
    """
    try:
        params = {'category': 'linear'} if exchange.id == 'bybit' else {}
        log.info(f"{log_prefix} 🔄 Загрузка тикеров (цены/объем) с {exchange.id}...")
        tickers = await exchange.fetch_tickers(params=params)
        
        log.debug(f"{log_prefix} ✅ Получено {len(tickers)} тикеров.")
        return tickers
    
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка загрузки тикеров: {e}", exc_info=True)
        return {}


# ============================================================================
# 3. fetch_ohlcv (Пагинация с улучшенными логами)
# ============================================================================

@retry_on_network_error() 
async def fetch_ohlcv(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since: int,
    limit: int,
    log_prefix: str
) -> List[List[float]]:
    """
    Загружает исторические OHLCV данные с АСИНХРОННОЙ ПАГИНАЦИЕЙ.
    """
    
    all_ohlcv_data = []
    current_since = since
    MAX_PAGES = 30 
    
    log.debug(
        f"{log_prefix} 📖 Начало пагинации {timeframe} (с {datetime.fromtimestamp(since/1000).strftime('%Y-%m-%d')}). "
        f"Лимит/стр: {limit}."
    )
    
    for page in range(1, MAX_PAGES + 1):
        try:
            ohlcv_chunk = await exchange.fetch_ohlcv(
                symbol,
                timeframe,
                current_since,
                limit
            )
            
            if not ohlcv_chunk:
                log.debug(f"{log_prefix} ✅ Пагинация завершена (пустой ответ на странице {page}).")
                break
                
            all_ohlcv_data.extend(ohlcv_chunk)
            current_since = ohlcv_chunk[-1][0] + 1 
            
            log.debug(
                f"{log_prefix} 📄 Страница {page}/{MAX_PAGES}: "
                f"Загружено {len(ohlcv_chunk)} свечей. Всего: {len(all_ohlcv_data)}."
            )

            if len(ohlcv_chunk) < limit:
                 log.debug(f"{log_prefix} ✅ Пагинация завершена (конец истории на странице {page}).")
                 break

        except Exception as e:
            log.warning(f"{log_prefix} ⚠️ Ошибка пагинации на странице {page}: {e}")
            raise 

    if len(all_ohlcv_data) >= limit * MAX_PAGES:
        log.warning(f"{log_prefix} ⚠️ Достигнуто MAX страниц ({MAX_PAGES}).")

    return all_ohlcv_data