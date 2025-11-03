# analysis/stage_0_prereqs.py

import logging
import asyncio

import config
# --- (ИЗМЕНЕНИЕ №1) ---
# (УДАЛЕНО) import services
# (УДАЛЕНО) from services import load_blacklist_from_mongo_async
from services import data_fetcher
from services import mongo_service
# --- (КОНЕЦ ИЗМЕНЕНИЯ) ---

# Импорты из нашего нового модуля
from .helpers import exchange_manager
from .constants import FETCH_MATURITY_TIMEOUT

log = logging.getLogger(__name__)

async def load_btc_and_blacklist(log_prefix=""):
    """
    (V3 - Этап 0)
    Асинхронно загружает кэш BTC и черный список.
    """
    log_prefix = f"{log_prefix}[Этап 0]"
    log.info(f"{log_prefix} Загрузка кэша BTC и Черного списка...")
    
    # 1. Загрузка BTC
    btc_cache_1d = None
    try:
        # (РЕШЕНИЕ №6) Используем правильные имена из config
        exchange_id = config.EXCHANGES_TO_LOAD[0] 
        symbol = config.BTC_SYMBOL 
        tf = '1d'
        days = config.HISTORY_LOAD_DAYS.get(tf, 180) # (Из config)
        
        async with exchange_manager(exchange_id, f"{log_prefix} [BTC]") as btc_ex:
            if btc_ex:
                # --- (ИЗМЕНЕНИЕ №2) ---
                ohlcv_map = await asyncio.wait_for(
                    data_fetcher.fetch_all_ohlcv_data(
                        btc_ex, 
                        symbol,
                        {tf: days}, # (Загружаем только 1 ТФ)
                        f"{log_prefix} [BTC]"
                    ),
                    timeout=FETCH_MATURITY_TIMEOUT
                )
                # --- (КОНЕЦ ИЗМЕНЕНИЯ) ---
                
                if ohlcv_map and tf in ohlcv_map:
                    btc_cache_1d = ohlcv_map[tf]
                    log.info(f"{log_prefix} ✅ Кэш BTC (1d) успешно загружен ({len(btc_cache_1d)} свечей).")
                else:
                    log.warning(f"{log_prefix} ❌ Не удалось загрузить кэш BTC (1d).")
            else:
                log.warning(f"{log_prefix} ❌ Не удалось инициализировать биржу для BTC.")
                
    except asyncio.TimeoutError:
        log.warning(f"{log_prefix} ❌ Таймаут при загрузке кэша BTC.")
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при загрузке кэша BTC: {e}", exc_info=True)

    # 2. Загрузка Черного списка (асинхронно)
    blacklist = set() # (ИЗМЕНЕНИЕ №2) set() вместо list()
    try:
        # (РЕШЕНИЕ №7) Используем services для асинхронной загрузки
        # --- (ИЗМЕНЕНИЕ №3) ---
        blacklist = await mongo_service.load_blacklist_from_mongo_async(f"{log_prefix} [Mongo]")
        # --- (КОНЕЦ ИЗМЕНЕНИЯ) ---
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при загрузке Черного списка: {e}", exc_info=True)

    return btc_cache_1d, blacklist