"""
Модуль Services (Фасад).

Этот __init__.py файл собирает все публичные функции из 
различных модулей (mongo_service, exchange_api, data_fetcher...)
и "выставляет" их в единый, удобный неймспейс 'services'.

Это позволяет остальной части приложения (router, analysis) 
импортировать функции напрямую, например:
from services import load_blacklist_from_mongo_async
"""

# --- Из mongo_service.py ---
# (Предполагаем, что здесь импорты из mongo_service.py)
from .mongo_service import (
    get_mongo_client,
    close_mongo_client,
    load_blacklist_from_mongo,
    load_blacklist_from_mongo_async
)

# --- Из exchange_utils.py ---
# (Предполагаем, что здесь импорты из exchange_utils.py)
from .exchange_utils import (
    retry_on_network_error,
    initialize_exchange
)

# --- Из exchange_api.py ---
# (Предполагаем, что здесь импорты из exchange_api.py)
from .exchange_api import (
    fetch_markets,
    fetch_tickers,
    fetch_ohlcv
)

# --- Из data_fetcher.py ---
# (Предполагаем, что здесь импорты из data_fetcher.py)
from .data_fetcher import (
    fetch_all_ohlcv_data,
    fetch_all_coins_data
)

# --- Из data_quality_service.py ---
from .data_quality_service import (
    get_data_quality_report
)

# --- (НОВОЕ) Из data_cache_service.py ---
from .data_cache_service import (
    get_cached_coins_data
)