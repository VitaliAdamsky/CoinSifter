# services/data_cache_service.py

import logging
import time
import asyncio
from typing import List, Dict, Any

# (ИЗМЕНЕНИЕ) Импортируем функцию из mongo_service, а не database
from .mongo_service import get_all_coins_from_mongo_async
# (ИЗМЕНЕНИЕ) Удален импорт PostgreSQL
# from database import fetch_all_coins_from_db 

log = logging.getLogger(__name__)

# --- Наш централизованный кэш ---
_cache: List[Dict[str, Any]] = []
_last_load_time: float = 0.0
_cache_lock = asyncio.Lock()

# (TTL) Время жизни кэша в секундах (15 минут)
DEFAULT_CACHE_TTL = 15 * 60

async def get_cached_coins_data(
    force_reload: bool = False, 
    ttl_seconds: int = DEFAULT_CACHE_TTL,
    log_prefix: str = "[DataCache]"
) -> List[Dict[str, Any]]:
    """
    (ИЗМЕНЕНО: ИСТОЧНИК - MONGODB)
    Централизованная функция для получения данных о монетах с TTL-кэшированием.
    
    1. Проверяет, "протух" ли кэш.
    2. Если да (или force_reload=True), асинхронно загружает данные из MongoDB.
    3. Возвращает данные из кэша.
    """
    global _cache, _last_load_time
    
    now = time.time()
    is_cache_expired = (now - _last_load_time) > ttl_seconds

    # Блокируем, чтобы только один запрос мог обновить кэш
    async with _cache_lock:
        # Повторно проверяем, вдруг кэш уже обновился, пока мы ждали lock
        now = time.time()
        is_cache_expired = (now - _last_load_time) > ttl_seconds
        
        if force_reload or is_cache_expired or not _cache:
            if force_reload:
                log.info(f"{log_prefix} Принудительная перезагрузка кэша...")
            elif not _cache:
                log.info(f"{log_prefix} Кэш пуст. Загрузка данных из MongoDB...")
            else:
                log.info(f"{log_prefix} Кэш протух (прошло {now - _last_load_time:.0f}с > {ttl_seconds}с). Обновление из MongoDB...")

            try:
                # --- (ИЗМЕНЕНИЕ) ВЫЗОВ MONGODB ---
                # Заменяем loop.run_in_executor(None, fetch_all_coins_from_db, ...)
                # на прямой await новой асинхронной функции.
                data = await get_all_coins_from_mongo_async(
                    f"{log_prefix} [Mongo]"
                )
                # --- КОНЕЦ ИЗМЕНЕНИЯ ---
                
                if data:
                    _cache = data
                    _last_load_time = time.time()
                    log.info(f"{log_prefix} ✅ Кэш успешно обновлен. Загружено {len(_cache)} монет из MongoDB.")
                elif not _cache:
                    # Если загрузка не удалась, но в кэше пусто
                    log.warning(f"{log_prefix} ⚠️ Не удалось загрузить данные из MongoDB, кэш остается пустым.")
                    _cache = []
                else:
                    # Если загрузка не удалась, но в кэше есть старые данные
                    log.warning(f"{log_prefix} ⚠️ Не удалось обновить данные из MongoDB. Будут использованы старые данные из кэша.")
            
            except Exception as e:
                log.error(f"{log_prefix} ❌ Крит. ошибка при обновлении кэша из MongoDB: {e}", exc_info=True)
                # Не сбрасываем кэш, если он уже есть, чтобы не потерять старые данные
        
        else:
            log.debug(f"{log_prefix} Данные взяты из кэша (свежий).")

    return _cache