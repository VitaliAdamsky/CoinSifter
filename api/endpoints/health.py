# api/endpoints/health.py

import logging
from fastapi import APIRouter, Depends, HTTPException
from ..security import verify_token
import logging

# (УДАЛЕН) Импорт 'fetch_last_analysis_timestamp' (PostgreSQL)

# --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
from typing import List, Dict, Any
# Импортируем НАПРЯМУЮ из файла, а не из __init__.py
from services.data_cache_service import get_cached_coins_data 
# --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

log = logging.getLogger(__name__)

# --- Setup ---
health_router = APIRouter()

@health_router.get("/health")
@health_router.head("/health")
def health_check():
    """Server health check."""
    return {"status": "ok"}


# --- (НОВЫЙ ЭНДПОИНТ) ---
@health_router.post(
    "/health/cache/reload",
    summary="Принудительно перезагрузить кэш монет из MongoDB",
    dependencies=[Depends(verify_token)] 
)
async def reload_cache() -> Dict[str, Any]:
    """
    Принудительно очищает кэш в памяти и загружает
    актуальные данные из MongoDB.
    """
    log_prefix = "[API.Health.CacheReload]"
    log.info(f"{log_prefix} 🔄 Получен ручной запрос на перезагрузку кэша...")
    
    try:
        reloaded_data: List[Dict] = await get_cached_coins_data(
            force_reload=True, 
            log_prefix=log_prefix
        )
        
        count = len(reloaded_data)
        log.info(f"{log_prefix} ✅ Кэш успешно перезагружен. Загружено {count} монет.")
        
        return {
            "message": "Кэш успешно перезагружен из MongoDB.",
            "coins_loaded": count
        }
        
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при ручной перезагрузке кэша: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка при перезагрузке кэша: {e}"
        )