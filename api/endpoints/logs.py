# api/endpoints/logs.py

import logging
import asyncio
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

# --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
# Импортируем НАПРЯМУЮ из файла, а не из __init__.py
from services.mongo_service import (
    get_mongo_logs, 
    clear_all_mongo_logs
)
# --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

# Импортируем наш модуль безопасности
from ..security import verify_token

# --- Настройка ---
log = logging.getLogger(__name__)
logs_router = APIRouter()

# --- API Эндпоинты (Логи) ---

@logs_router.get("/logs", dependencies=[Depends(verify_token)])
async def get_logs():
    """(V3) Получает ВСЕ логи (из MongoDB)."""
    try:
        logs = await get_mongo_logs(limit=100) 
        
        return JSONResponse(content=jsonable_encoder({"count": len(logs), "logs": logs}))
    
    except Exception as e:
        log.error(f"[API /logs GET] Ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error fetching logs")


# --- (НОВЫЙ ЭНДПОИНТ) ---
@logs_router.post(
    "/logs/clear",
    summary="(Ручное) Полностью очистить коллекцию логов",
    dependencies=[Depends(verify_token)] 
)
async def clear_logs_endpoint():
    """
    Полностью очищает коллекцию 'script_run_logs' в MongoDB.
    """
    log_prefix = "[API /logs/clear POST]"
    log.info(f"{log_prefix} 🔄 Получен ручной запрос на ПОЛНУЮ ОЧИСТКУ логов...")
    
    try:
        deleted_count = await clear_all_mongo_logs(log_prefix=log_prefix)
        
        log.info(f"{log_prefix} ✅ Очистка завершена. Удалено {deleted_count} логов.")
        return {
            "message": "Очистка логов завершена.",
            "logs_deleted": deleted_count
        }
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при очистке логов: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка при очистке логов: {e}"
        )