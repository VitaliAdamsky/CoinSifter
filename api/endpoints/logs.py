# api/endpoints/logs.py

import logging
import asyncio
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

# (ИЗМЕНЕНИЕ) Импортируем 'mongo_service'
from services import mongo_service
# (ИЗМЕНЕНИЕ) Удален импорт 'database'
# from database import fetch_logs_from_db

# (ИЗМЕНЕНИЕ) Импортируем наш модуль безопасности
from ..security import verify_token

# --- Настройка ---
log = logging.getLogger(__name__)

# (ИЗМЕНЕНИЕ) APIRouter
logs_router = APIRouter()

# --- API Эндпоинты (Логи) ---

@logs_router.get("/logs", dependencies=[Depends(verify_token)])
async def get_logs():
    """(V3) Получает ВСЕ логи (ИЗМЕНЕНО: из MongoDB)."""
    try:
        # (ИЗМЕНЕНИЕ) Заменяем 'fetch_logs_from_db' на 'get_mongo_logs'
        logs = await mongo_service.get_mongo_logs(limit=100) 
        
        return JSONResponse(content=jsonable_encoder({"count": len(logs), "logs": logs}))
    
    except Exception as e:
        log.error(f"[API /logs GET] Ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {e}")