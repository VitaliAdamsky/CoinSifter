# api/endpoints/logs.py

import logging
import asyncio  # (Этот импорт был в вашем файле)
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

# (Импорт пропущен) Импортируем database-функцию
from database import fetch_logs_from_db

# (Импорт пропущен) Импортируем наш модуль безопасности
from ..security import verify_token

# --- Настройка (Пропущена) ---
log = logging.getLogger(__name__)

# --- (!!!) ВОТ ОШИБКА (!!!) ---
# (Пропущено) Эта строка ОБЯЗАТЕЛЬНА.
logs_router = APIRouter()
# --------------------------------

# --- API Эндпоинты (Логи) ---

@logs_router.get("/logs", dependencies=[Depends(verify_token)])
async def get_logs():
    """(V3) Получает ВСЕ логи (PostgreSQL)."""
    try:
        loop = asyncio.get_event_loop()  # (Это из вашего файла)
        # (Это из вашего файла)
        logs = await loop.run_in_executor(None, fetch_logs_from_db, 100) 
        
        return JSONResponse(content=jsonable_encoder({"count": len(logs), "logs": logs}))
    
    except Exception as e:
        log.error(f"[API /logs GET] Ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {e}")