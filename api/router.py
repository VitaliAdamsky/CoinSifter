# api/router.py

import logging
import asyncio
from fastapi import FastAPI, Request, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import time

from .security import verify_token 
from .endpoints import (
    blacklist_router, 
    coins_router, 
    health_router, 
    logs_router,
    trigger_router,
    data_quality_router,
    formatted_symbols_router
)

# --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
# Импортируем сервисные функции НАПРЯМУЮ
from services.mongo_service import (
    close_mongo_client,
    load_blacklist_from_mongo_async
)
from services.data_cache_service import get_cached_coins_data
# --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

# --- Настройка ---
log = logging.getLogger(__name__)
app = FastAPI(title="Crypto Analysis API")

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ГРУППА РОУТЕРОВ API (БЕЗ ПРЕФИКСА) ---
app.include_router(health_router, tags=["Health"])
app.include_router(logs_router, tags=["Logs"])
app.include_router(blacklist_router, tags=["Blacklist"])
app.include_router(data_quality_router, tags=["Data Quality"])
app.include_router(trigger_router, tags=["Trigger"])
app.include_router(coins_router, tags=["Coins"])
app.include_router(formatted_symbols_router, tags=["Coins (Formatted)"])


# --- События Startup / Shutdown ---

@app.on_event("startup")
async def startup():
    """
    Выполняется при старте приложения.
    """
    log.info("...Событие Startup...")
    
    # 1. Загрузка черного списка (асинхронно)
    await load_blacklist_from_mongo_async(log_prefix="[Startup]")
    
    # 2. "Прогрев" кэша
    await get_cached_coins_data(force_reload=True, log_prefix="[Startup]")
    
    log.info("...Событие Startup завершено...")

@app.on_event("shutdown")
async def shutdown():
    """
    Выполняется при остановке приложения.
    """
    log.info("...Событие Shutdown...")
    close_mongo_client(log_prefix="[Shutdown]")
    log.info("...Событие Shutdown завершено...")


# --- Точка входа для Uvicorn (если запускается напрямую) ---
def run_api_server(host="0.0.0.0", port=8000):
    """Запускает Uvicorn сервер."""
    log.info(f"Запуск Uvicorn-сервера на {host}:{port}")
    uvicorn.run(app, host=host, port=port)