# api/router.py

import logging
import asyncio
from fastapi import FastAPI, Request, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import time

# (ИЗМЕНЕНО) Импортируем 'verify_token'
from .security import verify_token 
from .endpoints import (
    blacklist_router, 
    coins_router, 
    health_router, 
    logs_router,
    trigger_router,
    data_quality_router,
    formatted_symbols_router # (ИЗМЕНЕНИЕ №1)
)
# (ИЗМЕНЕНО) "Мертвый" импорт _data_loader УДАЛЕН

# Импортируем сервисные функции, которые будет использовать этот router
from services import (
    close_mongo_client,
    load_blacklist_from_mongo_async,
    get_cached_coins_data  # <-- (НОВОЕ) Добавлено для "прогрева" кэша
)

# --- Настройка ---
log = logging.getLogger(__name__)
app = FastAPI(title="Crypto Analysis API")

# --- Middleware ---

# CORS (Cross-Origin Resource Sharing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # TODO: Ограничить в production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Middleware для логирования времени запроса
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
    # Логирование запроса
    log.info(
        f"REQ: {request.method} {request.url.path} - "
        f"STATUS: {response.status_code} - "
        f"TIME: {process_time:.4f}s"
    )
    return response

# --- Подключение роутеров ---
log.info("Подключение API роутеров...")
app.include_router(health_router, prefix="", tags=["Health"])
app.include_router(trigger_router, prefix="", tags=["Triggers"])
app.include_router(coins_router, prefix="", tags=["Coins"])
app.include_router(blacklist_router, prefix="", tags=["Blacklist"])
app.include_router(logs_router, prefix="", tags=["Logs"])

# (НОВОЕ) Подключаем роутер качества данных
app.include_router(
    data_quality_router, 
    prefix="", 
)

# (ИЗМЕНЕНИЕ №1) Подключаем новый роутер
app.include_router(
    formatted_symbols_router, 
    prefix="", 
    tags=["Coins (Formatted)"]
)


# --- (БЛОК УДАЛЕН) ---
# Весь @app.get("""/data-quality-report", ...) УДАЛЕН ОТСЮДА,
# так как он перенесен в api/endpoints/data_quality.py
# --- (КОНЕЦ УДАЛЕННОГО БЛОКА) ---


# --- События Startup / Shutdown ---

@app.on_event("startup")
async def startup():
    """
    Выполняется при старте приложения.
    1. Загружает черный список из MongoDB.
    2. (ИЗМЕНЕНО) "Прогревает" кэш с монетами.
    """
    log.info("...Событие Startup...")
    
    # 1. Загрузка черного списка (асинхронно)
    await load_blacklist_from_mongo_async(log_prefix="[Startup]")
    
    # 2. (ИЗМЕНЕНО) "Прогрев" нового централизованного кэша
    # (force_reload=True гарантирует, что кэш будет загружен из БД при старте)
    await get_cached_coins_data(force_reload=True, log_prefix="[Startup]")
    
    log.info("...Событие Startup завершено...")

@app.on_event("shutdown")
async def shutdown():
    """
    Выполняется при остановке приложения.
    1. Закрывает пул соединений MongoDB.
    """
    log.info("...Событие Shutdown...")
    close_mongo_client(log_prefix="[Shutdown]")
    log.info("...Событие Shutdown завершено...")


# --- Точка входа для Uvicorn (если запускается напрямую) ---
def run_api_server(host="0.0.0.0", port=8000, log_level="info"):
    log.info(f"Запуск Uvicorn-сервера на {host}:{port} (Log: {log_level})")
    uvicorn.run(app, host=host, port=port, log_level=log_level)

if __name__ == "__main__":
    # Этот блок выполняется, если файл запущен как `python -m api.router`
    logging.basicConfig(level=logging.INFO)
    log.info("Запуск API-сервера в режиме отладки (standalone)...")
    run_api_server(log_level="debug")