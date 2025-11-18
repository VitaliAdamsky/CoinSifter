# api/endpoints/trigger.py

import logging
import asyncio
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends

# --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
# (БЫЛО) from services import mongo_service
# (СТАЛО) Импортируем НАПРЯМУЮ
import analysis
from services.mongo_service import (
    create_mongo_log_entry,
    update_mongo_log_status
)
# --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

# (УДАЛЕНЫ) Импорты PostgreSQL
# from database import create_log_entry, update_log_status 

# Импортируем наш модуль безопасности
from ..security import verify_token

# --- Настройка ---
log = logging.getLogger(__name__)
trigger_router = APIRouter()

# --- (V3) Вспомогательная функция для запуска анализа ---

async def run_analysis_in_background(
    log_id: str,  # (ИЗМЕНЕНИЕ) log_id теперь str (Mongo _id)
    log_prefix: str
):
    """
    (V3) Обертка для фонового запуска analysis_logic.
    Логирует ошибки и обновляет статус в БД (MongoDB).
    """
    log.info(f"{log_prefix} (BG) Фоновая задача запущена.")
    coins_saved = 0 
    
    try:
        # --- (V3) ГЛАВНЫЙ ВЫЗОВ ---
        # (ИЗМЕНЕНИЕ) 'analysis_logic' теперь ожидает 'log_id'
        coins_saved, details = await analysis.analysis_logic(
            run_id=log_id, 
            log_prefix=log_prefix
        )
        
        log.info(f"{log_prefix} (BG) Фоновая задача завершена. Монет сохранено: {coins_saved}")
        
        # (ИЗМЕНЕНИЕ) Используем 'update_mongo_log_status'
        await update_mongo_log_status(
            log_id_str=log_id,
            status="Завершен",
            details=details,
            coins_saved=coins_saved
        )

    except Exception as e:
        log.error(f"{log_prefix} (BG) КРИТИЧЕСКАЯ ОШИБКА в analysis_logic: {e}", exc_info=True)
        try:
            # (ИЗМЕНЕНИЕ) Используем 'update_mongo_log_status'
            await update_mongo_log_status(
                log_id_str=log_id,
                status="Ошибка",
                details=f"Критическая ошибка: {e}"
            )
        except Exception as db_e:
            log.error(f"{log_prefix} (BG) Не удалось даже обновить лог об ошибке: {db_e}", exc_info=True)

# --- (V3) API Эндпоинт (Триггер) ---

@trigger_router.post("/trigger/run-analysis", dependencies=[Depends(verify_token)])
async def trigger_analysis(background_tasks: BackgroundTasks):
    """
    (V3) Запускает полный анализ (асинхронно, в фоне).
    """
    log_prefix = f"[Run ID: ???] "
    log_id = None # (ИЗМЕНЕНИЕ) Определяем log_id здесь
    
    try:
        log.info(f"{log_prefix} (V3) /trigger вызван. Попытка создать запись в логе MongoDB...")
        
        # (ИЗМЕНЕНИЕ) Используем create_mongo_log_entry
        log_id = await create_mongo_log_entry(status="Запуск")
        
        if not log_id:
            raise HTTPException(status_code=500, detail="Не удалось создать запись в логе (MongoDB).")
        
        # Обновляем префикс, используя _id из Mongo
        log_prefix = f"[Run ID: {log_id}] "
        log.info(f"{log_prefix} Запись в логе создана. Запуск analysis_logic в фоне...")

        # Добавляем задачу в фон
        background_tasks.add_task(run_analysis_in_background, log_id, log_prefix)
        
        return {
            "message": "Анализ запущен в фоновом режиме.",
            "run_id": log_id # Возвращаем Mongo _id (str)
        }

    except Exception as e:
        log.error(f"{log_prefix} (V3) КРИТИЧЕСКАЯ ОШИБКА в /trigger: {e}", exc_info=True)
        detail_msg = f"Ошибка при запуске: {e}"
        if log_id:
            # Попытка обновить лог, если он был создан
            await update_mongo_log_status(
                log_id_str=log_id,
                status="Ошибка",
                details=detail_msg
            )
        raise HTTPException(status_code=500, detail=detail_msg)