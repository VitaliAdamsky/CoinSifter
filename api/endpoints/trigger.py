# api/endpoints/trigger.py

import logging
import asyncio
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends

# (ИЗМЕНЕНИЕ) Импортируем модули проекта
import analysis
from services import mongo_service  # <-- Замена PostreSQL
# (ИЗМЕНЕНИЕ) Удалены импорты PostgreSQL
# from database import create_log_entry, update_log_status 

# (ИЗМЕНЕНИЕ) Импортируем наш модуль безопасности
from ..security import verify_token

# --- Настройка ---
log = logging.getLogger(__name__)

# (ИЗМЕНЕНИЕ) Добавлен APIRouter
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
        # analysis_logic ожидает log_id (теперь str) для префикса лога
        coins_saved, details = await analysis.analysis_logic(log_id, log_prefix)

        log.info(f"{log_prefix} (BG) analysis_logic завершен. Сохранено {coins_saved} монет.")
        
        # (ИЗМЕНЕНИЕ) Заменяем loop.run_in_executor на прямой await 
        # новой функции Mongo
        await mongo_service.update_mongo_log_status(
            log_id, 
            "Завершено", 
            details, 
            coins_saved
        )

    except Exception as e:
        log.error(f"{log_prefix} (BG) КРИТИЧЕСКАЯ ОШИБКА в analysis_logic: {e}", exc_info=True)
        
        # (ИЗМЕНЕНИЕ) Заменяем loop.run_in_executor на прямой await 
        # новой функции Mongo
        await mongo_service.update_mongo_log_status(
            log_id, 
            "Ошибка", 
            f"Критическая ошибка: {e}", 
            0
        )


# --- API Эндпоинты ---

@trigger_router.post("/trigger", dependencies=[Depends(verify_token)])
async def trigger_analysis(background_tasks: BackgroundTasks):
    """
    (V3) Запускает полный процесс анализа (Этапы 0-4) в фоновом режиме.
    (ИЗМЕНЕНО: использует MongoDB для логов)
    """
    log_prefix = f"[Run ID: ???] "
    log_id = None # (ИЗМЕНЕНИЕ) Определяем log_id здесь
    
    try:
        log.info(f"{log_prefix} (V3) /trigger вызван. Попытка создать запись в логе MongoDB...")
        
        # (ИЗМЕНЕНИЕ) Используем create_mongo_log_entry
        log_id = await mongo_service.create_mongo_log_entry(status="Запуск")
        
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
        detail_msg = f"Ошибка при запуске анализа: {e}"
        
        # (ИЗМЕНЕНИЕ) Обновляем лог Mongo (если он был создан)
        if log_id:
            await mongo_service.update_mongo_log_status(
                log_id, 
                "Ошибка", 
                detail_msg, 
                coins_saved=0
            )
            
        raise HTTPException(status_code=500, detail=detail_msg)