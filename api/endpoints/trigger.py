# api/endpoints/trigger.py

import logging
import asyncio
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends

# (ПРОПУЩЕНО) Импортируем модули проекта
import analysis
from database import create_log_entry, update_log_status

# (ПРОПУЩЕНО) Импортируем наш модуль безопасности
from ..security import verify_token

# --- Настройка (ПРОПУЩЕНО) ---
log = logging.getLogger(__name__)

# --- (!!!) ВОТ ОШИБКА (!!!) ---
# (ПРОПУЩЕНО) Эта строка ОБЯЗАТЕЛЬНА.
trigger_router = APIRouter()

# --- (V3) Вспомогательная функция для запуска анализа ---
# (Это код, который вы прислали)
async def run_analysis_in_background(log_id: int, log_prefix: str):
    """
    (V3) Обертка для фонового запуска analysis_logic.
    Логирует ошибки и обновляет статус в БД.
    """
    log.info(f"{log_prefix} (BG) Фоновая задача запущена.")
    coins_saved = 0 
    loop = asyncio.get_event_loop() # (Это из вашего файла)
    
    try:
        # --- (V3) ГЛАВНЫЙ ВЫЗОВ ---
        coins_saved, details = await analysis.analysis_logic(log_id, log_prefix)

        log.info(f"{log_prefix} (BG) analysis_logic завершен. Сохранено {coins_saved} монет.")
        
        # (ИЗМЕНЕНО) Аргумент 'coins_saved' передан ПОЗИЦИОННО (в конце)
        await loop.run_in_executor(
            None, 
            update_log_status, 
            log_id, 
            "Завершено", 
            details, 
            coins_saved  # <-- Вот исправление
        )

    except Exception as e:
        log.error(f"{log_prefix} (BG) КРИТИЧЕСКАЯ ОШИБКА в analysis_logic: {e}", exc_info=True)
        # (ИЗМЕНЕНО) Аргумент 0 (coins_saved) передан ПОЗИЦИОННО (в конце)
        await loop.run_in_executor(
            None, 
            update_log_status, 
            log_id, 
            "Ошибка", 
            f"Критическая ошибка: {e}", 
            0  # <-- Уже был правильным, оставлен для консистентности
        )


# --- API Эндпоинты (ПРОПУЩЕНО) ---
# (Эта функция была ПОЛНОСТЬЮ пропущена в вашем файле)
@trigger_router.post("/trigger", dependencies=[Depends(verify_token)])
async def trigger_analysis(background_tasks: BackgroundTasks):
    """
    (V3) Запускает полный процесс анализа (Этапы 0-4) в фоновом режиме.
    """
    log_prefix = f"[Run ID: ???] " # (V3) Временный ID до создания лога
    try:
        log.info(f"{log_prefix} (V3) /trigger вызван. Попытка создать запись в логе...")
        
        # (ИСПРАВЛЕНО) (TypeError) Мы *обязаны* передать 'status',
        log_id = create_log_entry(status="Запуск")
        
        if not log_id:
            raise HTTPException(status_code=500, detail="Не удалось создать запись в логе.")
        
        # Обновляем префикс, используя ID из БД
        log_prefix = f"[Run ID: {log_id}] "
        log.info(f"{log_prefix} Запись в логе создана. Запуск analysis_logic в фоне...")

        # Добавляем задачу в фон
        background_tasks.add_task(run_analysis_in_background, log_id, log_prefix)
        
        return {
            "message": "Анализ запущен в фоновом режиме.",
            "run_id": log_id
        }

    except Exception as e:
        log.error(f"{log_prefix} (V3) КРИТИЧЕСКАЯ ОШИБКА в /trigger: {e}", exc_info=True)
        detail_msg = f"Ошибка при запуске анализа: {e}"
        # (Проблема #6) Обновляем лог (если он был создан) при сбое
        if 'log_id' in locals() and log_id:
            # (ИЗМЕНЕНО) Здесь update_log_status вызывается СИНХРОННО,
            # поэтому именованный аргумент 'coins_saved' РАБОТАЕТ.
            # Оставляем как есть, т.к. это не async-контекст.
            update_log_status(log_id, "Ошибка", detail_msg, coins_saved=0)
            
        raise HTTPException(status_code=500, detail=detail_msg)